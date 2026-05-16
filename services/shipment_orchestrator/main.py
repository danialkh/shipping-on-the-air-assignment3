"""Event-driven Shipment Orchestrator — primary Kafka-based microservice."""

import json
import os
import threading
import time
from typing import Any

from fastapi import FastAPI, Response
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS_CREATED = "shipping.orders.created"
TOPIC_SHIPMENTS_EVENTS = "shipping.shipments.events"
TOPIC_DRONE_ASSIGNMENTS = "shipping.drone.assignments"


ORCHESTRATOR_EVENTS_TOTAL = Counter(
    "shipment_orchestrator_events_processed_total",
    "Domain events consumed from Kafka",
    ["event_type", "status"],
)


ORCHESTRATOR_LATENCY = Histogram(
    "shipment_orchestrator_order_to_shipment_scheduled_seconds",
    "Wall time between OrderCreated (event timestamp) and ShipmentScheduled publish",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


ORCHESTRATOR_CONSUMER_ERRORS = Counter(
    "shipment_orchestrator_consumer_errors_total",
    "Errors in the Kafka consumer loop",
    ["phase"],
)


ORCHESTRATOR_READY = Gauge(
    "shipment_orchestrator_consumer_running",
    "1 if Kafka consumer loop is running",
)

producer: KafkaProducer | None = None
consumer_stop = threading.Event()


def bootstrap_producer():
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )


def handle_order_created(ev: dict[str, Any]) -> None:
    order_id = ev["order_id"]
    scheduled_at_ms = int(time.time() * 1000)
    occurred_at_ms = int(ev.get("occurred_at_ms", scheduled_at_ms))
    latency_s = max(0.0, (scheduled_at_ms - occurred_at_ms) / 1000.0)
    ORCHESTRATOR_LATENCY.observe(latency_s)

    shipment_id = f"s-{order_id[:8]}"

    shipment_event = {
        "event_type": "ShipmentScheduled",
        "shipment_id": shipment_id,
        "order_id": order_id,
        "occurred_at_ms": scheduled_at_ms,
        "estimated_pickup_minutes": 15,
        "hub_id": "HUB-WEST",
    }
    drone_event = {
        "event_type": "DroneAssigned",
        "shipment_id": shipment_id,
        "order_id": order_id,
        "drone_id": "DRONE-ALPHA-01",
        "occurred_at_ms": scheduled_at_ms,
        "route_summary": {"waypoints": 3},
    }

    assert producer is not None
    producer.send(TOPIC_SHIPMENTS_EVENTS, key=shipment_id.encode(), value=shipment_event).get(timeout=10)
    producer.send(TOPIC_DRONE_ASSIGNMENTS, key=drone_event["drone_id"].encode(), value=drone_event).get(timeout=10)


def consume_loop():
    global producer
    while not consumer_stop.is_set():
        try:
            if producer is None:
                bootstrap_producer()
            consumer = KafkaConsumer(
                TOPIC_ORDERS_CREATED,
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                group_id="shipment-orchestrator",
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )


            print("Shipment Orchestrator consumer connected to Kafka")
            print(f"Subscribed to topic: {TOPIC_ORDERS_CREATED}")
            print("Waiting for messages...")
            print ("-----------------------------------")
            print ("-----------------------------------")



        except KafkaError:
            ORCHESTRATOR_CONSUMER_ERRORS.labels("connect").inc()
            time.sleep(2)
            continue

        ORCHESTRATOR_READY.set(1)
        try:
            for msg in consumer:
                if consumer_stop.is_set():
                    break
                try:
                    ev = msg.value
                    if isinstance(ev, dict) and ev.get("event_type") == "OrderCreated":
                        handle_order_created(ev)
                        ORCHESTRATOR_EVENTS_TOTAL.labels("OrderCreated", "ok").inc()
                    else:
                        ORCHESTRATOR_EVENTS_TOTAL.labels(str(ev.get("event_type")), "skipped").inc()
                except Exception:
                    ORCHESTRATOR_EVENTS_TOTAL.labels("unknown", "error").inc()
                    ORCHESTRATOR_CONSUMER_ERRORS.labels("handler").inc()
        finally:
            consumer.close()
            ORCHESTRATOR_READY.set(0)
    if producer:
        producer.close()


thread: threading.Thread | None = None
app = FastAPI(title="Shipment Orchestrator")


@app.on_event("startup")
def startup():
    global thread
    consumer_stop.clear()
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()


@app.on_event("shutdown")
def shutdown():
    consumer_stop.set()
    if thread:
        thread.join(timeout=5)


@app.get("/health")
def health():
    return {"status": "ok", "kafka_group": "shipment-orchestrator"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
