"""Drone adapter — minimally adapted to Kafka: consumes assignments, emits status events."""

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
TOPIC_DRONE_ASSIGNMENTS = "shipping.drone.assignments"
TOPIC_DRONE_STATUS = "shipping.drone.status"

DRONE_EVENTS_CONSUMED = Counter(
    "drone_service_assignments_total",
    "Drone assignment events consumed from Kafka",
    ["event_type"],
)

DRONE_PUBLISH_SECONDS = Histogram(
    "drone_service_assignment_to_status_seconds",
    "Time from receiving DroneAssigned to emitting InFlight status",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

DRONE_CONSUMER_ERRORS = Counter(
    "drone_service_consumer_errors_total",
    "Errors while consuming Kafka",
    ["phase"],
)

DRONE_READY = Gauge("drone_service_consumer_running", "1 while Kafka consumer is active")

producer: KafkaProducer | None = None
consumer_stop = threading.Event()
thread: threading.Thread | None = None


def ensure_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            acks="all",
        )


def handle_assignment(ev: dict[str, Any], received_wall_ms: int) -> None:
    drone_id = ev.get("drone_id", "unknown")
    status = {
        "event_type": "DroneMissionStarted",
        "drone_id": drone_id,
        "shipment_id": ev.get("shipment_id"),
        "order_id": ev.get("order_id"),
        "occurred_at_ms": received_wall_ms,
        "telemetry": {"phase": "in_flight_simulated"},
    }
    elapsed = max(0.0, (int(time.time() * 1000) - received_wall_ms) / 1000.0)
    DRONE_PUBLISH_SECONDS.observe(elapsed)

    ensure_producer()
    assert producer is not None
    producer.send(
        TOPIC_DRONE_STATUS,
        key=status["drone_id"].encode(),
        value=status,
    ).get(timeout=10)


def consume_loop():
    global producer
    while not consumer_stop.is_set():
        try:
            ensure_producer()
            consumer = KafkaConsumer(
                TOPIC_DRONE_ASSIGNMENTS,
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                group_id="drone-fleet-alpha",
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
        except KafkaError:
            DRONE_CONSUMER_ERRORS.labels("connect").inc()
            time.sleep(2)
            continue

        DRONE_READY.set(1)
        try:
            for msg in consumer:
                if consumer_stop.is_set():
                    break
                try:
                    ev = msg.value
                    recv_ms = int(time.time() * 1000)
                    if isinstance(ev, dict) and ev.get("event_type") == "DroneAssigned":
                        handle_assignment(ev, recv_ms)
                        DRONE_EVENTS_CONSUMED.labels("DroneAssigned").inc()
                    else:
                        DRONE_EVENTS_CONSUMED.labels("unknown").inc()
                except Exception:
                    DRONE_CONSUMER_ERRORS.labels("handler").inc()
        finally:
            consumer.close()
            DRONE_READY.set(0)


app = FastAPI(title="Drone Service")


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
    if producer:
        producer.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
