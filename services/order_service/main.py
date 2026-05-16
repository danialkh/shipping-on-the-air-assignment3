import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS_CREATED = "shipping.orders.created"

ORDER_API_REQUEST_DURATION = Histogram(
    "order_api_request_duration_seconds",
    "Latency of HTTP handlers on the order service",
    ["route", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

ORDER_API_REQUESTS = Counter(
    "order_api_http_requests_total",
    "HTTP requests to the order service",
    ["route", "method", "status_code"],
)

KAFKA_PUBLISH_FAILURES = Counter(
    "order_service_kafka_publish_failures_total",
    "Failures when publishing domain events to Kafka",
)


class OrderCreate(BaseModel):
    customer_id: str = Field(min_length=1)
    parcel_weight_kg: float = Field(gt=0, le=500)
    destination_lat: float
    destination_lon: float


class OrderResponse(BaseModel):
    order_id: str


producer: KafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )
    yield
    producer.flush()
    producer.close()


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.middleware("http")
async def observe_requests(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    route = request.scope.get("route")
    route_name = route.path if route else request.url.path
    ORDER_API_REQUEST_DURATION.labels(route_name, request.method).observe(elapsed)
    ORDER_API_REQUESTS.labels(route_name, request.method, str(response.status_code)).inc()
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/orders", response_model=OrderResponse, status_code=201)
def create_order(body: OrderCreate):
    """Synchronous REST entry point — publishes OrderCreated for the event-driven orchestrator."""
    oid = str(uuid.uuid4())
    event: dict[str, Any] = {
        "event_type": "OrderCreated",
        "order_id": oid,
        "occurred_at_ms": int(time.time() * 1000),
        "customer_id": body.customer_id,
        "parcel_weight_kg": body.parcel_weight_kg,
        "destination_lat": body.destination_lat,
        "destination_lon": body.destination_lon,
    }
    try:
        assert producer is not None
        future = producer.send(TOPIC_ORDERS_CREATED, key=oid.encode(), value=event)
        future.get(timeout=10)
    except (KafkaError, AssertionError):
        KAFKA_PUBLISH_FAILURES.inc()
        return Response(content="kafka_unavailable", status_code=503)
    return OrderResponse(order_id=oid)
