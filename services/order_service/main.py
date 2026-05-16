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

# --- FIXED POSTGRESQL IMPORTS ---
import psycopg
from psycopg_pool import ConnectionPool  # Fixes the ConnectionPool AttributeError
from psycopg.rows import dict_row        # Fixes the Pylance attribute warning

# --- Configuration Environment Variables ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS_CREATED = "shipping.orders.created"

DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "shipping_records")
DB_USER = os.environ.get("POSTGRES_USER", "admin")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "secretpassword")

DB_CONN_STRING = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

# --- Prometheus Metrics Setup ---
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

# --- Pydantic Data Models ---
class OrderCreate(BaseModel):
    customer_id: str = Field(min_length=1)
    parcel_weight_kg: float = Field(gt=0, le=500)
    destination_lat: float
    destination_lon: float


class OrderResponse(BaseModel):
    order_id: str


# --- Global Service Interfaces ---
producer: KafkaProducer | None = None
db_pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, db_pool
    
    # 1. Initialize PostgreSQL Connection Pool
    db_pool = ConnectionPool(conninfo=DB_CONN_STRING, min_size=2, max_size=10)
    
    # 2. Automatically verify database connection & create schema if missing
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id UUID PRIMARY KEY,
                    customer_id VARCHAR(255) NOT NULL,
                    parcel_weight_kg NUMERIC(6, 2) NOT NULL,
                    destination_lat NUMERIC(9, 6) NOT NULL,
                    destination_lon NUMERIC(9, 6) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

    # 3. Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )
    
    yield
    
    # --- Clean Teardown Routine ---
    if producer:
        producer.flush()
        producer.close()
    if db_pool:
        db_pool.close()


app = FastAPI(title="Order Service", lifespan=lifespan)


# --- Monitoring Middleware ---
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


# --- REST API Routing Endpoints ---
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/orders", response_model=OrderResponse, status_code=201)
def create_order(body: OrderCreate):
    """Saves order data to PostgreSQL securely, then dispatches an Event-Driven notification to Kafka."""
    oid = str(uuid.uuid4())
    
    # 1. Relational Database Persistence Layer
    try:
        assert db_pool is not None
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (order_id, customer_id, parcel_weight_kg, destination_lat, destination_lon)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (oid, body.customer_id, body.parcel_weight_kg, body.destination_lat, body.destination_lon)
                )
                conn.commit()
    except Exception:
        # Halt execution and return 500 if database commit fails to guarantee consistency
        return Response(content="database_storage_failure", status_code=500)

    # 2. Event-Driven Messaging Broadcast Layer
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