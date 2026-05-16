# 🛸 Shipping On-The-Air: Microservices Platform

Welcome to the **Shipping On-The-Air** platform! This is an event-driven, microservices-based system designed to manage autonomous drone delivery pipelines. The system features synchronous REST API entry points, asynchronous event propagation via Apache Kafka, and full-stack telemetry monitoring using Prometheus.

---

## 🏗️ System Architecture

The application is built using three primary microservices operating in harmony:
1. **Order Service (FastAPI):** Synchronous REST gateway that takes customer orders, tracks API metrics, and produces `OrderCreated` events.
2. **Shipment Orchestrator:** The central state machine. It consumes order events from Kafka, evaluates logistics, and coordinates fulfillment.
3. **Drone Service:** Manages autonomous drone fleet distribution, battery states, and dispatch pathing calculations.

---
<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/bf645c8e-4a28-492f-a9d0-b290cb315b48" />

<img width="1408" height="768" alt="Gemini_Generated_Image_wcwbkywcwbkywcwb" src="https://github.com/user-attachments/assets/5e3fd760-91c2-4247-ac12-a990839bd069" />


## 🚀 Getting Started

### Prerequisites
Make sure you have the following installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
* [Postman](https://www.postman.com/) or `curl` (to send test requests)

### 1. Spin Up the Platform
Clone the repository, navigate to the root directory, and launch all services containerized in the background:

```bash
cd shipping-on-the-air-assignment
docker compose up -d --build

## 🏗️ System Architecture

The application is built using three primary microservices operating in harmony:
1. **Order Service (FastAPI):** Synchronous REST gateway that takes customer orders, tracks API metrics, and produces `OrderCreated` events.
2. **Shipment Orchestrator:** The central state machine. It consumes order events from Kafka, evaluates logistics, and coordinates fulfillment.
3. **Drone Service:** Manages autonomous drone fleet distribution, battery states, and dispatch pathing calculations.

---

## 🚀 Getting Started

### Prerequisites
Make sure you have the following installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
* [Postman](https://www.postman.com/) or `curl` (to send test requests)

### 1. Spin Up the Platform
Clone the repository, navigate to the root directory, and launch all services containerized in the background:

```bash
cd shipping-on-the-air-assignment
docker compose up -d --build

```

This will automatically configure and launch:

* FastAPI Microservices (`order-service`, `drone-service`, `shipment-orchestrator`)
* Apache Kafka Broker & Zookeeper
* Prometheus Monitoring Server

---

## 🕹️ How to Use and Test the Application

Follow this end-to-end walkthrough to simulate real data flowing through the entire infrastructure.

### Step 1: Submit a Delivery Order

Send an HTTP `POST` request to the `order-service` to initialize a drone shipment.

* **URL:** `http://localhost:8001/orders`
* **Method:** `POST`
* **Headers:** `Content-Type: application/json`
* **Payload (JSON):**

```json
{
  "customer_id": "cust_danial_99",
  "parcel_weight_kg": 4.5,
  "destination_lat": 44.1383,
  "destination_lon": 12.2463
}

```

**Expected Response:** An HTTP `201 Created` status code and a unique tracked identification string:

```json
{
  "order_id": "07a120a1-f665-40b7-8c7c-972caf447ef1"
}

```

---

### Step 2: Intercept the Live Kafka Event Pipeline

When the order is created, the system securely streams an event log downstream. To watch these messages broadcast across the broker live, attach a console consumer directly to the cluster container:

```bash
docker exec -it shipping-on-the-air-assignment-kafka-1 \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic shipping.orders.created \
  --from-beginning

```

*(Note: If your container name differs slightly, find your exact name using `docker ps`)*

---

### Step 3: Monitor Live Telemetry Dashboards

The system comes pre-configured with Prometheus to automatically scrape container runtimes and application business metrics.

#### 1. Check Service Health Targets

Open your browser and navigate to: **`http://localhost:9090/targets`**

* Verify that `order-service`, `drone-service`, and `shipment-orchestrator` show a green **`UP`** status.

#### 2. Graph Performance Metrics

Head to **`http://localhost:9090`**, click the **Graph** tab, and enter these custom metrics into the query expression engine:

| Metric Name | Type | Description |
| --- | --- | --- |
| `order_api_http_requests_total` | Counter | Tracks total API requests by endpoint (`/orders`, `/metrics`, `/health`) and status code (`201`, `200`). |
| `order_api_request_duration_seconds_bucket` | Histogram | Measures the request latency across your REST endpoints. |
| `drone_active_missions` | Gauge | Displays the real-time count of virtual drones currently flying in the air. |

*Tip: For maximum precision right after running your postman requests, change the graph time window from `1h` (1 hour) to `1m` or `5m` to zoom right in on your data spikes!*

---

## 🛠️ Service Infrastructure Map

If you want to access raw runtime profiles or health paths directly, the ports are exposed on your host loopback adapter as follows:

* 📦 **Order Service API / Metrics:** `http://localhost:8001/metrics`
* 🎛️ **Shipment Orchestrator Metrics:** `http://localhost:8002/metrics`
* 🛸 **Drone Service Metrics:** `http://localhost:8003/metrics`
* 📊 **Prometheus Web UI Panel:** `http://localhost:9090`

---

## 🛑 Tearing Down

To stop the services and clean up running container memories, execute:

```bash
docker compose down

```

```

```
# shipping-on-the-air-assignment3
# shipping-on-the-air-assignment3
# shipping-on-the-air-assignment3
