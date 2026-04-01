import os
import time
import random
import json
from dotenv import load_dotenv

# External systems
from kafka import KafkaProducer
import pika
import psycopg2
import redis
from pymongo import MongoClient

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

load_dotenv()

SERVICE_NAME = os.getenv("SERVICE_NAME")
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", 0.2))

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(SERVICE_NAME)

# -------------------------
# Connections
# -------------------------

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

redis_client = redis.Redis(host=os.getenv("REDIS_HOST"))

mongo = MongoClient(os.getenv("MONGO_URI"))
mongo_db = mongo.test

pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

rabbit_conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST"))
)
rabbit_channel = rabbit_conn.channel()
rabbit_channel.queue_declare(queue="test")


# -------------------------
# Logging helper
# -------------------------

def log_event(resource, event, severity="INFO", extra=None):
    log = {
        "resource": resource,
        "event": event,
        "severity": severity,
        "timestamp": time.time(),
        "extra": extra or {}
    }
    print(json.dumps(log))


# =========================
# NORMAL BEHAVIORS
# =========================

def kafka_normal():
    with tracer.start_as_current_span("kafka_normal"):
        producer.send(os.getenv("KAFKA_TOPIC"), {"msg": "normal"})
        log_event("kafka", "message_sent")


def redis_normal():
    with tracer.start_as_current_span("redis_normal"):
        redis_client.set("key", "value")
        redis_client.get("key")
        log_event("redis", "normal_get_set")


def postgres_normal():
    with tracer.start_as_current_span("postgres_normal"):
        cur = pg_conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        log_event("postgres", "simple_query")


def mongo_normal():
    with tracer.start_as_current_span("mongo_normal"):
        mongo_db.test.insert_one({"x": random.randint(1, 100)})
        log_event("mongo", "insert")


def rabbitmq_normal():
    with tracer.start_as_current_span("rabbitmq_normal"):
        rabbit_channel.basic_publish(exchange="", routing_key="test", body="hello")
        log_event("rabbitmq", "message_published")


def cpu_normal():
    with tracer.start_as_current_span("cpu_normal"):
        sum(i*i for i in range(1000))
        log_event("openshift", "normal_cpu")


# =========================
# ANOMALIES
# =========================

def cpu_spike():
    with tracer.start_as_current_span("cpu_spike"):
        for _ in range(10**7):
            pass
        log_event("openshift", "cpu_spike", "WARN")


def redis_miss():
    with tracer.start_as_current_span("redis_miss"):
        redis_client.get("nonexistent_key")
        log_event("redis", "key_miss", "WARN")


def postgres_slow_query():
    with tracer.start_as_current_span("postgres_slow"):
        cur = pg_conn.cursor()
        cur.execute("SELECT pg_sleep(5)")
        cur.close()
        log_event("postgres", "slow_query", "WARN")


def kafka_retry():
    with tracer.start_as_current_span("kafka_retry"):
        try:
            producer.send("invalid-topic", {"bad": "data"})
        except Exception:
            log_event("kafka", "retry_error", "ERROR")


def rabbitmq_queue_buildup():
    with tracer.start_as_current_span("rabbitmq_buildup"):
        for _ in range(10000):
            rabbit_channel.basic_publish(exchange="", routing_key="test", body="spam")
        log_event("rabbitmq", "queue_buildup", "WARN")


def mongo_slow_query():
    with tracer.start_as_current_span("mongo_slow"):
        list(mongo_db.test.find({"x": {"$gt": 0}}))
        log_event("mongo", "collection_scan", "WARN")


# =========================
# Scheduler
# =========================

normal_funcs = [
    kafka_normal,
    redis_normal,
    postgres_normal,
    mongo_normal,
    rabbitmq_normal,
    cpu_normal
]

anomaly_funcs = [
    cpu_spike,
    redis_miss,
    postgres_slow_query,
    kafka_retry,
    rabbitmq_queue_buildup,
    mongo_slow_query
]


def run():
    while True:
        if random.random() < ANOMALY_RATE:
            func = random.choice(anomaly_funcs)
        else:
            func = random.choice(normal_funcs)

        try:
            func()
        except Exception as e:
            log_event("system", "exception", "ERROR", {"error": str(e)})

        time.sleep(random.uniform(
            float(os.getenv("SLEEP_MIN")),
            float(os.getenv("SLEEP_MAX"))
        ))


if __name__ == "__main__":
    run()