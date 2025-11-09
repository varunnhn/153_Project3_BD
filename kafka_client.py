# kafka_client.py
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

log = logging.getLogger("kafka_client")
log.setLevel(logging.INFO)

DEFAULT_BOOTSTRAP = "localhost:9092"

def make_producer(bootstrap_servers=None):
    bs = bootstrap_servers or DEFAULT_BOOTSTRAP
    return KafkaProducer(
        bootstrap_servers=bs,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        retries=5,
        linger_ms=10
    )

def produce_task(producer, topic, key, message):
    try:
        fut = producer.send(topic, key=key, value=message)
        fut.get(timeout=10)
        log.info("Produced message key=%s to topic=%s", key, topic)
    except KafkaError as e:
        log.exception("Failed to produce message: %s", e)
        raise

def make_consumer(topic, group_id, bootstrap_servers=None, auto_offset_reset='earliest'):
    bs = bootstrap_servers or DEFAULT_BOOTSTRAP
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bs,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

