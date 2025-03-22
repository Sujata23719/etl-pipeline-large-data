from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka Configuration
KAFKA_TOPIC = "message_topic"
EMAIL_TOPIC = "email_topic"
KAFKA_BROKER = "localhost:9092"
CONSUMER_HEARTBEAT_URL = "http://localhost:5001/heartbeat"
MIDDLEWARE_URL = "http://127.0.0.1:5000"

def create_producer(bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_message(producer, topic, value):
    producer.send(topic, value)
    producer.flush()

def create_consumer(bootstrap_servers, topic, group_id):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def consume_messages(consumer, callback):
    for message in consumer:
        callback(message.value)