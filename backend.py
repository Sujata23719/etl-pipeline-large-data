from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_mail import Mail, Message

import threading
import time
import requests
from utils import CONSUMER_HEARTBEAT_URL, EMAIL_TOPIC, KAFKA_BROKER, KAFKA_TOPIC, create_producer, create_consumer, consume_messages as consume_kafka_messages

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# Consumer Status Tracking
consumer_status = {"alive": False}

producer = create_producer(KAFKA_BROKER)
consumer = create_consumer(KAFKA_BROKER, EMAIL_TOPIC, "email_group")

# Flask-Mail Configuration
app.config["MAIL_SERVER"] = "smtp.gmail.com"
app.config["MAIL_PORT"] = 587
app.config["MAIL_USE_TLS"] = True
app.config["MAIL_USERNAME"] = ""  # Change this
app.config["MAIL_PASSWORD"] = ""  # Change this
app.config["MAIL_DEFAULT_SENDER"] = "your-email@gmail.com"
email_target = ""  # Change recipient

mail = Mail(app)

logs = []

@app.route("/process", methods=["POST"])
def send_message():
    data = request.json
    message = data.get("message", "")
    
    if not message:
        return jsonify({"status": "error", "message": "No message provided"}), 400

    producer.send(KAFKA_TOPIC, message)
    producer.flush()

    return jsonify({"status": "Message sent successfully!"})

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    """Heartbeat endpoint to check consumer status"""
    response = jsonify({"consumer_status": "running" if consumer_status["alive"] else "stopped"})

    # Explicitly set CORS headers
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type")
    response.headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    return response

@app.route("/logs", methods=["GET"])
def get_logs():
    return jsonify(logs)

@app.route("/logs", methods=["POST"])
def add_log():
    data = request.json
    log = data.get("log", "")
    if not log:
        return jsonify({"status": "error", "message": "No log provided"}),

    logs.append(log)
    return jsonify({"status": "Log added successfully!"})

def check_consumer():
    """Check consumer health by calling its heartbeat API"""
    while True:
        try:
            res = requests.get(CONSUMER_HEARTBEAT_URL, timeout=3)
            if res.status_code == 200:
                data = res.json()
                consumer_status["alive"] = data.get("consumer_status") == "running"
            else:
                consumer_status["alive"] = False
            print("Consumer is up and running!")
        except requests.RequestException:
            consumer_status["alive"] = False
            print("Consumer is down!")
        time.sleep(5)  

def monitor_consumer():
    # Start the consumer heartbeat check in a separate thread
    heartbeat_thread = threading.Thread(target=check_consumer, daemon=True)
    heartbeat_thread.start()

def send_email(subject, body, recipients):
    """Send an email notification"""
    with app.app_context():
        try:
            msg = Message(subject, recipients=recipients, body=body)
            mail.send(msg)
            print(f"Email sent to {recipients}")
        except Exception as e:
            print(f"Failed to send email: {e}")

def consume_messages():
    """Listen to Kafka topic and send email notifications"""
    consume_kafka_messages(consumer, lambda msg: process_email(msg))

def process_email(message):
    email_subject = "New Kafka Message Received"
    email_body = f"New message from Kafka: {message}"
    send_email(email_subject, email_body, [email_target])  

consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

if __name__ == "__main__":
    monitor_consumer()
    app.run(port=5000)
