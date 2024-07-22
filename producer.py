from confluent_kafka import Producer
import json
import time
import random
with open('credential.json','r')as file:
    CREDENTIALS =json.load(file)
# Kafka configuration
conf = {
    'bootstrap.servers': f'{CREDENTIALS["BOOTSTRAP_SERVER"]}:{CREDENTIALS["BOOTSTRAP_PORT"]}',  # Kafka broker(s)
}


# Create Producer instance
producer = Producer(conf)

# Kafka topic
topic = CREDENTIALS["TOPIC"]


def delivery_report(err, msg):
    """Delivery report callback called once for each produced message."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_sample_data():
    """Generate sample data to send to Kafka."""
    return {
        'id': random.randint(1, 1000),
        'value': random.random(),
        'timestamp': int(time.time())
    }

# Produce messages
try:
    while True:
        message = generate_sample_data()
        producer.produce(topic, value=json.dumps(message), callback=delivery_report)
        producer.poll(0)  # Poll to handle delivery reports and other events
        time.sleep(1)  # Simulate delay between messages

    # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.flush()  # Ensure all messages are sent before exiting

