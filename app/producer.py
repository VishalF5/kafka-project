from confluent_kafka import Producer
import json
import time
import random
import os
import logging


logging.basicConfig(
    filename=f'{os.getenv("LOGS_DIR")}/producer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


pwd = os.path.dirname(os.path.abspath(__file__))  # path to this code's dir
root_dir = os.path.abspath(os.path.join(pwd, ".."))
credentials_location = os.path.join(root_dir, "config", "credential.json")

try:
    with open(credentials_location) as f:
            credentials = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"Credentials File Not Found At Location {credentials_location}")

BOOTSTRAP_SERVER = credentials["BOOTSTRAP_SERVER"]
BOOTSTRAP_PORT = credentials["BOOTSTRAP_PORT"]
TOPIC = credentials["TOPIC"]


conf = {
    'bootstrap.servers': f'{BOOTSTRAP_SERVER}:{BOOTSTRAP_PORT}',
}


# Create Producer instance
producer = Producer(conf)

def delivery_report(err, msg):
    """Delivery report callback called once for each produced message."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_sample_data():
    """Generate sample data to send to Kafka."""
    return {
        'id': random.randint(1, 1000),
        'value': random.random(),
        'timestamp': int(time.time())
    }

if __name__ == "__main__":
    # Produce messages
    try:
        while True:
            message = generate_sample_data()
            producer.produce(TOPIC, value=json.dumps(message), callback=delivery_report)
            producer.poll(0)  # Poll to handle delivery reports and other events
            time.sleep(1)  # Simulate delay between messages

        # Wait for any outstanding messages to be delivered and delivery reports to be received
            producer.flush()
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting

