from confluent_kafka import Producer
import json
import time
import os
import logging
import requests

logging.basicConfig(
    filename=f'{os.getenv("LOGS_DIR")}/producer.log',
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


pwd = os.path.dirname(os.path.abspath(__file__))  # path to this code's dir
root_dir = os.path.abspath(os.path.join(pwd, ".."))
credentials_location = os.path.join(root_dir, "config", "credential.json")

try:
    with open(credentials_location) as f:
        credentials = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(
        f"Credentials File Not Found At Location {credentials_location}"
    )

BOOTSTRAP_SERVER = credentials["BOOTSTRAP_SERVER"]
BOOTSTRAP_PORT = credentials["BOOTSTRAP_PORT"]
TOPIC = credentials["TOPIC"]
API_TOKEN = credentials["API_TOKEN"]


conf = {
    "bootstrap.servers": f"{BOOTSTRAP_SERVER}:{BOOTSTRAP_PORT}",
}


# Create Producer instance
producer = Producer(conf)


def delivery_report(err, msg):
    """Delivery report callback called once for each produced message."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


if __name__ == "__main__":
    # Produce messages
    try:
        while True:
            url = f"https://eodhd.com/api/real-time/AAPL.US?s=VTI,EUR.FOREX&api_token={API_TOKEN}&fmt=json"
            stocks = requests.get(url).json()

            for stock in stocks:
                print(stock)
                producer.produce(
                    TOPIC, value=json.dumps(stock), callback=delivery_report
                )
                producer.poll(0)
                producer.flush()
            time.sleep(60)

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
