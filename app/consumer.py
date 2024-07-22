from confluent_kafka import Consumer
import json
import pymysql
import json
import os
import logging


logging.basicConfig(
    filename=f'{os.getenv("LOGS_DIR")}/consumer.log',
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
CONSUMER_GROUP_ID = credentials["CONSUMER_GROUP_ID"] 
MYSQL_HOST = credentials["MYSQL_HOST"]
MYSQL_PORT = credentials["MYSQL_PORT"]
MYSQL_USER = credentials["MYSQL_USER"]
MYSQL_PASSWORD = credentials["MYSQL_PASSWORD"]
MYSQL_DATABASE = credentials["MYSQL_DATABASE"]
MYSQL_TABLE = credentials["MYSQL_TABLE"]
TOPIC = credentials["TOPIC"]

# Kafka configuration
conf = {
    'bootstrap.servers': f'{BOOTSTRAP_SERVER}:{BOOTSTRAP_PORT}',
      'group.id':  CONSUMER_GROUP_ID
}

# Create Consumer instance
consumer = Consumer(conf)

def connect_to_mysql():
    """
    Connects to the MySQL database and returns a connection object.
    """
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        return connection
    except pymysql.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
        return None


def insert_data_to_mysql(data):
    """
    Inserts data into the specified MySQL table.
    """
    connection = connect_to_mysql()
    if connection:
        try:
            cursor = connection.cursor()
            create_table = f'''
                            CREATE TABLE IF NOT EXISTS {MYSQL_TABLE}( 
                                    id INT, 
                                    value DECIMAL(20,20), 
                                    timestamp INT 
                                )
                            '''
            cursor.execute(create_table)
            connection.commit()

            # Prepare SQL statement based on your table schema
            sql = f"INSERT INTO {MYSQL_TABLE} (id, value, timestamp) VALUES (%s, %s, %s)"
            cursor.execute(sql, (data['id'], data['value'], data['timestamp']))
            connection.commit()
            logger.info(f"Data inserted successfully!")
        except pymysql.Error as err:
            logger.error(f"Error inserting data: {err}")
        finally:
            connection.close()


def consume_and_process():
    """
    Consumes messages from the topic and inserts data into MySQL.
    """
    consumer.subscribe([TOPIC])
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages
        if msg is None:
            continue
        elif msg.error():
            logger.error(f"Consumer error: {msg.error()}")
        else:
            data = json.loads(msg.value().decode('utf-8'))
            insert_data_to_mysql(data)


if __name__ == '__main__':
    consume_and_process()
    consumer.close()
