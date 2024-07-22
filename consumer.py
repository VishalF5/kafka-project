from confluent_kafka import Consumer
import json
import pymysql
import json

with open('credential.json','r')as file:
    CREDENTIALS =json.load(file)

# MySQL configuration
MYSQL_HOST = CREDENTIALS["MYSQL_HOST"]
MYSQL_PORT = CREDENTIALS["MYSQL_PORT"]
MYSQL_USER = CREDENTIALS["MYSQL_USER"]
MYSQL_PASSWORD = CREDENTIALS["MYSQL_PASSWORD"]
MYSQL_DATABASE = CREDENTIALS["MYSQL_DATABASE"]
MYSQL_TABLE = CREDENTIALS["MYSQL_TABLE"]

# Kafka configuration
conf = {
    'bootstrap.servers': f'{CREDENTIALS["BOOTSTRAP_SERVER"]}:{CREDENTIALS["BOOTSTRAP_PORT"]}',
      'group.id': CREDENTIALS["CONSUMER_GROUP_ID"]  # Kafka broker(  # Consumer group ID
}

# Create Consumer instance
consumer = Consumer(conf)

# Kafka topic
topic = CREDENTIALS["TOPIC"]


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
        print(f"Error connecting to MySQL: {err}")
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
            print(f"Data inserted successfully!")
        except pymysql.Error as err:
            print(f"Error inserting data: {err}")
        finally:
            connection.close()


def consume_and_process():
    """
    Consumes messages from the topic and inserts data into MySQL.
    """
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages
        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            data = json.loads(msg.value().decode('utf-8'))
            insert_data_to_mysql(data)
            print(data)


if __name__ == '__main__':
    consume_and_process()
    consumer.close()
