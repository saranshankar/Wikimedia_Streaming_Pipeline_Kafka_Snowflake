import json
import time
import snowflake.connector
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        consumer = KafkaConsumer(
            'wikimedia',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='wikimedia-group'
        )

        print("Connected to Kafka!")
        break

    except NoBrokersAvailable:
        print("Kafka not ready. Retrying in 5 seconds...")
        time.sleep(5)

while True:
    try:
        conn = snowflake.connector.connect(
            user='your user name',
            password='your password',
            account='your account',
            warehouse='COMPUTE_WH',
            database='WIKIMEDIA_DB',
            schema='PUBLIC'
        )

        cur = conn.cursor()

        print("Connected to Snowflake!")
        break

    except Exception as e:
        print("Snowflake not ready:", e)
        time.sleep(5)

cur.execute("""
CREATE TABLE IF NOT EXISTS WIKIMEDIA_EVENTS (
    ID INTEGER AUTOINCREMENT,
    TITLE STRING,
    USERNAME STRING,
    WIKI STRING,
    EVENT_TYPE STRING,
    EVENT_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

conn.commit()

for message in consumer:

    event = message.value

    try:

        title = event.get("title")
        username = event.get("user")
        wiki = event.get("wiki")
        event_type = event.get("type")

        cur.execute(
            """
            INSERT INTO WIKIMEDIA_EVENTS
            (TITLE, USERNAME, WIKI, EVENT_TYPE)
            VALUES (%s, %s, %s, %s)
            """,
            (title, username, wiki, event_type)
        )

        conn.commit()

        print(
            f"Inserted: {title} | {username}"
        )

    except Exception as e:
        print("Insert Error:", e)