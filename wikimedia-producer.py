import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Connect to Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Producer connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("Kafka not ready...")
        time.sleep(5)

while True:
    try:
        print("Connecting to Wikimedia Stream...")

        response = requests.get(
            STREAM_URL,
            stream=True,
            timeout=30,
            headers={
                "Accept": "text/event-stream",
                "User-Agent": "Kafka-Wikimedia-Project"
            }
        )

        print("Connected to Wikimedia Stream")

        for line in response.iter_lines():

            if not line:
                continue

            line = line.decode("utf-8")

            if line.startswith("data:"):
                try:
                    data = json.loads(line[5:].strip())

                    producer.send("wikimedia", value=data)
                    producer.flush()

                    print(
                        f"Sent: {data.get('title', 'N/A')} | "
                        f"{data.get('user', 'N/A')}"
                    )

                except Exception as e:
                    print("Event Error:", e)

    except Exception as e:
        print("Stream Error:", e)
        time.sleep(5)