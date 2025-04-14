# src/ingestion/random_number_producer.py
import time
import random
import json
from kafka import KafkaProducer

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="broker:9092",  # Use your internal broker hostname and port
        value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize messages as JSON
    )

    print(producer.bootstrap_connected())

    topic = "random_numbers"

    while True:
        # Produce a random number between 1 and 100
        number = random.randint(1, 100)
        message = {"random_number": number}

        # Send the message to the Kafka topic
        producer.send(topic, message)
        print(f"Sent message: {message}")

        # Wait for 1 second before sending the next message
        time.sleep(1)

if __name__ == "__main__":
    main()