# src/ingestion/jetstream_producer.py
import json
import logging
from kafka import KafkaProducer
from websockets import connect

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants
KAFKA_BROKER_URL = "broker:9092"
KAFKA_TOPIC = "text_posts"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


async def consume_jetstream():
    uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

    async with connect(uri) as websocket:
        async for message in websocket:
            # Parse message
            event = json.loads(message)

            # Safely access the 'record' key using .get()
            commit_record = event.get("commit", {}).get("record")

            # Check if commit_record exists and is of type 'app.bsky.feed.post'
            if commit_record and commit_record.get("$type") == "app.bsky.feed.post":
                text_data = {
                    "did": event["did"],
                    "createdAt": commit_record.get("createdAt"),
                    "text": commit_record.get(
                        "text", ""
                    ),  # Default to empty string if text is missing
                    "reply_to": commit_record.get("reply", {}),
                }

                # Send to Kafka
                producer.send(KAFKA_TOPIC, text_data)
                logger.info(f"Produced to Kafka: {text_data}")

            else:
                logger.warning(f"Unexpected message structure: {event}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(consume_jetstream())
