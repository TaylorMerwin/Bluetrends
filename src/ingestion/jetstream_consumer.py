# src/ingestion/jetstream_consumer.py
import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants
KAFKA_BROKER_URL = "broker:9092"
KAFKA_TOPIC = "text_posts"

def main():
    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        auto_offset_reset='earliest',  # start at the earliest message
        enable_auto_commit=True,
        group_id='bluesky-consumer-group',
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    
    logger.info("Kafka Consumer is connected and listening for messages...")
    
    # Consume messages
    for message in consumer:
        try:
            record = message.value
            logger.info("Received message: %s", record)
            
            # Process the message (for example, extract fields and use them)
            did = record.get("did")
            created_at = record.get("createdAt")
            text = record.get("text")
            reply_to = record.get("reply_to")
            
            
        except Exception as e:
            logger.error("Error processing message: %s", str(e))

if __name__ == "__main__":
    main()