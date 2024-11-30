import csv
import json
import time
import logging
import argparse
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

def create_topics(admin_client, topic_names, num_partitions=5, replication_factor=1):
    """Create Kafka topics."""
    topics = [
        NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
        for topic in topic_names
    ]
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        logging.info(f"Created topics: {', '.join(topic_names)}")
    except TopicAlreadyExistsError:
        logging.warning(f"Topics already exist: {', '.join(topic_names)}")
    except KafkaError as e:
        logging.error(f"Failed to create topics: {e}")
        raise

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Kafka Producer for Transactions CSV")
    parser.add_argument("--file", required=True, help="Path to the transactions CSV file")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between messages (seconds)")
    parser.add_argument("--create", action="store_true", help="Create Kafka topics before sending data")
    args = parser.parse_args()

    if args.create:
        logging.info("Creating Kafka topic...")
        admin_client = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers)
        create_topics(admin_client, ["transactions", "flagged_transactions"])
        admin_client.close()

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Read CSV file
    logging.info(f"Reading data from {args.file}")
    with open(args.file, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Send each record to Kafka
        for row in reader:
            try:
                producer.send("transactions", value=row)
                logging.info(f"Sent record: {row}")
                time.sleep(args.interval)
            except Exception as e:
                logging.error(f"Error sending record: {e}")

    # Close producer
    producer.close()
    logging.info("Finished sending all records.")

if __name__ == "__main__":
    main()
