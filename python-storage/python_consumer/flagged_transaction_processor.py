import asyncio
import httpx
from confluent_kafka import Consumer, KafkaException
from datetime import datetime
import json

# Configuration
KAFKA_TOPIC = 'flagged_transactions'
# KAFKA_TOPIC = 'transactions'
KAFKA_BOOTSTRAP_SERVERS = '10.8.1.96:29092'
STORAGE_MANAGER_URL = 'http://10.8.1.95:8000'
STORE_ENDPOINT = f"{STORAGE_MANAGER_URL}/store_data/"
FETCH_ENDPOINT = f"{STORAGE_MANAGER_URL}/fetch_data/"
FILES_ENDPOINT = f"{STORAGE_MANAGER_URL}/file_list/"
FETCH_INTERVAL = 3 * 10  # 30 minutes in seconds


async def send_to_storage_manager(data):
    """Send a row of data to the object storage manager."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(STORE_ENDPOINT, json={"data": json.loads(data)}, headers={
                "Content-type": "application/json"
            })
            response.raise_for_status()
        except Exception as e:
            print(f"Error sending data to storage manager: {e}")


async def consume_kafka_messages():
    """Consume messages from Kafka and send them to the storage manager."""
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'transaction-processor-group',
        'auto.offset.reset': 'earliest',
    })

    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {message.error()}")
                    continue
            data = message.value().decode('utf-8')
            # print(f"Received message: {data}")
            await send_to_storage_manager(data)
    except Exception as e:
        print(f"Error consuming Kafka messages: {e}")
    finally:
        consumer.close()


async def fetch_and_summarize():
    """Fetch prefetched files every 30 minutes and summarize transactions."""
    await asyncio.sleep(FETCH_INTERVAL / 100)
    while True:
        print(f"Fetching files at {datetime.now()}")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(FILES_ENDPOINT)
                response.raise_for_status()
                print("File list:", response.json()['file_list'])
                files = sorted(response.json()['file_list'], key=lambda x: x['timestamp'], reverse=True)[:3]
                transaction_count = 0

                for file in files:
                    print(f"Processing file: {file['file_name']}")
                    response = await client.get(FETCH_ENDPOINT + file['file_name'])
                    response.raise_for_status()
                    resp = response.json().get("response", None)
                    if resp is None:
                        print(response.json())
                        continue
                    # print("resp:", resp)
                    file_data = resp['text']
                    # print(file_data['response'].keys(), file_data['message'])
                    print("file", file_data)
                    transaction_count += len(file_data)

                print(f"Summary: {transaction_count} transactions in the last 30 seconds.")
        except Exception as e:
            print(f"Error fetching or summarizing files: {e}")

        await asyncio.sleep(FETCH_INTERVAL)


async def main():
    """Run Kafka consumer and fetch summarizer concurrently."""
    await asyncio.gather(
        consume_kafka_messages(),
        fetch_and_summarize()
    )


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully.")

