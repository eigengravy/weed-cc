import os
import json
import time
import asyncio
import httpx
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError

# Configuration
KAFKA_TOPIC = 'flagged-transactions'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'  # Kafka server(s)
STORAGE_MANAGER_URL = 'http://0.0.0.0:8000'  # Base URL of the Object Storage Manager
FETCH_INTERVAL = 30 * 60  # 30 minutes in seconds

# Initialize Kafka Consumer
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'transaction-group',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)

# Function to send data to the Object Storage Manager
async def send_to_storage_manager(data: str):
    payload = {
        "data": data,
        "filename": datetime.now().strftime('%Y-%m-%d_%H-%M') + '.csv'  # Create the filename based on current time
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f'{STORAGE_MANAGER_URL}/store_data/', json=payload)
            response.raise_for_status()
            return {"status": "success", "message": f"Data sent to {STORAGE_MANAGER_URL}/store_data/"}
        except httpx.HTTPStatusError as e:
            return {"status": "failed", "message": f"Failed with status {e.response.status_code}"}
        except Exception as e:
            return {"status": "failed", "message": f"Error: {str(e)}"}

# Function to fetch data and summarize flagged transactions every 30 minutes
async def fetch_and_summarize_files():
    while True:
        await asyncio.sleep(FETCH_INTERVAL)

        # Ping the storage manager to get the file names 


        # Fetch the files from storage manager
        for file_name in await fetch_files_from_metadata():
            try:
                async with httpx.AsyncClient() as client:
                    file_url = f"{STORAGE_MANAGER_URL}/fetch_data/{file_name}"
                    response = await client.get(file_url)
                    response.raise_for_status()
                    flagged_transactions = summarize_flagged_transactions(response.text)
                    print(f"File {file_name} from storage has {flagged_transactions} flagged transactions.")
            except Exception as e:
                print(f"Error fetching file {file_name} from storage: {e}")

# Fetch the list of files from metadata
async def fetch_files_from_metadata():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f'{STORAGE_MANAGER_URL}/file_list/')
            response.raise_for_status()
            data = response.json()
            # get the latest 3 files 
            file_names = sorted(data, key=lambda x: x['timestamp'], reverse=True)[:3]
            file_names = [file['file_name'] for file in file_names]
            return file_names

        except Exception as e:
            print(f"Error fetching metadata from storage: {e}")
            return []

# Summarize flagged transactions in a file (assuming it's a CSV file)
def summarize_flagged_transactions(file_content):
    flagged_transactions = 0
    for line in file_content.splitlines():
        # Assuming the file has a column `flagged` (adjust as needed)
        columns = line.split(",")
        if columns and columns[0].lower() == "true":  # Adjust the column index and condition as needed
            flagged_transactions += 1
    return flagged_transactions


# Function to consume Kafka messages and send to storage manager
def consume_kafka_messages():
    consumer.subscribe([KAFKA_TOPIC])

    while True:
        try:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages (1 second timeout)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Process the message and send it to the Object Storage Manager
            data = msg.value().decode('utf-8')
            response = asyncio.run(send_to_storage_manager(data))
            print(f"Message sent to storage: {response}")

        except Exception as e:
            print(f"Error consuming Kafka messages: {e}")
            time.sleep(5)

# Main function to start both jobs
async def main():
    # Start consuming Kafka messages
    asyncio.create_task(consume_kafka_messages())

    # Start fetching files and summarizing flagged transactions every 30 minutes
    await fetch_and_summarize_files()

if __name__ == "__main__":
    asyncio.run(main())
