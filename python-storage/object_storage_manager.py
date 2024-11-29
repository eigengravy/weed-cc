import os
import time
import random
import asyncio
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import httpx  # For making HTTP requests to storage targets

# Base URLs for the three storage targets (without the endpoints)
STORAGE_TARGETS = [
    'http://0.0.0.0:8001',  # Base URL for storage target 1
    'http://0.0.0.0:8002',  # Base URL for storage target 2
    'http://0.0.0.0:8003',  # Base URL for storage target 3
]

PREFETCH_INTERVAL = 3 * 60  # 30 minutes in seconds
CHUNK_INTERVAL = 1* 60  # 10 minutes in seconds
METADATA_FILE = 'data_storage.json'

# Helper function to get the filename for the current time (10-minute chunks)
def get_file_name(current_time):
    timestamp = current_time.strftime('%Y-%m-%d_%H-%M')
    return f'{timestamp}.csv'

# Define a model for the CSV row data
class CSVRow(BaseModel):
    data: str  # Assuming each row is just a string of data (you can modify as needed)

# Object Storage Manager (OSM) class
class ObjectStorageManager:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.data_buffer = []  # Buffer to store CSV rows temporarily
        self.last_prefetch_time = time.time()
        self.prefetched_files = {} # file_name : data
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        """Load metadata from the JSON file."""
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, 'r') as f:
                return json.load(f)
        return []

    def _save_metadata(self):
        """Save metadata to the JSON file."""
        with open(METADATA_FILE, 'w') as f:
            json.dump(self.metadata, f, indent=4)

    def get_targets(self, file_name):
        metadata = self._load_metadata()
        targets = [entry['targets'] for entry in metadata if entry['file_name'] == file_name]
        if not targets:
            return random.sample(STORAGE_TARGETS, 2)
        return targets[0]

    async def distribute_data(self, data: str):
        """Distribute data to two storage targets via HTTP POST."""
        # Choose two random storage targets for this data chunk
        current_time = datetime.now()
        latest_file = self.metadata[-1] if self.metadata else None
        if latest_file:
            if current_time - datetime.strptime(latest_file["time"], '%Y-%m-%d %H:%M:%S') > timedelta(seconds=CHUNK_INTERVAL):
                file_name = latest_file["file_name"]
                targets = latest_file['targets']
            else:
                file_name = get_file_name(current_time)
                targets = random.sample(STORAGE_TARGETS, 2)
                self.metadata.append({
                    "time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "file_name": file_name,
                    "targets": targets
                })
                self._save_metadata()
        else: 
            file_name = get_file_name(current_time)
            targets = random.sample(STORAGE_TARGETS, 2)
            self.metadata.append({
                "time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                "file_name": file_name,
                "targets": targets
            })
            self._save_metadata()
        # Prepare the data to send (including the filename in the request body)
        payload = {
            "data": data,
            "filename": file_name  # Add filename to the payload to organize data at the target side
        }

        # Send the data to the selected storage targets
        async with self.lock:
            responses = []
            async with httpx.AsyncClient() as client:
                for target_base_url in targets:
                    try:
                        target_url = f"{target_base_url}/store_data/"
                        response = await client.post(target_url, json=payload)
                        response.raise_for_status()  # Raise error for any HTTP issues
                        responses.append({"target": target_base_url, "status": "success"})
                    except httpx.HTTPStatusError as e:
                        responses.append({"target": target_base_url, "status": f"failed ({e.response.status_code})"})
                    except Exception as e:
                        responses.append({"target": target_base_url, "status": f"failed ({str(e)})"})

            return responses

    async def prefetch_files(self):
        """Prefetch files using the metadata stored in data_storage.json."""
        metadata = self._load_metadata()
        current_time = datetime.now()

        # Filter out the metadata entries that are older than 30 minutes
        prefetched_files = []
        for entry in metadata:
            file_time = datetime.strptime(entry["time"], '%Y-%m-%d %H:%M:%S')
            if current_time - file_time <= timedelta(minutes=30):
                prefetched_files.append(entry)

        # Fetch the files from the storage targets
        for entry in prefetched_files:
            for target_base_url in entry["targets"]:
                file_path = f"{target_base_url}/fetch_data/{entry['file_name']}"
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(file_path)
                        response.raise_for_status()
                        # Store the file in the prefetched files list after successfully fetching it
                        self.prefetched_files[entry['file_name']] = response.text
                        print(f"Prefetching: {file_path} - Status: {response.status_code}")
                except httpx.HTTPStatusError as e:
                    print(f"File not found: {file_path} - Status: {e.response.status_code}")
                except Exception as e:
                    print(f"Error fetching file {file_path}: {e}")

# Create the FastAPI app and object storage manager
app = FastAPI()
osm = ObjectStorageManager()

# HTTP POST endpoint to receive and store CSV rows
@app.post("/store_data/")
async def store_data(row: CSVRow):
    try:
        result = await osm.distribute_data(row.data)
        return {"message": "Data stored successfully", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error storing data: {e}")

# HTTP GET endpoint to fetch data files from storage targets
@app.get("/fetch_data/{file_name}")
async def fetch_data(file_name: str):
    try:
        # Check if the file is in the prefetched files list
        if file_name in osm.prefetched_files:
            return {"message": f"File {file_name} already prefetched.", "file_name": file_name}

        # If not in the prefetched list, proceed with fetching the file
        metadata = osm._load_metadata()
        targets = [entry['targets'] for entry in metadata if entry['file_name'] == file_name]
        if not targets:
            raise HTTPException(status_code=404, detail="File not found.")
        
        # Fetch the file from the storage targets
        response = {}
        
        async with httpx.AsyncClient() as client:
            for target_base_url in set(targets):  # Flatten the list and ensure uniqueness
                target_url = f"{target_base_url}/fetch_data/{file_name}"
                # Ping target to check if online 
                try: 
                    resp = await client.get(target_url)
                    resp.raise_for_status()

                # These exceptions will return if this is the last target tried. I.e All targets failed
                except httpx.HTTPStatusError as e:
                    response = {"target": target_base_url, "status": f"failed ({e.resp.status_code})"}
                    continue
                except Exception as e:
                    response = {"target": target_base_url, "status": f"failed ({str(e)})"}
                    continue
                try:
                    resp = await client.get(target_url)
                    resp.raise_for_status()
                    responce = {"target": target_base_url, "status": "success", "text": resp.text}
                    break
                except httpx.HTTPStatusError as e:
                    response = {"target": target_base_url, "status": f"failed ({e.response.status_code})"}
                except Exception as e:
                    response = {"target": target_base_url, "status": f"failed ({str(e)})"}

        if response["status"] == "success":
            return {"message": "File fetched successfully", "response": response}
        else: 
            raise HTTPException(status_code=500, detail=f"Error fetching file: {response}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {e}")

# Endpoint to get the list of file names with timestamps
@app.get("/file_list/")
async def file_list():
    try:
        # Load the metadata from the file
        metadata = osm._load_metadata()
        # Return the file names and timestamps
        file_info = [
            {"file_name": entry["file_name"], "timestamp": entry["time"]}
            for entry in metadata
        ]
        return {"file_list": file_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving file list: {e}")

# Background task to periodically prefetch files every 30 minutes
async def periodic_prefetch():
    while True:
        current_time = time.time()
        if current_time - osm.last_prefetch_time > PREFETCH_INTERVAL:
            await osm.prefetch_files()
            osm.last_prefetch_time = current_time
        await asyncio.sleep(PREFETCH_INTERVAL)  # Sleep for 30 minutes before checking again

# Start the periodic prefetch task when the app starts
@app.on_event("startup")
async def start_periodic_prefetch():
    asyncio.create_task(periodic_prefetch())  # Run the prefetch in the background

# Run the server with `uvicorn`
# You can run this in the terminal using: `uvicorn filename:app --reload`
