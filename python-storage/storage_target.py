from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import json
import csv
import httpx  # For making HTTP requests to storage targets




app = FastAPI()

# Directory to store files
STORAGE_DIR = 'storage_data'
MANAGER_URL = "http://localhost:8000"
MY_URL = os.environ['MY_URL']

# Ensure the directory exists
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

# Define the model for the incoming data
class DataPayload(BaseModel):
    data: dict  # JSON string representing row(s) to be written to CSV
    filename: str  # Name of the CSV file

@app.post("/store_data/")
async def store_data(payload: DataPayload):
    try:
        # Parse the data string as JSON
        data_row = payload.data
        # Ensure the data is a dict
        if not isinstance(data_row, dict):
            raise ValueError("Data must be a JSON dict")

        # Write rows to the CSV file
        file_path = os.path.join(STORAGE_DIR, payload.filename)
        file_exists = os.path.exists(file_path)

        with open(file_path, 'a', newline='\n') as f:
            writer = csv.writer(f)
            # Write header only if the file is being created
            if not file_exists:
                writer.writerow(data_row.keys())  # Write headers (keys of the first row)
                print(f"Wrote row: {data_row.keys()}")
            writer.writerow(data_row.values())  # Write row values

        return {"message": f"File {payload.filename} stored successfully."}
    except json.JSONDecodeError:
        print("jsohn error")
        raise HTTPException(status_code=400, detail="Invalid JSON in 'data'.")
    except Exception as e:
        print("error", e)
        raise HTTPException(status_code=500, detail=f"Error storing data: {str(e)}")

@app.get("/fetch_data/{filename}")
async def fetch_data(filename: str) -> list:
    try:
        file_path = os.path.join(STORAGE_DIR, filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found.")

        data = []
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                data.append(row)  # Add each row as a list of values

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@app.get("/go_down")
async def go_down():

    os.system(f"rm -rf {STORAGE_DIR}")
    return {"message": "I am down"}

@app.get("/go_up")
async def get_up():
    # Curl the manager to get the storage target to go up
    async with httpx.AsyncClient() as client:
        resp = await client.get(MANAGER_URL + "/buddy_sync/" + MY_URL)
        resp.raise_for_status()
        file_data = await resp.json()
        # Save the files to the storage target
        for file in file_data:
            file_path = os.path.join(STORAGE_DIR, file['filename'])
            with open(file_path, 'w') as f:
                f.write(file['data'])
        # except httpx.HTTPStatusError as e:
        #     response = {"target": target_base_url, "status": f"failed ({e.response.status_code})"}
        # except Exception as e:
        #     response = {"target": target_base_url, "status": f"failed ({str(e)})"}
    return {"message": "I am up"}

# To run this: `uvicorn storage_target:app --reload`
