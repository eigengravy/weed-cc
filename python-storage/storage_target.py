from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

app = FastAPI()

# Directory to store files
STORAGE_DIR = 'storage_data'

# Ensure the directory exists
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

# Define the model for the incoming data
class DataPayload(BaseModel):
    data: str
    filename: str

@app.post("/store_data/")
async def store_data(payload: DataPayload):
    try:
        # Store data to the file
        file_path = os.path.join(STORAGE_DIR, payload.filename)
        with open(file_path, 'w') as f:
            f.write(payload.data)
        return {"message": f"File {payload.filename} stored successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error storing data: {str(e)}")

@app.get("/fetch_data/{filename}")
async def fetch_data(filename: str):
    try:
        file_path = os.path.join(STORAGE_DIR, filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found.")
        
        with open(file_path, 'r') as f:
            data = f.read()
        return {"filename": filename, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

# To run this: `uvicorn storage_target:app --reload`
