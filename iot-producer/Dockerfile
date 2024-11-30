# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the local files to the container
COPY . /app

# Create a virtual environment and install dependencies
RUN python -m venv venv && \
    . venv/bin/activate && \
    pip install --no-cache-dir --upgrade pip && \
    pip install -r requirements-dev.txt

# Specify the command to run the Kafka producer
ENTRYPOINT [ "python3", "produce_transactions.py" ]
