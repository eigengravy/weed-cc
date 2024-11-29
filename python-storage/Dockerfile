# Use official Python image from DockerHub
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to install dependencies
COPY requirements.txt /app/

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
COPY . /app/

# Expose the port FastAPI will run on
EXPOSE 8000

# Set environment variable for Uvicorn to run in production mode
ENV UVICORN_CMD="uvicorn"

# Default command to run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

