# Use an official Python runtime as a parent image
FROM python:3.12.4-alpine3.20

# Set the working directory to /app
WORKDIR /dummy_data

# Copy the current directory contents into the container at /app
COPY . /dummy_data

# Run the command to install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt


