# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory to /app
WORKDIR /dummy_data

# Copy the current directory contents into the container at /app
COPY . /dummy_data

# Run the command to install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run hello.py when the container launches
# CMD ["python3", "gen_data.py"]


