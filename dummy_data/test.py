# import os
# from dotenv import load_dotenv

# # Specify the path to the .env file
# dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# # Load the environment variables from the .env file
# load_dotenv(dotenv_path)

# # Access environment variables
# # pg_host = os.getenv('POSTGRES_HOST')
# pg_host = os.getenv('HOSTNAME')
# pg_port = os.getenv('POSTGRES_PORT')

# print(f"postgres host: {pg_host}")
# print(f"postgres port: {pg_port}")

# # import docker

# # client = docker.DockerClient()
# # container = client.containers.get(istsos-miu-database)
# # ip_add = container.attrs['NetworkSettings']['IPAddress']
# # print(ip_add)

# import netifaces as ni

# import docker

# client = docker.DockerClient()
# container = client.containers.get("database")
# ip_add = container.attrs['NetworkSettings']['IPAddress']
# print(ip_add)
# print("#############################")

# import os
# folder_path = 'data'

# # Get a list of all files in the folder
# file_list = os.listdir(folder_path)
# print(file_list)
# # Iterate through the files and delete CSV files
# for filename in file_list:
#     if filename.endswith('.csv'):
#         file_path = os.path.join(folder_path, filename)
#         os.remove(file_path)


# # Generate a unique name
# start_date = '2023-01-01'
# end_date = '2023-12-31'
# start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
# end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

# # Calculate the time range in seconds
# time_range = (end_datetime - start_datetime).total_seconds()

# # Generate a random number of seconds within the time range
# random_seconds = random.randint(0, int(time_range))

# # Add the random number of seconds to the start datetime
# time = start_datetime + timedelta(seconds=random_seconds)    

# phenomenonTime = time
# resultTime =time


from datetime import datetime
from isodate import parse_datetime, parse_duration
import iso8601
# Specify the start datetime and timestep duration
start_datetime_str = '2020-01-01T12:00:00.000+01:00'
timestep_str = 'PT10M'

# Parse the start datetime and timestep duration using iso8601
start_datetime = iso8601.parse_date(start_datetime_str)
print(start_datetime)
timestep_duration = parse_duration(timestep_str)
print(timestep_duration)
# Generate timestamps using the specified timestep
timestamps = []
current_time = start_datetime

# Generate timestamps until a certain point (e.g., 10 timestamps)
for _ in range(10):
    timestamps.append(current_time.isoformat())
    current_time += timestep_duration

# Print the generated timestamps
for timestamp in timestamps:
    print(timestamp)
