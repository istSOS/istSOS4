import time

import requests

# Set variables from user input
istsos_endopint = input(
    "Enter the istsos endpoint (default: http://localhost:8018/istsos4/v1.1): "
)
if istsos_endopint.strip() == "":
    istsos_endopint = "http://localhost:8018/istsos4/v1.1"
istsos_username = input("Enter your istsos username: ")
if istsos_username.strip() == "":
    print("You must enter a username")
    exit()
istsos_password = input("Enter your istsos password: ")
if istsos_password.strip() == "":
    print("You must enter a password")
    exit()
datastream_id = input("Enter the datastream ID: ")
if datastream_id.strip() == "":
    print("You must enter a datastream ID")
    exit()
else:
    try:
        datastream_id = int(datastream_id)
    except ValueError:
        print("Datastream ID must be an integer")
        exit()
frequency = input(
    "Enter the frequency of the stream in seconds (default: 5): "
)
if frequency.strip() == "":
    frequency = 5
else:
    try:
        frequency = int(frequency)
    except ValueError:
        print("Frequency must be an integer")
        exit()
latitude = input("Enter the latitude of your position (default: 45.8): ")
if latitude.strip() == "":
    latitude = 45.8
else:
    try:
        latitude = float(latitude)
    except ValueError:
        print("Latitude must be a float")
        exit()
longitude = input("Enter the longitude of your position (default: 9.1): ")
if longitude.strip() == "":
    longitude = 9.1
else:
    try:
        longitude = float(longitude)
    except ValueError:
        print("Longitude must be a float")
        exit()

# basic observation schema
observation = {
    "phenomenonTime": "2015-03-03T00:00:00Z",
    "resultTime": "2015-03-03T00:00:00Z",
    "result": 3,
    "resultQuality": "100",
    "Datastream": {"@iot.id": 1},
}

# Login to istsos and get token
req = requests.post(
    f"{istsos_endopint}/Login",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"username": istsos_username, "password": istsos_password},
)
if req.status_code != 200:
    print("Login failed")
    exit()
else:
    print("Login successful")
token_obj = req.json()

# API to get weather data
response = requests.get(
    "https://archive-api.open-meteo.com/v1/archive",
    params={
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "2024-01-21",
        "end_date": "2024-11-31",
        "temperature_unit": "celsius",
        "hourly": "temperature_2m",
    },
)
data = response.json()
temperatures = data["hourly"]
print(
    "Data received from the API: https://archive-api.open-meteo.com/v1/archive"
)

# Create a fake data
idx = 0
while True:
    observation["phenomenonTime"] = temperatures["time"][idx]
    observation["resultTime"] = temperatures["time"][idx]
    observation["result"] = temperatures["temperature_2m"][idx]
    observation["Datastream"]["@iot.id"] = datastream_id
    response = requests.post(
        f"{istsos_endopint}/Observations",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token_obj['access_token']}",
            "commit-message": "Add fake observation for tutorial",
        },
        json=observation,
    )
    if response.status_code == 201:
        print(f"Data sent: {observation}")
    else:
        print(f"Failed to send data: {observation}")
        print(response.text)
    print("Waiting for the next data...")
    print("")
    idx += 1
    time.sleep(frequency)
