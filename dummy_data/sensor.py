import csv
import random

def generate_sensor_data(id_num, sens):
    data = []

    for i in range(0, sens):
        id = id_num + i
        name = f"{random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor'])}_{str(id)}"
        description = f"Sensor is a {name}"
        encodingType = "application/pdf"
        url = f"https://example.com/{name}-specs.pdf"
        metadata = "https://example.com/specs.pdf"
        properties = f"{{}}"

        # Append the row to the data list
        data.append([str(id), name, description, encodingType, metadata, properties])

    print("creating Sensor data...")
    # Write the data to a CSV file
    with open('data/Sensor.csv', 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description","encodingType", "metadata", "properties"])
        writer.writerows(data)