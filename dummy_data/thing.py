import csv
import random

def generate_thing_data(id_num, thing_num, loc_num):
    data = []

    for i in range(0, thing_num):
        id = id_num + i
        name = f"{random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor'])}_{str(id)}"
        description = f"A sensor that measures the {name} in a room"
        properties = f'{{"model": "Model-{id}", "manufacturer": "Manufacturer-{id}"}}'
        location_id = random.randint(1, loc_num)

        # Append the row to the data list
        data.append([str(id), name, description, properties, str(location_id)])

    print("creating Thing data...")
    # Write the data to a CSV file
    with open('data/Thing.csv', 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description", "properties", "location_id"])
        writer.writerows(data)