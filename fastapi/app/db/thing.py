import csv
import random
import string

def generate_dummy_data():
    data = []

    for i in range(1, 201):

        # Generate a unique ID
        id = i


        # Generate a unique name
        name = random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor']) + "_" + str(i)

        # Generate a random description
        description = f"A sensor that measures the {name} in a room"

        # Generate random properties
        properties = f'{{"model": "Model-{i}", "manufacturer": "Manufacturer-{i}"}}'

        # Generate a random location ID
        location_id = random.randint(1, 200)

        # Append the row to the data list
        data.append([str(id), name, description, properties, str(location_id)])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Thing.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "name", "description", "properties", "location_id"])
    writer.writerows(dummy_data)
