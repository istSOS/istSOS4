import csv
import random

def generate_observedProperty_data(id_num,obs_prop):
    data = []

    for i in range(0, obs_prop):
        id = id_num + i
        name = f"{random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor'])}_{str(id)}"
        definition = f"http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#{name}"
        description = f"{name} present in a substance or an object"
        properties = f"{{}}"

        # Append the row to the data list
        data.append([id, name, definition, description, properties])

    print("creating ObservedProperty data...")
    # Write the data to a CSV file
    with open('data/ObservedProperty.csv', 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "definition", "description","properties"])
        writer.writerows(data)