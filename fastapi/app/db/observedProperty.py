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
        # Generate random properties
        defination = "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#"+name
        # Generate a random description
        description = name+" present in a substance or an object"




        # Append the row to the data list
        data.append([id, name, defination,description])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('ObservedProperty.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "name", "defination", "description"])
    writer.writerows(dummy_data)
