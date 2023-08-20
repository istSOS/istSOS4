import csv
import random
import string

def generate_observedProperty_data(id_num,obs_prop):
    data = []

    for i in range(0, obs_prop):

        # Generate a unique ID
        id = id_num+i


        # Generate a unique name
        name = random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor']) + "_" + str(id)
        # Generate random properties
        defination = "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#"+name
        # Generate a random description
        description = name+" present in a substance or an object"
        properties=metadata=f'{{}}'




        # Append the row to the data list
        data.append([id, name, defination,description,properties])
        


# Generate 200 combinations of data

    #print(data)
    print("creating Observedproperty data...")
    # Write the data to a CSV file
    with open('data/ObservedProperty.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "defination", "description","properties"])
        writer.writerows(data)
