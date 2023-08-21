import csv
import random
import string

def generate_thing_data(id_num,thing_num,loc_num):
    data = []
  
    new_var = thing_num
    for i in range(0, new_var):

        # Generate a unique ID
        id = id_num+i


        # Generate a unique name
        name = random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor']) + "_" + str(id)

        # Generate a random description
        description = f"A sensor that measures the {name} in a room"

        # Generate random properties
        properties = f'{{"model": "Model-{id}", "manufacturer": "Manufacturer-{id}"}}'

        # Generate a random location ID
        location_id =random.randint(1, loc_num)


        # Append the row to the data list
        data.append([str(id), name, description, properties, str(location_id)])
        


# Generate 200 combinations of data

    print("creating thing data...")
    # Write the data to a CSV file
    with open('data/Thing.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description", "properties", "location_id"])
        writer.writerows(data)



