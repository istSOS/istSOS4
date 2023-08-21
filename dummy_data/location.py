import csv
import random
import string

def generate_location_data(id_num,location_num):
    data = []

    for i in range(0, location_num):
        # Generate a unique ID
        id = id_num+i

        # Generate a unique name
        name = "Room No " + str(id)

        # Generate a random description
        description = f"A sensor that measures the {name} in a room"

        # Generate random properties
        encodingType = "application/vnd.geo+json"

        # Generate a random location ID
        location = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        
        properties=metadata=f'{{}}'

        # Append the row to the data list
        data.append([id, name, description, encodingType, location,properties])

        
        



# Generate 200 combinations of data

    #print(data)
    print("creating location data...")
    # Write the data to a CSV file
    with open('data/Location.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description", "encodingType", "location","properties"])
        writer.writerows(data)
