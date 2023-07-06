import csv
import random
import string

def generate_dummy_data():
    data = []

    for i in range(1, 201):
        # Generate a unique ID
        id = str(i)

        # Generate a unique name
        name = "Room No " + str(i)

        # Generate a random description
        description = f"A sensor that measures the {name} in a room"

        # Generate random properties
        encodingType = "application/vnd.geo+json"

        # Generate a random location ID
        location = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"

        # Append the row to the data list
        data.append([id, name, description, encodingType, location])

    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Location.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "name", "description", "encodingType", "location"])
    writer.writerows(dummy_data)
