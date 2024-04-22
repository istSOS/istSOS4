import csv
import os
path = 'data'

if not os.path.exists(path):
    os.makedirs(path)
    print("Folder created successfully.")
else:
    print("No new folder created as folder already exists.")

def generate_location_data(id_num, location_num):
    data = []

    for i in range(0, location_num):
        id = id_num + i
        name = f"Room No {str(id)}"
        description = f"A sensor that measures the {name} in a room"
        encodingType = "application/vnd.geo+json"
        location = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        properties = f"{{}}"

        # Append the row to the data list
        data.append([id, name, description, encodingType, location, properties])

    print("creating Location data...")
    # Write the data to a CSV file
    with open('data/Location.csv', 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description", "encodingType", "location", "properties"])
        writer.writerows(data)