import csv
import random
import string

def generate_dummy_data():
    data = []

    for i in range(1, 201):

        # Generate a unique ID
        id = i


        # Generate a unique name
        name = "Room No"+ "_" + str(i)
        encodingType="application/vnd.geo+json"
        feature="0101000020E6100000BA490C022B7F52C0355EBA490C624440"




        # Append the row to the data list
        data.append([str(id), name, encodingType,feature])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('FeatureOfInterest.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "name", "encodingType", "feature"])
    writer.writerows(dummy_data)
