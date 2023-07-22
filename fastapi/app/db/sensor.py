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
        encodingType="application/pdf"
        url="https://example.com/"+name +"-specs.pdf"
        metadata=f'{{"specification": "https://example.com/specs.pdf"}}'


    

        # Append the row to the data list
        data.append([str(id), name, encodingType,metadata])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Sensor.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "name", "encodingType", "metadata"])
    writer.writerows(dummy_data)
