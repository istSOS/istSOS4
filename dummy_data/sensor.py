import csv
import random
import string

def generate_sensor_data(id_num,sens):
    data = []

    for i in range(0, sens):

        # Generate a unique ID
        id = id_num+i


        # Generate a unique name
        name = random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor']) + "_" + str(id)
        description ="Sensor is a "+name
        encodingType="application/pdf"
        url="https://example.com/"+name +"-specs.pdf"
        metadata=f'{{"specification": "https://example.com/specs.pdf"}}'
        properties=metadata=f'{{}}'


    

        # Append the row to the data list
        data.append([str(id), name,description, encodingType,metadata,properties])
        


# Generate 200 combinations of data

    #print(data)
    print("creating sensor data...")
    # Write the data to a CSV file
    with open('data/Sensor.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "description","encodingType", "metadata","properties"])
        writer.writerows(data)
