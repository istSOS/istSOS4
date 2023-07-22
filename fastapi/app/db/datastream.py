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
        description = "A datastream that provides the measurements from a" +name+ " sensor"
        unitOfMeasurement = f'{{"name": "degree Celsius","symbol": "degC","definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"}}'
        observationType = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
        observedArea="0103000020E61000000100000005000000BA490C022B7F52C0355EBA490C624440BA490C022B7F52C0FCA9F1D24D624440F4FDD478E97E52C0FCA9F1D24D624440F4FDD478E97E52C0355EBA490C624440BA490C022B7F52C0355EBA490C624440"
        phenomenonTime ="[""2023-03-25 14:00:00+00"",""2023-03-25 15:00:00+00"")"  #need to make this dynamic
        resultTime ="[""2023-03-25 14:00:00+00"",""2023-03-25 15:00:00+00"")"
        thing_id =i
        #sensor_id =random.randint(1, 200)
        sensor_id =i
        observedproperty_id =i

  




        # Append the row to the data list
        data.append([str(id), name, description,unitOfMeasurement,observationType,observedArea,phenomenonTime,resultTime,thing_id,sensor_id,observedproperty_id])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Datastream.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id","name","description","unitOfMeasurement","observationType","observedArea","phenomenonTime","resultTime","thing_id","sensor_id","observedproperty_id"])
    writer.writerows(dummy_data)
