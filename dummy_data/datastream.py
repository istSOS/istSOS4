import csv
import random
import string

def generate_datastream_data(id_num,data_stream,thing_num,sens_num,obs_prop_num):
    data = []

    for i in range(0, data_stream):

        # Generate a unique ID
        id = id_num+i


        # Generate a unique name
        name = random.choice(['Temperature Sensor', 'Humidity Sensor', 'Pressure Sensor', 'Light Sensor', 'CO2 Sensor', 'Motion Sensor']) + "_" + str(id)
        description = "A datastream that provides the measurements from a" +name+ " sensor"
        unitOfMeasurement = f'{{"name": "degree Celsius","symbol": "degC","definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"}}'
        observationType = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
        observedArea="0103000020E61000000100000005000000BA490C022B7F52C0355EBA490C624440BA490C022B7F52C0FCA9F1D24D624440F4FDD478E97E52C0FCA9F1D24D624440F4FDD478E97E52C0355EBA490C624440BA490C022B7F52C0355EBA490C624440"
        phenomenonTime ="[""2023-03-25 14:00:00+00"",""2023-03-25 15:00:00+00"")"  #need to make this dynamic
        resultTime ="[""2023-03-25 14:00:00+00"",""2023-03-25 15:00:00+00"")"
        properties=metadata=f'{{}}'

        thing_id =random.randint(1, thing_num)
        #sensor_id =random.randint(1, 200)
        sensor_id =random.randint(1, sens_num)
        observedproperty_id =random.randint(1, obs_prop_num)

  




        # Append the row to the data list
        data.append([str(id), name, description,unitOfMeasurement,observationType,observedArea,phenomenonTime,resultTime,properties,thing_id,sensor_id,observedproperty_id])
        


# Generate 200 combinations of data

    #print(data)
    print("creating datastream data...")
    # Write the data to a CSV file
    with open('data/Datastream.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id","name","description","unitOfMeasurement","observationType","observedArea","phenomenonTime","resultTime","properties","thing_id","sensor_id","observedproperty_id",])
        writer.writerows(data)
