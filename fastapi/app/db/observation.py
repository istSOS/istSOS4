import csv
import random
import string

def generate_dummy_data():
    data = []

    for i in range(1, 201):

        # Generate a unique ID
        id = i


        # Generate a unique name
        
        phenomenonTime ="2023-03-25 14:00:00+00 / 2023-03-25 15:00:00+00" 
        resultTime ="2023-03-25 14:00:00+00 / 2023-03-25 15:00:00+00"
        result=25
        resultQuality=""
        validTime=""
        parameters=""
        datastream_id =random.randint(1, 200)
        feature_of_interest_id =random.randint(1, 200)


  




        # Append the row to the data list
        data.append([str(id), phenomenonTime,resultTime,result,resultQuality,validTime,parameters,datastream_id,feature_of_interest_id])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Observation.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id","name","phenomenonTime","resultTime","result","resultQuality","validTime","parameters","datastream_id","feature_of_interest_id"])
    writer.writerows(dummy_data)
