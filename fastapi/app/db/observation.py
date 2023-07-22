import csv
import random
import random
from datetime import datetime, timedelta


def generate_dummy_data():
    data = []

    for i in range(1, 201):

        # Generate a unique ID
        id = i


        # Generate a unique name
        start_date = '2023-01-01'
        end_date = '2023-12-31'
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

        # Calculate the time range in seconds
        time_range = (end_datetime - start_datetime).total_seconds()

        # Generate a random number of seconds within the time range
        random_seconds = random.randint(0, int(time_range))

        # Add the random number of seconds to the start datetime
        time = start_datetime + timedelta(seconds=random_seconds)    

        phenomenonTime = time
        resultTime =time
        result=25
        resultQuality=""
        validTime=""
        parameters=""
        #datastream_id =random.randint(1, 200)
        datastream_id =i
        feature_of_interest_id =i


  




        # Append the row to the data list
        data.append([str(id), phenomenonTime,resultTime,result,resultQuality,validTime,parameters,datastream_id,feature_of_interest_id])
        
    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('Observation.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id","phenomenonTime","resultTime","result","resultQuality","validTime","parameters","datastream_id","feature_of_interest_id"])
    writer.writerows(dummy_data)
