import csv
import random
import random
from datetime import datetime, timedelta


def generate_observation_data(id_num,obs,data_stream_num,feature_num):
    data = []

    for i in range(0, obs):

        # Generate a unique ID
        id = id_num+i


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
        
        
        
        resultType=random.randint(1, 5)
        if resultType==1:
            resultString="testvalue"
            resultInteger="None"
            resultDouble="None"
            resultBoolean="None"
            resultJSON="None"
        elif resultType==2:
            resultInteger=random.randint(1,100)
            resultString="None"
            resultDouble="None"
            resultBoolean="None"
            resultJSON="None"
        elif resultType==3:
            resultDouble=random.random()
            resultString="None"
            resultInteger="None"
            resultBoolean="None"
            resultJSON="None"
                        
        elif resultType==4:
            resultString="None"
            resultInteger="None"
            resultDouble="None"
            resultJSON="None"        
            resultBoolean=bool(random.choice([True, False]))
        elif resultType==5:
            resultJSON=metadata=f'{{"testvalue": "value-{i}"}}'
            resultString="None"
            resultInteger="None"
            resultDouble="None"
            resultBoolean="None"
   
        parameters=metadata=f'{{}}'        
        
        resultQuality="None"
        validTime="None"

        
        datastream_id =random.randint(1, data_stream_num)
        # datastream_id =i+1
        feature_of_interest_id =random.randint(1, feature_num)


  




        # Append the row to the data list
        data.append([str(id), phenomenonTime,resultTime,resultType,resultString,resultInteger,resultDouble,resultBoolean,resultJSON,resultQuality,validTime,parameters,datastream_id,feature_of_interest_id])
        

# Generate 200 combinations of data
    #print(data)
    print("creating observation data...")
    # Write the data to a CSV file
    with open('data/Observation.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id","phenomenonTime","resultTime","resultType","resultString","resultInteger","resultDouble","resultBoolean","resultJSON","resultQuality","validTime","parameters","datastream_id","feature_of_interest_id"])
        writer.writerows(data)
