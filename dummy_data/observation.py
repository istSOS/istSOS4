import csv
import random
import random
from datetime import datetime
from isodate import parse_datetime, parse_duration

def generate_observation_data(id_num,obs,static_data_num,data_stream_num,feature_num,start_datetime,timestep):

    data = []
    last_id=0

        # Specify the start datetime and timestep duration
    # start_datetime_str = '2020-01-01T12:00:00.000+01:00'
    # timestep_str = 'PT10M'
    start_datetime_str = start_datetime
    timestep_str = timestep

    # Parse the start datetime and timestep duration using iso8601
    start_datetime = start_datetime_str
    # print(start_datetime)
    timestep_duration = parse_duration(timestep_str)
    # print(timestep_duration)
    # Generate timestamps using the specified timestep

    current_time = start_datetime

    # Generate timestamps until a certain point (e.g., 10 timestamps)
    # for _ in range(10):
    #     timestamps.append(current_time.isoformat())
    #     current_time += timestep_duration


    for j in range(0,data_stream_num):
        for i in range(0, obs):

            # Generate a unique ID
            id = id_num+i+last_id


            timestamps=current_time.isoformat()
            current_time += timestep_duration
          

            phenomenonTime = timestamps
            resultTime =timestamps
            
            
            
            resultType=random.randint(0, 4)
            if resultType==0:
                resultString="testvalue"
                resultInteger="None"
                resultDouble="None"
                resultBoolean="None"
                resultJSON="None"
            elif resultType==1:
                resultInteger=random.randint(1,100)
                resultString="None"
                resultDouble="None"
                resultBoolean="None"
                resultJSON="None"
            elif resultType==2:
                resultDouble=random.random()
                resultString="None"
                resultInteger="None"
                resultBoolean="None"
                resultJSON="None"
                            
            elif resultType==3:
                resultString="None"
                resultInteger="None"
                resultDouble="None"
                resultJSON="None"        
                resultBoolean=bool(random.choice([True, False]))
            elif resultType==4:
                resultJSON=metadata=f'{{"testvalue": "value-{i}"}}'
                resultString="None"
                resultInteger="None"
                resultDouble="None"
                resultBoolean="None"
    
            parameters=metadata=f'{{}}'        
            
            resultQuality="None"
            validTime="None"

            
            datastream_id = static_data_num+j
            # print(datastream_id)
            # datastream_id =i+1
            feature_of_interest_id =random.randint(1, feature_num)


    




            # Append the row to the data list
            data.append([str(id), phenomenonTime,resultTime,resultType,resultString,resultInteger,resultDouble,resultBoolean,resultJSON,resultQuality,validTime,parameters,datastream_id,feature_of_interest_id])
        last_id = id

# Generate 200 combinations of data
    #print(data)
    print("creating observation data...")
    # Write the data to a CSV file
    with open('data/Observation.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id","phenomenonTime","resultTime","resultType","resultString","resultInteger","resultDouble","resultBoolean","resultJSON","resultQuality","validTime","parameters","datastream_id","feature_of_interest_id"])
        writer.writerows(data)
    last_id=0
    print("after clearing:")
    print(id)

# generate_observation_data(1,6,5,3)