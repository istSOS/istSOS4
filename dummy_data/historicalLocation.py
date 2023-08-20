import csv
import random
import random
from datetime import datetime, timedelta

def generate_historicalLocation_data(id_num,hist_num,thing_num,loc_num):
    data = []

    for i in range(0, hist_num):
        # Generate a unique ID
        id = id_num+i

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

     
        thing_id=random.randint(1, thing_num)
        location_id=random.randint(1, loc_num)


        # Append the row to the data list
        data.append([id, time, thing_id, location_id])



    # Generate 200 combinations of data
   
    #print(data)
    print("creating historical location data...")
    # Write the data to a CSV file
    with open('data/HistoricalLocation.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "time", "thing_id", "location_id"])
        writer.writerows(data)
