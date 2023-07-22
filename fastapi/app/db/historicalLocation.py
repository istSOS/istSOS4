import csv
import random
import random
from datetime import datetime, timedelta

def generate_dummy_data():
    data = []

    for i in range(1, 201):
        # Generate a unique ID
        id = i

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

     
        # thing_id=random.randint(1, 200)
        # location_id=random.randint(1, 200)
        thing_id=i
        location_id=i

        # Append the row to the data list
        data.append([id, time, thing_id, location_id])

    return data

# Generate 200 combinations of data
dummy_data = generate_dummy_data()
print(dummy_data)
# Write the data to a CSV file
with open('HistoricalLocation.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["id", "time", "thing_id", "location_id"])
    writer.writerows(dummy_data)
