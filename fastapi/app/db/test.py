import random
from datetime import datetime, timedelta

def generate_random_timestamp(start_date, end_date):
    # Convert the start and end dates to datetime objects
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    # Calculate the time range in seconds
    time_range = (end_datetime - start_datetime).total_seconds()

    # Generate a random number of seconds within the time range
    random_seconds = random.randint(0, int(time_range))

    # Add the random number of seconds to the start datetime
    random_datetime = start_datetime + timedelta(seconds=random_seconds)

    return random_datetime

# Example usage
start_date = '2023-01-01'
end_date = '2023-12-31'

random_timestamp = generate_random_timestamp(start_date, end_date)
print(random_timestamp)
