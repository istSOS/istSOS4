# Dummy data

This container generates a random dataset to mimic a real-world monitoring network. It allows you to create a specified number of entities ("things") for each observed property, with observations over a defined time interval at a given frequency. Additionally, it includes information about locations, historical locations, features of interest, sensors, datastreams, and observations associated with the things.

## Input Parameters

- `N_THINGS (int)`: Number of things being observed in the dataset.
- `N_OBSERVED_PROPERTIES` (int): Number of different properties or features being recorded for each thing.
- `INTERVAL` (str): Time interval over which the data is generated, following the ISO 8601 duration format (e.g., "P1Y" for a period of 1 year).
- `FREQUENCY` (str): Frequency at which data points are recorded, using the ISO 8601 duration format (e.g., "PT30M" for a period of 30 minutes).
- `START_DATETIME` (str): Specifies the start date for phenomenonTime

## Entities Counts

- `Locations`, `HistoricalLocations`, and `FeaturesOfInterest`: One per thing.
- `Sensors` and `Datastreams`: : One per thing per observed property.
- `Observations`: Number of observations for each datastream depends on the frequency and interval (e.g., one observation every 30 minutes for one year).

## Data Generation Options

- `DUMMY_DATA` (bool): Specifies whether the dataset should be populated with dummy data.
- `CLEAR_DATA` (bool): Specifies whether the dataset should be cleared before generating new data.

By adjusting these parameters, you can create a customized dataset that suits your needs for testing and development purposes.
