# Dummy data

The dataset consists of observed properties for a specified number of entities ("things") over a defined time interval with a given frequency. Additionally, it includes information about locations, historical locations, features of interest, sensors, datastreams, and observations associated with the things.

## Parameters

- `n_things` (int): Specifies the number of things being observed in the dataset.
- `n_observed_properties` (int): Denotes the number of different properties or features being recorded for each item.
- `interval` (str): Defines the time interval over which the data is generated. The format "P1Y" follows the ISO 8601 duration format, indicating a period of 1 year.
- `frequency` (str): Sets the frequency at which data points are recorded. "PT30M" is in the ISO 8601 duration format, representing a period of 30 minutes.

## Other entities

- `Locations`, `HistoricalLocations`, and `FeaturesOfInterest`: The number of these entities is equal to the number of things.
- `Sensors` and `Datastreams`: The number of these entities is equal to the number of things multiplied by the number of observed properties.
- `Observations`: The number of observations for each datastream depends on the frequency and interval. For this specific case, there is one observation every 30 minutes for one year.

## Data Generation

- `dummy data` (bool): Indicates whether the dataset should be populated with dummy data.
- `clear data` (bool): Indicates whether the dataset should be cleared.
