# Dummy data

The dataset consists of observed properties for a certain number of things over a specified time interval with a given frequency. Additionally, it includes information about locations, historical locations, features of interest, sensors, datastreams and observations associated with the things.

## Parameters

- `n_things`: This parameter specifies the number of things being observed in the dataset.
- `n_observed_properties`: This parameter denotes the number of different properties or features being recorded for each item.
- `interval`: This parameter defines the time interval over which the data is generated. The format "P1Y" follows the ISO 8601 duration format, indicating a period of 1 year.
- `frequency`: This parameter sets the frequency at which data points are recorded. "PT30M" is also in the ISO 8601 duration format, representing a period of 30 minutes.

## Other entities

- `Locations`, `HistoricalLocations`, and `FeaturesOfInterest`: the number of these entities is equal to the number of things.
- `Sensors` and `Datastreams`: the number these entities is equal to the number of things multiplied by the number of observed properties.
- `Observations`: the number of this entity for each datastream depends on the frequency and interval. For our specific case, we have one observation every 30 minutes for one year.

## Data Generation

- `dummy data`: Indicates that the generated data is not dummy or placeholder data but represents a realistic simulation based on the given parameters.
- `clear data`: Specifies that the dataset should not be cleared or reset after generation, implying that the data is persistent and can be reused.
