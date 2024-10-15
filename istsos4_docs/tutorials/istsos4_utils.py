# Define a function that queries the API
import datetime as dt
import logging
from pprint import pprint
from typing import (
    List,
    Dict,
    Optional,
)  # Typing is optional in Python but helps with documentation and code editors
from warnings import warn

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import re

logging.basicConfig(level=logging.INFO)


class sta:
    def __init__(self, base_url: str):
        """_init the sta class_

        Args:
            base_url (str): _The base url to the Sensor Things API service_
        """
        self.base = base_url

        # test if endpoint is reachable
        response = requests.get(f"{self.base}")

        # Print the response
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)

    def query_api(
        self,
        entity: str,
        # entity_id: Optional[int] = None,
        params: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Query the SensorThings API  and return values.
        Query STA API and return values. Queries are logged, errors raise exceptions
        and data are paged if they don't all fit in a single response.
        Args:
            entity (str): The entity to query (e.g. "Things", "Sensors", "Datastreams", "Observations")
            params (Optional[Dict]): Query parameters to pass to the API.
        Returns:
            List[Dict]: The data returned by the API.
        """
        # Query API
        response = requests.get(self.base + entity, params=params)
        logging.info(f"query_api request: {response.url}")
        response.raise_for_status()  # throw error on bad query

        # Regular expression to match any (*) where * is an integer
        pattern = r"\(\d+\)"

        # Check if the pattern exists in the text
        match = re.search(pattern, entity)

        # Store results
        response_json = response.json()

        if bool(match):
            data = response_json
        else:
            data = response_json["value"]

        # Loop over pages. get() returns None, which is "falsey", if nextLink doesn't exist.
        while next_link := response_json.get("@iot.nextLink"):
            response = requests.get(next_link)
            logging.info(f"query_api request: {response.url}")
            response.raise_for_status()  # throw error on bad query

            response_json = response.json()
            data.extend(response_json["value"])

        return data

    def map_things(self, things: List[Dict], center=None) -> pd.DataFrame:
        """
        Map the things to a pandas dataframe
        Args:
            things (List[Dict]): The things to map
        Returns:
            pd.DataFrame: The mapped things
        """
        # return pd.DataFrame(things)
        import folium

        # Step 1: Query the API for Thing data
        things = self.query_api("Things", "$expand=Locations")

        # Step 2: Create a base map using folium
        # Define the initial location of the map (centered around some coordinates)
        map_center = [
            0,
            0,
        ]  # This can be set to a reasonable default or updated based on data
        mymap = folium.Map(location=map_center, zoom_start=2)

        # Step 3: Add markers to the map for each Thing with a valid location
        if things:
            for thing in things:
                # Extract the location data for each Thing
                if thing.get("Locations"):
                    for location in thing["Locations"]:
                        coords = location["location"]["coordinates"]
                        lat, lon = (
                            coords[1],
                            coords[0],
                        )  # GeoJSON uses [longitude, latitude]

                        # Add a marker for each location
                        folium.Marker(
                            location=[lat, lon],
                            popup=f"Thing: {thing['name']}<br>Description: {thing['description']}",
                            tooltip=thing["name"],
                        ).add_to(mymap)

        # Step 4: Display the map in Jupyter
        return mymap
