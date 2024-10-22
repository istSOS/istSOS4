import datetime as dt
import logging
from pprint import pprint
from typing import (
    List,
    Dict,
    Optional,
)
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

        response = requests.get(f"{self.base}")

        if response.status_code != 200:
            print("Error:", response.status_code, response.text)

    def query_api(
        self,
        entity: str,
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

        # Loop over pages. get() returns None, which is "false", if nextLink doesn't exist.
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
        import folium

        map_center = center if center else [50.244, 8.1]
        mymap = folium.Map(location=map_center, zoom_start=7)

        if things:
            for thing in things:
                if thing.get("Locations"):
                    for location in thing["Locations"]:
                        coords = location["location"]["coordinates"]
                        lat, lon = (
                            coords[1],
                            coords[0],
                        )
                        folium.Marker(
                            location=[lat, lon],
                            tooltip=thing["name"],
                        ).add_to(mymap)

        return mymap

    def map_datastreams(self, datastreams: List[Dict], center=None) -> pd.DataFrame:
        """
        Map the datastreams to a pandas dataframe
        Args:
            datastreams (List[Dict]): The datastreams to map
        Returns:
            pd.DataFrame: The mapped datastreams
        """
        import folium

        map_center = center if center else [50.244, 8.1]
        mymap = folium.Map(location=map_center, zoom_start=7)

        if datastreams:
            for datastream in datastreams:
                observed_area = datastream.get("observedArea", None)
                if observed_area:
                    coordinates = observed_area["coordinates"][0]
                    switched_coordinates = [[lat, lon] for lon, lat in coordinates]
                    folium.Polygon(
                        locations=switched_coordinates,
                        tooltip=datastream["name"],
                        color="crimson",
                        fill=True,
                        fillColor="crimson",
                        fill_opacity=0.1
                    ).add_to(mymap)
        return mymap