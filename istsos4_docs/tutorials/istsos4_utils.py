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

        # Store results
        response_json = response.json()
        data = response_json["value"]

        # Loop over pages. get() returns None, which is "falsey", if nextLink doesn't exist.
        while next_link := response_json.get("@iot.nextLink"):
            response = requests.get(next_link)
            logging.info(f"query_api request: {response.url}")
            response.raise_for_status()  # throw error on bad query

            response_json = response.json()
            data.extend(response_json["value"])

        return data
