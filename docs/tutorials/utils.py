import csv
import datetime as dt
import json
import logging
import re
from pprint import pprint
from typing import Dict, List, Optional
from urllib.parse import quote, unquote
from warnings import warn

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from saqc import SaQC


class sta:
    def __init__(self, base_url: str, verbose=False):
        """_init the sta class_

        Args:
            base_url (str): _The base url to the Sensor Things API service_
        """
        if base_url[-1] != "/":
            base_url += "/"
        self.base = base_url
        self.headers = {"Content-Type": "application/json"}
        self.verbose = verbose
        self.user = None
        self.token_obj = None

    def create_user(
        self, username: str, password: str, role: str, uri=None, contact=None
    ):
        """
        Create a new user
        Args:
            username (str): The username of the new user
            password (str): The password of the new user
            role (str): The role of the new user
        Returns:
            requests.models.Response: The response from the API
        """
        data = {
            "username": username,
            "password": password,
            "uri": uri,
            "contact": contact,
            "role": role,
        }
        self.user = data

        response = requests.post(f"{self.base}/users", json=data)
        if "already exists" in response.text:
            response.status_code = 409
            logging.warning("User already exists")
            return True
        logging.info("User created successfully")
        response.raise_for_status()

        return True

    def create_thing(self, body, commit_message=None):
        """
        Create a new thing
        Args:
            body (Dict): The body of the new thing
        Returns:
            requests.models.Response: The response from the API
        """
        tmp_headers = self.headers.copy()
        if commit_message:
            tmp_headers["commit-message"] = commit_message
        response = requests.post(
            f"{self.base}Things", json=body, headers=tmp_headers
        )
        if "unique" in response.text:
            response.status_code = 409
            logging.warning("Thing already exists")
            return response
        response.raise_for_status()
        return response

    def update_thing(self, thing_id, body, commit_message=None):
        """
        Update a thing
        Args:
            thing_id (int): The ID of the thing to update
            body (Dict): The body of the thing
        Returns:
            requests.models.Response: The response from the API
        """
        tmp_headers = self.headers.copy()
        if commit_message:
            tmp_headers["commit-message"] = commit_message
        response = requests.patch(
            f"{self.base}Things({thing_id})", json=body, headers=tmp_headers
        )
        response.raise_for_status()

        return response

    def update_observation(self, observation_id, body, commit_message=None):
        """
        Update an observation
        Args:
            observation_id (int): The ID of the observation to update
            body (Dict): The body of the observation
        Returns:
            requests.models.Response: The response from the API
        """
        tmp_headers = self.headers.copy()
        if commit_message:
            tmp_headers["commit-message"] = commit_message
        response = requests.patch(
            f"{self.base}Observations({observation_id})",
            json=body,
            headers=tmp_headers,
        )
        if response.status_code != 200:
            logging.error(response.text)
        response.raise_for_status()

        return response

    def get_token(self, username: str, password: str):
        """
        Get a token for a user
        Args:
            username (str): The username of the user
            password (str): The password of the user
        Returns:
            str: The token for the user
        """
        data = {
            "username": username,
            "password": password,
        }

        response = requests.post(
            f"{self.base}" + "login",
            data={
                "username": username,
                "password": password,
                "grant_type": "password",
            },
        )
        response.raise_for_status()
        res = response.json()
        self.token_obj = res

        self.headers["Authorization"] = f"Bearer {res['access_token']}"

        return res

    def check_api(self):

        response = requests.get(f"{self.base[:-1]}")

        if response.status_code != 200:
            logging.info("Error:", response.status_code, response.text)

    def csv2sta(
        self,
        csv_file: str,
        datastream_id: int,
        step=10000,
        head=True,
        commit_message="Insert observations",
        max_rows=None,  # max number of rows to insert
    ):
        """
        Insert observations from a CSV file to a datastream
        Args:
            csv_file (str): The path to the CSV file
            datastream_id (int): The ID of the datastream
            step (int): The number of rows to insert in a single request
            head (bool): Whether to skip the first row of the CSV file
            commit_message (str): The commit message for the request
            max_rows (Optional[int]): The maximum number of rows to insert
        """
        max_rows = max_rows
        tmp_rows = 0
        tmp_headers = self.headers.copy()
        tmp_headers["commit-message"] = commit_message
        with open(csv_file, "r") as f:
            data = csv.reader(f, delimiter=",")
            i = 0
            post_data = [
                {
                    "Datastream": {"@iot.id": datastream_id},
                    "components": [
                        "result",
                        "phenomenonTime",
                        "resultTime",
                        "resultQuality",
                    ],
                    "dataArray": [],
                }
            ]
            for r in data:
                if head and i == 0:
                    i += 1
                    continue
                else:
                    ob = [
                        float(r[2]),
                        r[0],
                        r[0],
                        r[3],
                    ]
                    post_data[0]["dataArray"].append(ob)
                i += 1
                tmp_rows += 1
                if i == step:
                    req = requests.post(
                        f"{self.base}BulkObservations",
                        data=json.dumps(post_data),
                        headers=tmp_headers,
                    )
                    if req.status_code == 201:
                        logging.info(f"Observation created successfully ({i})")
                    else:
                        logging.warning(req.text)
                        break
                    i = 0
                    post_data = [
                        {
                            "Datastream": {"@iot.id": datastream_id},
                            "components": [
                                "result",
                                "phenomenonTime",
                                "resultTime",
                                "resultQuality",
                            ],
                            "dataArray": [],
                        }
                    ]
                if max_rows:
                    if tmp_rows >= max_rows:
                        if i > 0:
                            req = requests.post(
                                f"{self.base}BulkObservations",
                                data=json.dumps(post_data),
                                headers=tmp_headers,
                            )
                            if req.status_code == 201:
                                logging.info(
                                    f"Observation created successfully ({i})"
                                )
                            else:
                                logging.warning(req.text)
                                break
                        i = 0
                        tmp_rows = 0
                        break
            if i > 0:
                req = requests.post(
                    f"{self.base}BulkObservations",
                    data=json.dumps(post_data),
                    headers=tmp_headers,
                )
                if req.status_code == 201:
                    logging.info(f"Observation created successfully ({i})")
                else:
                    logging.warning(req.text)
                tmp_rows = 0

    def query_api(
        self, entity: str, params: Optional[Dict] = None, travel_time=False
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
        response = requests.get(
            self.base + entity, params=params, headers=self.headers
        )
        decoded_url = unquote(response.url)
        logging.info(f"URL: {decoded_url}")
        if response.status_code != 200:
            logging.error(response.text)
        response.raise_for_status()  # throw error on bad query

        # Regular expression to match any (*) where * is an integer
        pattern = r"\(\d+\)"

        # Check if the pattern exists in the text
        match = re.search(pattern, entity)

        # Store results
        response_json = response.json()

        if bool(match):
            data = response_json
        elif travel_time:
            data = response_json
        else:
            data = response_json["value"]

        # Loop over pages. get() returns None, which is "false", if nextLink doesn't exist.
        while next_link := response_json.get("@iot.nextLink"):
            response = requests.get(next_link)
            logging.info(f"URL: {response.url}")
            response.raise_for_status()  # throw error on bad query

            response_json = response.json()
            if travel_time:
                data["value"].extend(response_json["value"])
            else:
                data.extend(response_json["value"])
        return data

    def get_dfs_by_datastreams(
        self, filter, top=15000, orderby="phenomenonTime asc"
    ):
        """
        Get dataframes by datastreams
        Args:
            filter (str): The filter to apply
            top (int): The number of results to return
            orderby (str): The order of the results
        Returns:
            List[pd.DataFrame]: The dataframes
        """
        dfs = {}
        qcs = {}
        datastreams = self.query_api("Datastreams", {"$filter": f"{filter}"})
        for datastream in datastreams:
            logging.info(f"Datastream: {datastream['name']}")
            logging.info(f"Description: {datastream['description']}")
            logging.info(
                f"Unit of measurement: {datastream['unitOfMeasurement']['name']} ({datastream['unitOfMeasurement']['symbol']})"
            )
            observations = self.query_api(
                f"Datastream({datastream['@iot.id']})/Observations?$top=15000&$orderby=phenomenonTime asc",
                # {"$top": 15000, "$orderby": "phenomenonTime asc"},$
                travel_time=True,
            )
            logging.info(
                f"Number of observations: {len(observations['value'])}"
            )
            if len(observations["value"]) == 0:
                logging.info("\n")
                logging.info("--------------------")
                continue
            logging.info(f"As of: {observations['@iot.as_of']}")
            # logging.info(f"First observation:")
            # Create a DataFrame
            df = pd.DataFrame(observations["value"])
            df.index = pd.to_datetime(df["phenomenonTime"])
            df["result"] = pd.to_numeric(df["result"])
            dfs[datastream["name"]] = df
            # Create a QC object
            qcs[datastream["name"]] = SaQC(data=df, scheme="float")
            df["ylabel"] = f"{datastream['unitOfMeasurement']['symbol']}"
            logging.info("\n")
            logging.info("--------------------")
        return dfs, qcs

    def map_things(self, things: List[Dict], center=None) -> pd.DataFrame:
        """
        Map the things to a pandas dataframe
        Args:
            things (List[Dict]): The things to map
        Returns:
            pd.DataFrame: The mapped things
        """
        import folium

        map_center = center if center else [46.505978, 8.511378]
        mymap = folium.Map(location=map_center, zoom_start=16)

        if things:
            for thing in things:
                if thing.get("Locations"):
                    print(thing["Locations"])
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

    def map_datastreams(
        self, datastreams: List[Dict], center=None
    ) -> pd.DataFrame:
        """
        Map the datastreams to a pandas dataframe
        Args:
            datastreams (List[Dict]): The datastreams to map
        Returns:
            pd.DataFrame: The mapped datastreams
        """
        import folium

        map_center = center if center else [46.172245, 8.956099]
        mymap = folium.Map(location=map_center, zoom_start=16)

        if datastreams:
            for datastream in datastreams:
                observed_area = datastream.get("observedArea", None)
                if observed_area:
                    coordinates = observed_area["coordinates"][0]
                    switched_coordinates = [
                        [lat, lon] for lon, lat in coordinates
                    ]
                    folium.Polygon(
                        locations=switched_coordinates,
                        tooltip=datastream["name"],
                        color="crimson",
                        fill=True,
                        fillColor="crimson",
                        fill_opacity=0.1,
                    ).add_to(mymap)
        return mymap
