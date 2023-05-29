"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
class STA2REST:
    @staticmethod
    def convert_entity(entity: str) -> str:
        """
        Convert an entity name to its corresponding SensorThings entity representation.

        Args:
            entity (str): The entity name to convert.

        Returns:
            str: The corresponding SensorThings entity representation.
        """
        entity_mapping = {
            "Things": "sensorthings.\"Thing\"",
            "Locations": "sensorthings.\"Location\"",
            "Sensors": "sensorthings.\"Sensor\"",
            "ObservedProperties": "sensorthings.\"ObservedProperty\"",
            "Datastreams": "sensorthings.\"Datastream\"",
            "Observations": "sensorthings.\"Observation\"",
            "FeaturesOfInterest": "sensorthings.\"FeatureOfInterest\"",
            "HistoricalLocations": "sensorthings.\"HistoricalLocation\"",
        }
        return entity_mapping.get(entity, entity)

    @staticmethod
    def convert_property(property_name: str) -> str:
        """
        Convert a property name to its corresponding SensorThings property representation.

        Args:
            property_name (str): The property name to convert.

        Returns:
            str: The corresponding SensorThings property representation.
        """
        property_mapping = {
            "name": "\"name\"",
            "description": "\"description\"",
            "encodingType": "\"encodingType\"",
            "metadata": "\"metadata\"",
            "definition": "\"definition\"",
        }
        return property_mapping.get(property_name, property_name)

    @staticmethod
    def convert_query_param(query_param: str) -> str:
        """
        Convert a query parameter to its corresponding SensorThings query parameter representation.

        Args:
            query_param (str): The query parameter to convert.

        Returns:
            str: The corresponding SensorThings query parameter representation.
        """
        query_param_mapping = {
            "filter": "filter",
            "orderby": "order",
            "top": "limit",
            "skip": "offset",
            "select": "select",
            "count": "count",
        }
        return query_param_mapping.get(query_param, query_param)

    @staticmethod
    def convert_query(sta_query: str) -> str:
        """
        Convert a SensorThings query to its corresponding representation.

        Args:
            sta_query (str): The SensorThings query to convert.

        Returns:
            str: The corresponding converted query.
        """
        sta_query = sta_query.replace("$", "").split("&")
        converted_query_params = []
        for query_param in sta_query:
            key, value = query_param.split("=")
            converted_key = STA2REST.convert_query_param(key)
            # TODO: Check for strange query parameters
            converted_query_params.append(f"{converted_key}={value}")
        return "&".join(converted_query_params)
