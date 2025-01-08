"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""

import re
from datetime import datetime, timezone

from app import DEBUG, VERSION, VERSIONING
from dateutil.parser import isoparse

from .sta_parser.ast import *
from .sta_parser.lexer import Lexer
from .sta_parser.parser import Parser
from .visitors import NodeVisitor

try:
    DEBUG = DEBUG
except:
    DEBUG = 0


class STA2REST:
    """
    This class provides utility functions to convert various elements used in SensorThings queries to their corresponding
    representations in a REST API.
    """

    # Mapping from SensorThings entities to their corresponding database table names
    ENTITY_MAPPING = {
        "Commits": "Commit",
        "Things": "Thing",
        "Locations": "Location",
        "Sensors": "Sensor",
        "ObservedProperties": "ObservedProperty",
        "Datastreams": "Datastream",
        "Observations": "Observation",
        "FeaturesOfInterest": "FeaturesOfInterest",
        "HistoricalLocations": "HistoricalLocation",
        "Commit": "Commit",
        "Thing": "Thing",
        "Location": "Location",
        "Sensor": "Sensor",
        "ObservedProperty": "ObservedProperty",
        "Datastream": "Datastream",
        "Observation": "Observation",
        "FeatureOfInterest": "FeaturesOfInterest",
        "HistoricalLocation": "HistoricalLocation",
    }

    # Default columns for each entity
    DEFAULT_SELECT = {
        "Commit": [
            "id",
            "self_link",
            "location_navigation_link",
            "thing_navigation_link",
            "historicallocation_navigation_link",
            "observedproperty_navigation_link",
            "sensor_navigation_link",
            "datastream_navigation_link",
            "featuresofinterest_navigation_link",
            "observation_navigation_link",
            "author",
            "encoding_type",
            "message",
            "date",
            "action_type",
        ],
        "Location": [
            "id",
            "self_link",
            "thing_navigation_link",
            "historicallocation_navigation_link",
            "name",
            "description",
            "encoding_type",
            "location",
            "properties",
        ],
        "LocationTravelTime": [
            "id",
            "self_link",
            "thing_navigation_link",
            "historicallocation_navigation_link",
            "name",
            "description",
            "encoding_type",
            "location",
            "properties",
            "system_time_validity",
        ],
        "Thing": [
            "id",
            "self_link",
            "location_navigation_link",
            "historicallocation_navigation_link",
            "datastream_navigation_link",
            "name",
            "description",
            "properties",
        ],
        "ThingTravelTime": [
            "id",
            "self_link",
            "location_navigation_link",
            "historicallocation_navigation_link",
            "datastream_navigation_link",
            "name",
            "description",
            "properties",
            "system_time_validity",
        ],
        "HistoricalLocation": [
            "id",
            "self_link",
            "location_navigation_link",
            "thing_navigation_link",
            "time",
        ],
        "HistoricalLocationTravelTime": [
            "id",
            "self_link",
            "location_navigation_link",
            "thing_navigation_link",
            "time",
            "system_time_validity",
        ],
        "ObservedProperty": [
            "id",
            "self_link",
            "datastream_navigation_link",
            "name",
            "description",
            "definition",
            "properties",
        ],
        "ObservedPropertyTravelTime": [
            "id",
            "self_link",
            "datastream_navigation_link",
            "name",
            "description",
            "definition",
            "properties",
            "system_time_validity",
        ],
        "Sensor": [
            "id",
            "self_link",
            "datastream_navigation_link",
            "name",
            "description",
            "encoding_type",
            "sensor_metadata",
            "properties",
        ],
        "SensorTravelTime": [
            "id",
            "self_link",
            "datastream_navigation_link",
            "name",
            "description",
            "encoding_type",
            "sensor_metadata",
            "properties",
            "system_time_validity",
        ],
        "Datastream": [
            "id",
            "self_link",
            "thing_navigation_link",
            "sensor_navigation_link",
            "observedproperty_navigation_link",
            "observation_navigation_link",
            "name",
            "description",
            "unit_of_measurement",
            "observation_type",
            "observed_area",
            "phenomenon_time",
            "result_time",
            "properties",
        ],
        "DatastreamTravelTime": [
            "id",
            "self_link",
            "thing_navigation_link",
            "sensor_navigation_link",
            "observedproperty_navigation_link",
            "observation_navigation_link",
            "name",
            "description",
            "unit_of_measurement",
            "observation_type",
            "observed_area",
            "phenomenon_time",
            "result_time",
            "properties",
            "system_time_validity",
        ],
        "FeaturesOfInterest": [
            "id",
            "self_link",
            "observation_navigation_link",
            "name",
            "description",
            "encoding_type",
            "feature",
            "properties",
        ],
        "FeaturesOfInterestTravelTime": [
            "id",
            "self_link",
            "observation_navigation_link",
            "name",
            "description",
            "encoding_type",
            "feature",
            "properties",
            "system_time_validity",
        ],
        "Observation": [
            "id",
            "self_link",
            "featuresofinterest_navigation_link",
            "datastream_navigation_link",
            "phenomenon_time",
            "result_time",
            "result",
            "result_quality",
            "valid_time",
            "parameters",
        ],
        "ObservationDataArray": [
            "id",
            "phenomenon_time",
            "result_time",
            "result",
            "result_quality",
            "valid_time",
            "parameters",
        ],
        "ObservationTravelTime": [
            "id",
            "self_link",
            "featuresofinterest_navigation_link",
            "datastream_navigation_link",
            "phenomenon_time",
            "result_time",
            "result",
            "result_quality",
            "valid_time",
            "parameters",
            "system_time_validity",
        ],
    }

    SELECT_MAPPING = {
        "encodingType": "encoding_type",
        "metadata": "sensor_metadata",
        "unitOfMeasurement": "unit_of_measurement",
        "observationType": "observation_type",
        "observedArea": "observed_area",
        "phenomenonTime": "phenomenon_time",
        "resultTime": "result_time",
        "resultQuality": "result_quality",
        "validTime": "valid_time",
        "systemTimeValidity": "system_time_validity",
        "actionType": "action_type",
        "selfLink": "self_link",
    }

    REVERSE_SELECT_MAPPING = {v: k for k, v in SELECT_MAPPING.items()}

    if VERSIONING:
        for key in DEFAULT_SELECT:
            if key != "Commit":
                DEFAULT_SELECT[key].append("commit_navigation_link")

    @staticmethod
    def get_default_column_names(entity: str) -> list:
        """
        Get the default column names for a given entity.

        Args:
            entity (str): The entity name.

        Returns:
            list: The default column names.
        """
        select = STA2REST.DEFAULT_SELECT.get(entity, ["*"])
        for old_key, new_key in STA2REST.SELECT_MAPPING.items():
            if old_key in select:
                select.remove(old_key)
                select.append(new_key)

        return select

    @staticmethod
    def convert_entity(entity: str) -> str:
        """
        Converts an entity name from STA format to REST format.

        Args:
            entity (str): The entity name in STA format.

        Returns:
            str: The converted entity name in REST format.
        """
        return STA2REST.ENTITY_MAPPING.get(entity, entity)

    @staticmethod
    def convert_to_database_id(entity: str) -> str:
        # First we convert the entity to lower case
        entity = STA2REST.convert_entity(entity).lower()
        return entity + "_id"

    @staticmethod
    def convert_query(full_path: str) -> str:
        """
        Converts a STA query to a PostgREST query.

        Args:
            sta_query (str): The STA query.

        Returns:
            str: The converted PostgREST query.
        """

        # check if we have a query
        path = full_path
        query = None
        single_result = False
        if "?" in full_path:
            # Split the query from the path
            path, query = full_path.split("?")

        # Parse the uri
        uri = STA2REST.parse_uri(path)

        if not uri:
            raise Exception("Error parsing uri")

        # Check if we have a query
        query_ast = QueryNode(
            None, None, None, None, None, None, None, None, None, None, False
        )
        if query:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            query_ast = parser.parse()

        main_entity, main_entity_id = uri["entity"]
        entities = uri["entities"]

        if query_ast.as_of:
            if query_ast.from_to:
                raise Exception("AS_OF and FROM_TO cannot be used together")

            local_timezone = datetime.now().astimezone().tzinfo

            as_of_value = isoparse(query_ast.as_of.value)

            if as_of_value.tzinfo is None:
                local_datetime = as_of_value.replace(tzinfo=local_timezone)
                utc_datetime = local_datetime.astimezone(timezone.utc)
                query_ast.as_of.value = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")
            else:
                utc_datetime = as_of_value.astimezone(timezone.utc)
                query_ast.as_of.value = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")

            if isoparse(query_ast.as_of.value) > datetime.now(timezone.utc):
                raise Exception("AS_OF value cannot be in the future")

            main_entity += "TravelTime"
            entities = [
                (entity + "TravelTime", value) for entity, value in entities
            ]
            as_of_filter = f"system_time_validity eq {query_ast.as_of.value}"
            query_ast.filter = FilterNode(
                query_ast.filter.filter + f" and {as_of_filter}"
                if query_ast.filter
                else as_of_filter
            )
            if query_ast.expand:
                for identifier in query_ast.expand.identifiers:
                    identifier.identifier = STA2REST.ENTITY_MAPPING.get(
                        identifier.identifier, identifier.identifier
                    )
                    identifier.subquery = (
                        QueryNode(
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            True,
                        )
                        if identifier.subquery is None
                        else identifier.subquery
                    )
                    if identifier.identifier != "Commit":
                        identifier.identifier += "TravelTime"
                        identifier.subquery.filter = FilterNode(
                            identifier.subquery.filter.filter
                            + f" and {as_of_filter}"
                            if identifier.subquery.filter
                            else as_of_filter
                        )

        if query_ast.from_to:
            if query_ast.as_of:
                raise Exception("AS_OF and FROM_TO cannot be used together")

            local_timezone = datetime.now().astimezone().tzinfo

            from_to_value1 = isoparse(query_ast.from_to.value1)
            if from_to_value1.tzinfo is None:
                local_datetime = from_to_value1.replace(tzinfo=local_timezone)
                utc_datetime = local_datetime.astimezone(timezone.utc)
                query_ast.from_to.value1 = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")
            else:
                utc_datetime = from_to_value1.astimezone(timezone.utc)
                query_ast.from_to.value1 = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")

            from_to_value2 = isoparse(query_ast.from_to.value2)
            if from_to_value2.tzinfo is None:
                local_datetime = from_to_value2.replace(tzinfo=local_timezone)
                utc_datetime = local_datetime.astimezone(timezone.utc)
                query_ast.from_to.value2 = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")
            else:
                utc_datetime = from_to_value2.astimezone(timezone.utc)
                query_ast.from_to.value2 = utc_datetime.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z")

            if isoparse(query_ast.from_to.value1) > isoparse(
                query_ast.from_to.value2
            ):
                raise Exception("FROM_TO value1 cannot be greater than value2")

            main_entity += "TravelTime"
            entities = [
                (entity + "TravelTime", value) for entity, value in entities
            ]
            from_to_filter = f"system_time_validity eq ({query_ast.from_to.value1}, {query_ast.from_to.value2})"
            query_ast.filter = FilterNode(
                query_ast.filter.filter + f" and {from_to_filter}"
                if query_ast.filter
                else from_to_filter
            )
            if query_ast.expand:
                if (
                    len(query_ast.expand.identifiers) == 1
                    and query_ast.expand.identifiers[0].identifier == "Commit"
                ):
                    for identifier in query_ast.expand.identifiers:
                        identifier.identifier = STA2REST.ENTITY_MAPPING.get(
                            identifier.identifier, identifier.identifier
                        )
                        identifier.subquery = (
                            QueryNode(
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                True,
                            )
                            if identifier.subquery is None
                            else identifier.subquery
                        )
                else:
                    raise Exception(
                        "Illegal operation: FROM_TO with expand is only valid for Commit"
                    )

        if DEBUG:
            print(f"Main entity: {main_entity}")

        if entities:
            if not query_ast.expand:
                query_ast.expand = ExpandNode([])

            index = 0

            # Merge the entities with the query
            for entity in entities:
                entity_name = entity[0]
                sub_query = QueryNode(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    True,
                )
                if entity[1]:
                    sub_query.filter = FilterNode(f"id eq {entity[1]}")

                if query_ast.as_of:
                    as_of_filter = (
                        f"system_time_validity eq {query_ast.as_of.value}"
                    )
                    if sub_query.filter:
                        sub_query.filter = FilterNode(
                            sub_query.filter.filter + f" and {as_of_filter}"
                        )

                if query_ast.from_to:
                    from_to_filter = f"system_time_validity eq ({query_ast.from_to.value1}, {query_ast.from_to.value2})"
                    if sub_query.filter:
                        sub_query.filter = FilterNode(
                            sub_query.filter.filter + f" and {from_to_filter}"
                        )

                # Check if we are the last entity
                if index == len(entities) - 1:
                    # Check if we have a property name
                    if uri["property_name"]:
                        single_result = True
                        # Add the property name to the select node
                        if not query_ast.select:
                            query_ast.select = SelectNode([])
                            query_ast.select.identifiers.append(
                                IdentifierNode(uri["property_name"])
                            )

                query_ast.expand.identifiers.append(
                    ExpandNodeIdentifier(entity_name, sub_query, False)
                )
                index += 1
        else:
            if uri["property_name"]:
                if not query_ast.select:
                    query_ast.select = SelectNode([])
                query_ast.select.identifiers.append(
                    IdentifierNode(uri["property_name"])
                )

        # Check if we have a filter in the query
        if main_entity_id:
            query_ast.filter = FilterNode(
                query_ast.filter.filter + f" and id eq {main_entity_id}"
                if query_ast.filter
                else f"id eq {main_entity_id}"
            )

            single_result = True

        if uri["single"]:
            single_result = True

        if query_ast.from_to:
            single_result = False

        # Check if query has an expand but not a select and does not have sub entities
        if query_ast.expand and not query_ast.select and not entities:
            # Add default columns to the select node
            default_columns = STA2REST.get_default_column_names(main_entity)
            query_ast.select = SelectNode([])
            for column in default_columns:
                query_ast.select.identifiers.append(IdentifierNode(column))

        if DEBUG:
            print(query_ast)

        # Visit the query ast to convert it
        visitor = NodeVisitor(
            main_entity,
            full_path,
            uri["ref"],
            uri["value"],
            single_result,
            entities,
        )
        query_converted = visitor.visit(query_ast)

        # Result format is allowed only for Observations
        if query_ast.result_format and main_entity != "Observation":
            raise Exception(
                "Illegal operation: $resultFormat is only valid for /Observations"
            )
        return query_converted

    @staticmethod
    def parse_entity(entity: str):
        # Check if we have an id in the entity and match only the number
        match = re.search(r"\(\d+\)", entity)
        id = None
        if match:
            # Get the id from the match without the brackets
            id = match.group(0)[1:-1]
            # Remove the id from the entity
            entity = entity.replace(match.group(0), "")

        # Check if the entity is in the ENTITY_MAPPING
        if entity in STA2REST.ENTITY_MAPPING:
            entity = STA2REST.ENTITY_MAPPING[entity]
        else:
            return None

        return (entity, id)

    @staticmethod
    def parse_uri(uri: str) -> str:
        # Split the uri by the '/' character
        version = VERSION
        parts = uri.split(version)
        parts = parts[1]
        parts = parts.split("/")

        # Remove the first part
        parts.pop(0)

        if parts[-1] == "$ref":
            entity_name = parts[-2]
        elif parts[-1] == "$value":
            entity_name = parts[-3]
        else:
            entity_name = parts[-1]
        single = False
        keys_list = list(STA2REST.ENTITY_MAPPING.keys())
        if entity_name in keys_list:
            index = keys_list.index(entity_name)
            if index > 8:
                single = True
        # Parse first entity
        main_entity = STA2REST.parse_entity(parts.pop(0))
        if not main_entity:
            raise Exception("Error parsing uri: invalid entity")

        # Check all the entities in the uri
        entities = []
        property_name = ""
        ref = False
        value = False
        for index, entity in enumerate(parts):
            # Parse the entity
            result = STA2REST.parse_entity(entity)
            if result:
                entities.append(result)
            elif entity == "$ref":
                if property_name:
                    raise Exception(
                        "Error parsing uri: $ref after property name"
                    )
                ref = True
            elif entity == "$value":
                if property_name:
                    value = True
                else:
                    raise Exception(
                        "Error parsing uri: $value without property name"
                    )
            else:
                property_name += entity
                if (
                    index < len(parts) - 1
                    and parts[index + 1] != "$ref"
                    and parts[index + 1] != "$value"
                ):
                    property_name += "/"
        # Reverse order of entities
        if entities:
            entities = entities[::-1]
            entities.append(main_entity)
            main_entity = entities[0]
            entities.pop(0)

        return {
            "version": version,
            "entity": main_entity,
            "entities": entities,
            "property_name": property_name,
            "ref": ref,
            "value": value,
            "single": single,
        }


if __name__ == "__main__":
    """
    Example usage of the STA2REST module.

    This example converts a STA query to a REST query.
    """
    query = "/v1.1/Datastreams(1)/Observations(1)/resultTime"
    print("QUERY", query)
    print("CONVERTED", STA2REST.convert_query(query))
