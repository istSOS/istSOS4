"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
import re 
import urllib.parse
from odata_query.grammar import ODataLexer
from odata_query.grammar import ODataParser
from filter_visitor import FilterVisitor

odata_filter_lexer = ODataLexer()
odata_filter_parser = ODataParser()

class STA2REST:
    """
    This class provides utility functions to convert various elements used in SensorThings queries to their corresponding
    representations in a compatible PostgREST REST API.
    """

    # Mapping from SensorThings entities to their corresponding database table names
    ENTITY_MAPPING = {
        "Things": "Thing",
        "Locations": "Location",
        "Sensors": "Sensor",
        "ObservedProperties": "ObservedProperty",
        "Datastreams": "Datastream",
        "Observations": "Observation",
        "FeaturesOfInterest": "FeatureOfInterest",
        "HistoricalLocations": "HistoricalLocation",
    }

    # Mapping from SensorThings properties to their corresponding database column names
    PROPERTY_MAPPING = {
        "name": "name",
        "description": "description",
        "encodingType": "encodingType",
        "metadata": "metadata",
        "definition": "definition",
    }

    # Mapping from SensorThings query parameters to their corresponding PostgRESTR query parameters
    QUERY_PARAM_MAPPING = {
        "$filter": "filter",
        "$orderby": "order",
        "$top": "limit",
        "$skip": "offset",
        "$select": "select",
        "$count": "count",
        "$expand": "expand",
    }

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
    def convert_property(property_name: str) -> str:
        """
        Converts a property name from STA format to REST format.

        Args:
            property_name (str): The property name in STA format.

        Returns:
            str: The converted property name in REST format.
        """
        return STA2REST.PROPERTY_MAPPING.get(property_name, property_name)

    @staticmethod
    def convert_query_param(query_param: str) -> str:
        """
        Converts a query parameter name from STA format to REST format.

        Args:
            query_param (str): The query parameter name in STA format.

        Returns:
            str: The converted query parameter name in REST format.
        """
        return STA2REST.QUERY_PARAM_MAPPING.get(query_param, query_param)

    @staticmethod
    def split_but_not_between(query: str, split: str, delim1: str, delim2: str) -> list:
        """
        Splits a string by a given character, but only if the character is not between two other given characters.

        Args:
            query (str): The string to split.
            split (str): The character to split by.
            delim1 (str): The first delimiter character.
            delim2 (str): The second delimiter character.

        Returns:
            list: The list of substrings.
        """

        result = []
        stack = []
        current = ''
        for char in query:
            # Check if the current character is a delimiter
            if char == delim1:
                current += char
                stack.append(delim1)
            # Check if the current character is a delimiter
            elif char == delim2:
                current += char
                # Check if the current delimiter is the same as the last one in the stack
                if stack and stack[-1] == delim1:
                    stack.pop()
            elif char == split:
                if stack:
                    # Inside parentheses, treat comma as part of the current substring
                    current += char
                else:
                    # Outside parentheses, split the string and reset the current substring
                    result.append(current.strip())
                    current = ''
            else:
                # Regular character, add it to the current substring
                current += char
        
        # Add the last substring
        if current:
            result.append(current.strip())

        return result

    @staticmethod
    def convert_order_by_value(value: str) -> str:
        """
        Converts an order by value from STA format to REST format.

        Args:
            value (str): The order by value in STA format.

        Returns:
            str: The converted order by value in REST format.
        """

        # strip all the spaces after commas ",   " -> ","
        value = re.sub(r",\s+", ",", value)
        # replace space and slash with dot
        value = value.replace(" ", ".").replace("/", ".")
        return value

    @staticmethod
    def convert_filter_by_value(value: str) -> str:
        """
        Converts a filter by value from STA format to REST format.

        see https://docs.ogc.org/is/18-088/18-088.html#_built_in_filter_operations
        see https://postgrest.org/en/stable/references/api/tables_views.html#logical-operators

        Args:
            value (str): The filter by value in STA format.

        Returns:
            str: The converted filter by value in REST format.
        """
        # Get the AST tree from the filter
        ast = odata_filter_parser.parse(odata_filter_lexer.tokenize(value))
        # Visit the tree to convert the filter
        res = FilterVisitor().visit(ast)
        return res

    @staticmethod
    def convert_expand(expand_query: str, previous_entity: str = None) -> str:
        """
        Converts an expand query from STA format to REST format.

        Args:
            expand_query (str): The expand query in STA format.
            previous_entity (str, optional): The previous entity name. Defaults to None.

        Returns:
            str: The converted expand query in REST format.
        """

        # split by comma but not between parentheses
        result = STA2REST.split_but_not_between(expand_query, ",", "(", ")")
    
        entities = []
        additionals = []
        for entity in result:
            converted_entity = ""
            has_select = False
            # Check if the entity has a subquery
            if "(" in entity:
                entity, subquery = entity.split("(",1)
                c_entity = STA2REST.convert_entity(entity)
                converted_entity = c_entity + "("
                subquery = subquery[:-1]
                # Get all the actions in the subquery
                actions = STA2REST.split_but_not_between(subquery, ";", "(", ")")
                for action in actions:
                    param, value = action.split("=",1)
                    converted_param = STA2REST.convert_query_param(param)
                    if param == "$expand":
                        # Recursively convert the subquery
                        result = STA2REST.convert_expand(value, c_entity)
                        entities.extend(result["select"].split(","))
                        additionals += result["additionals"]
                    elif param == "$select":
                        has_select = True
                        converted_entity += value
                    else:
                        # Adjust values
                        if param == "$orderby":
                            value = STA2REST.convert_order_by_value(value)
                        elif param == "$filter":
                            value = STA2REST.convert_filter_by_value(value)

                        # Add the previous entity if present
                        p_entity = previous_entity + "." if previous_entity != None else ""

                        # Add the action to the additionals
                        if param != "$filter":
                            additionals.append(f"{p_entity}{c_entity}.{converted_param}={value}")
                        else:
                            additionals.append(f"{p_entity}{c_entity}.{value}")
            
            if not has_select:
                converted_entity = STA2REST.convert_entity(entity) + "(*"
            
            converted_entity += ")"
            
            entities.append(converted_entity)

        return {
            "select": ",".join(entities),
            "additionals": additionals
        }

    @staticmethod
    def convert_query(sta_query: str) -> str:
        """
        Converts a query from STA format to REST format.

        Args:
            sta_query (str): The query in STA format.

        Returns:
            str: The converted query in REST format.
        """

        # remove unwanted characters from the query
        sta_query = urllib.parse.unquote(sta_query)

        # get the query parameters
        sta_query = sta_query.split("&")
        converted_query_params = {}

        # convert the query parameters
        for query_param in sta_query:
            key, value = query_param.split("=", 1)
            if key == "$expand":
                converted_value = STA2REST.convert_expand(value)
            else:
                if key == "$orderby":
                    value = STA2REST.convert_order_by_value(value)
                elif key == "$filter":
                    value = STA2REST.convert_filter_by_value(value)
                converted_value = value
            converted_key = STA2REST.convert_query_param(key)
            converted_query_params[converted_key] = converted_value

        # Check for expand and if present merge the select
        if "expand" in converted_query_params and "select" in converted_query_params["expand"]:
            if "select" in converted_query_params:
                converted_query_params["select"] += "," + converted_query_params["expand"]["select"]
            else:
                converted_query_params["select"] = converted_query_params["expand"]["select"]
        
        # Get additionals parameters and remove them from the query
        additionals = []
        if "expand" in converted_query_params and "additionals" in converted_query_params["expand"]:
            additionals = converted_query_params["expand"]["additionals"]
            del converted_query_params["expand"]

        # Check if the filter is present and move it to the additionals
        if "filter" in converted_query_params:
            additionals.append(converted_query_params["filter"])
            del converted_query_params["filter"]

        # Merge in format key=value&key=value 
        converted_query = "&".join([f"{key}={value}" for key, value in converted_query_params.items()])
        
        # Add additionals
        for additional in additionals:
            converted_query += "&" + additional

        # Remove the first & if present
        if converted_query.startswith("&"):
            converted_query = converted_query[1:]
        
        return converted_query

if __name__ == "__main__":
    """
    Example usage of the STA2REST module.

    This example converts a STA query to a REST query.
    """
    query = "$filter=result gt 20 or result le 3.5"
    print("QUERY", query)
    print("CONVERTED", STA2REST.convert_query(query))