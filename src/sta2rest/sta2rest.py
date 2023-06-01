"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
import re 

class STA2REST:
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

    PROPERTY_MAPPING = {
        "name": "name",
        "description": "description",
        "encodingType": "encodingType",
        "metadata": "metadata",
        "definition": "definition",
    }

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
        return STA2REST.ENTITY_MAPPING.get(entity, entity)

    @staticmethod
    def convert_property(property_name: str) -> str:
        return STA2REST.PROPERTY_MAPPING.get(property_name, property_name)

    @staticmethod
    def convert_query_param(query_param: str) -> str:
        return STA2REST.QUERY_PARAM_MAPPING.get(query_param, query_param)

    @staticmethod
    def split_but_not_between(query: str, split: str, delim1: str, delim2: str) -> list:
        result = []
        stack = []
        current = ''
        for char in query:
            if char == delim1:
                current += char
                stack.append(delim1)
            elif char == delim2:
                current += char
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
        
        if current:
            result.append(current.strip())

        return result

    @staticmethod
    def convert_order_by_value(value: str) -> str:
        # strip all the spaces after commas ",   " -> ","
        value = re.sub(r",\s+", ",", value)
        # replace space and slash with dot
        value = value.replace(" ", ".").replace("/", ".")
        return value

    @staticmethod
    def convert_expand(expand_query: str, previous_entity: str = None) -> str:
        result = STA2REST.split_but_not_between(expand_query, ",", "(", ")")
    
        entities = []
        additionals = []
        for entity in result:
            converted_entity = ""
            has_select = False
            if "(" in entity:
                entity, subquery = entity.split("(",1)
                c_entity = STA2REST.convert_entity(entity)
                converted_entity = c_entity + "("
                subquery = subquery[:-1]
                actions = STA2REST.split_but_not_between(subquery, ";", "(", ")")
                for action in actions:
                    param, value = action.split("=",1)
                    converted_param = STA2REST.convert_query_param(param)
                    if param == "$expand":
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

                        p_entity = previous_entity + "." if previous_entity != None else ""
                        additionals.append(f"{p_entity}{c_entity}.{converted_param}={value}")
            
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
        sta_query = sta_query.split("&")
        converted_query_params = {}
        for query_param in sta_query:
            key, value = query_param.split("=", 1)
            if key == "$expand":
                converted_value = STA2REST.convert_expand(value)
            else:
                if key == "$orderby":
                    value = STA2REST.convert_order_by_value(value)
                converted_value = value
            converted_key = STA2REST.convert_query_param(key)
            converted_query_params[converted_key] = converted_value

        # Check for expand and if present merge the select
        if "expand" in converted_query_params and "select" in converted_query_params["expand"]:
            if "select" in converted_query_params:
                converted_query_params["select"] += "," + converted_query_params["expand"]["select"]
            else:
                converted_query_params["select"] = converted_query_params["expand"]["select"]
        
        # Get additionals and remove them from the query
        additionals = []
        if "expand" in converted_query_params and "additionals" in converted_query_params["expand"]:
            additionals = converted_query_params["expand"]["additionals"]
            del converted_query_params["expand"]

        # merge in format key=value&key=value 
        converted_query = "&".join([f"{key}={value}" for key, value in converted_query_params.items()])
        
        for additional in additionals:
            converted_query += "&" + additional
        
        return converted_query