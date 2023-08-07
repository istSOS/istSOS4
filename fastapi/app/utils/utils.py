import httpx
from app.sta2rest import sta2rest

class PostgRESTError(Exception):
    """
    Exception raised for errors in the PostgREST response.
    """
    pass

def get_result_type_and_column(input_string):
    """
    Get the result type and the column name for the result

    Args:
        input_string (str): The input string

    Returns:
        int: The result type
        str: The column name
    """
    try:
        value = eval(str(input_string))
    except (SyntaxError, NameError):
        result_type = 0
        column_name = "resultString"
    else:
        if isinstance(value, int):
            result_type = 1
            column_name = "resultInteger"
        elif isinstance(value, float):
            result_type = 2
            column_name = "resultDouble"
        elif isinstance(value, dict):
            result_type = 4
            column_name = "resultJSON"
        else:
            result_type = None
            column_name = None

    if input_string == "true" or input_string == "false":
        result_type = 3
        column_name = "resultBoolean"

    if result_type is not None:
        return result_type, column_name
    else:
        raise Exception("Cannot cast result to a valid type")
    
def flatten_entity_body(entity, body, name = None):
    """
    Flatten the entity body

    Args:
        entity (dict): The entity
        body (dict): The body
        name (str, optional): The name of the entity. Defaults to None.

    Returns:
        dict: The flattened entity body
    """

    # check if entity is an array
    if isinstance(entity, list):
        # loop trought all the values
        for item in entity:
            # create the entity
            flatten_entity_body(item, body, name)
        return body

    for key in list(entity):
        if isinstance(key, str) and key in sta2rest.STA2REST.ENTITY_MAPPING:
            converted_key = sta2rest.STA2REST.convert_entity(key)
            body[converted_key] = entity[key]
            
            flatten_entity_body(entity[key], body, converted_key)
            
            if name:
                if isinstance(body[name], list):
                    for item in body[name]:
                        item[converted_key] = {
                            "@iot.id": entity[key]["@iot.id"] if "@iot.id" in entity[key] else None
                        }
                else:
                    body[name][converted_key] = {
                        "@iot.id": entity[key]["@iot.id"] if "@iot.id" in entity[key] else None
                    }

    return body

def format_entity_body(entity_body): 
    """
    Format the entity body

    Args:
        entity_body (dict): The entity body

    Returns:
        dict: The formatted entity body
    """

    # Check if entity_body is an array
    if isinstance(entity_body, list):
        # Loop trought all the values
        for i in range(len(entity_body)):
            # Create the entity
            entity_body[i] = format_entity_body(entity_body[i])
        return entity_body

    formatted_body = {}
    # Loop trought all the keys in the body
    for key in entity_body:
        if isinstance(key, str) and key in sta2rest.STA2REST.ENTITY_MAPPING:
            if "@iot.id" in entity_body[key]:
                new_key = sta2rest.STA2REST.convert_to_database_id(key)
                formatted_body[new_key] = entity_body[key]["@iot.id"]
        elif key == "result":
            value = entity_body["result"]
            result_type, column_name = get_result_type_and_column(value)
            formatted_body[column_name] = value
            formatted_body["resultType"] = result_type
        else:
            formatted_body[key] = entity_body[key]

    return formatted_body

async def create_entity(entity_name, body):
    """
    Create an entity

    Args:
        entity_name (str): The entity name
        body (dict): The body

    Raises:
        PostgRESTError: If the entity cannot be created

    """

    entity_body = {
        entity_name: body
    }

    body = flatten_entity_body(entity_body, entity_body)


    # Creation order
    created_ids = {}
    creation_order = ["Location","Thing", "Sensor", "ObservedProperty", "FeaturesOfInterest", "Datastream", "Observation"]
    for entity_name in creation_order:
        if entity_name in body:
            
            formatted_body = format_entity_body(body[entity_name])

            if "@iot.id" in formatted_body:
                continue

            # check if the entity has sub entities and if they are empty check if id is present
            if isinstance(formatted_body, list):
                for item in formatted_body:
                    for key in item:
                        if key in created_ids:
                            item[key] = created_ids[key]
            else:
                for key in formatted_body:
                    # check if key is present in created_ids
                    if key in created_ids:
                        formatted_body[key] = created_ids[key]

            url = "http://postgrest:3000/" + entity_name

            print("Creating entity: ", entity_name)
            print("Body: ", formatted_body)
            
            async with httpx.AsyncClient() as client:   
                # post to postgrest
                r = await client.post(url, json=formatted_body, headers={"Prefer": "return=representation"})

                # get response
                result = r.json()

                # print r status
                if r.status_code != 201:
                    raise PostgRESTError(result["message"])
                
                # get first element of the result
                result = result[0]

                # get the id of the created entity
                id_key = sta2rest.STA2REST.convert_to_database_id(entity_name)
                created_ids[id_key] = result["id"]

                print("Created entity: ", id_key, " with id: ", result["id"])

    return None


def __flatten_navigation_links(row):
    """
    Flatten the navigation links

    Args:
        row (dict): The row

    Returns:
        dict: The flattened navigation links
    """
    if row and "@iot.navigationLink" in row:
        # merge all the keys from the navigationLink
        row.update(row["@iot.navigationLink"])
        del row["@iot.navigationLink"]

        # check if skip@iot.navigationLink is present and remove it
        if "skip@iot.navigationLink" in row:
            del row["skip@iot.navigationLink"]

def __flatten_expand_entity(data):
    """
    Flatten the entity

    Args:
        data (dict): The data

    Returns:
        dict: The flattened entity
    """

    # Check if it is an array
    if not isinstance(data, list):
        # throw an error
        raise PostgRESTError(data)

    # check if data is empty
    if not data:
        return data
    
    # Check if there is only one key and it is in an ENTITY_MAPPING from the sta2rest module
    if len(data[0].keys()) == 1 and list(data[0].keys())[0] in sta2rest.STA2REST.ENTITY_MAPPING:
        # Get the value of the first key
        key_name = list(data[0].keys())[0]
        data = data[0][key_name]

    return data

def __create_ref_format(data):
    """
    Create the ref format

    Args:
        data (dict): The data

    Returns:
        dict: The ref format
    """

    rows = [data]

    # Check if it is an array
    if isinstance(data, list):
        key_name = list(data[0].keys())[0]
        # Check if the key is in an ENTITY_MAPPING from the sta2rest module
        if key_name in sta2rest.STA2REST.ENTITY_MAPPING:
            rows = data[0][key_name]
            if not isinstance(rows, list):
                rows = [rows]
        else:
            rows = data

    data = {
        "value": []
    }

    for row in rows:
        data["value"].append({
            "@iot.selfLink": row["@iot.selfLink"]
        })
    
    return data