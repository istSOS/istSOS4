import httpx
import traceback
import os
import pprint
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from app.utils.utils import PostgRESTError, __create_ref_format, __flatten_expand_entity, __flatten_navigation_links
from app.settings import tables, serverSettings
from sqlalchemy.orm import sessionmaker, joinedload, load_only
from sqlalchemy import create_engine, or_
from ...models import (
    Location, Thing, HistoricalLocation, ObservedProperty, Sensor,
    Datastream, FeaturesOfInterest, Observation,
    LocationTravelTime, ThingTravelTime, HistoricalLocationTravelTime,
    ObservedPropertyTravelTime, SensorTravelTime, DatastreamTravelTime,
    FeaturesOfInterestTravelTime, ObservationTravelTime
)

v1 = APIRouter()


def __handle_root(request: Request):
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": 
                request.url._url + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    } 
    return response

@v1.api_route("/{path_name:path}", methods=["GET"])
async def catch_all_get(request: Request, path_name: str):
    if not path_name:
        # Handle the root path
        return __handle_root(request)

    try:    
        # get full path from request
        full_path = request.url.path
        if request.url.query:
            full_path += "?" + request.url.query

        result = sta2rest.STA2REST.convert_query(full_path)
        subqueries = []
        for subquery in result["subqueries"]:
            subqueries.append(subquery)
        query = result["query"]
        items = query.all()
        item_dicts = [item.to_dict_expand() if result["dict_expand"] else item.to_dict() for item in items]
        query_count = result["count_query"][0].scalar()
        data = {}
        if len(item_dicts) == 1 and result["single_result"]:
            data = item_dicts[0]
            if not result["id_query_result"]:
                del data["@iot.id"]
        else:
            if not result["id_query_result"]:
                for item in item_dicts:
                    if "@iot.id" in item:
                        del item["@iot.id"]

            keys_to_check = {tp[0] for tp in result["id_subquery_result"] if not tp[1]}

            for item in item_dicts:
                for key in keys_to_check:
                    if key in item and isinstance(item[key], (list, dict)):
                        val = item[key]
                        if isinstance(val, list):
                            for v in val:
                                if isinstance(v, dict) and "@iot.id" in v:
                                    del v["@iot.id"]
                        elif isinstance(val, dict) and "@iot.id" in val:
                            del val["@iot.id"]

            nextLink = f"{os.getenv('HOSTNAME').rstrip('/v1.1/')}{full_path}"
            new_top_value = 100
            if '$top' in nextLink:
                start_index = nextLink.find('$top=') + 5
                end_index = nextLink.find('&', start_index) if '&' in nextLink[start_index:] else len(nextLink)
                top_value = int(nextLink[start_index:end_index])
                new_top_value = top_value
                nextLink = nextLink[:start_index] + str(new_top_value) + nextLink[end_index:]
            else:
                if '?' in nextLink:
                    nextLink = nextLink + f"&$top={new_top_value}"
                else:
                    nextLink = nextLink + f"?$top={new_top_value}"
            if '$skip' in nextLink:
                start_index = nextLink.find('$skip=') + 6
                end_index = nextLink.find('&', start_index) if '&' in nextLink[start_index:] else len(nextLink)
                skip_value = int(nextLink[start_index:end_index])
                new_skip_value = skip_value + new_top_value
                nextLink = nextLink[:start_index] + str(new_skip_value) + nextLink[end_index:]
            else:
                new_skip_value = new_top_value
                nextLink = nextLink + f"&$skip={new_skip_value}"
            if result["count_query"][1]:
                data["@iot.count"] = query_count

            if new_skip_value < query_count:
                data["@iot.nextLink"] = nextLink

            # Always included
            data["value"] = item_dicts

        if result['ref']:
            if 'value' in data:
                data["value"] = [{'@iot.selfLink': item.get('@iot.selfLink')} for item in data["value"] if '@iot.selfLink' in item]
            else:
                data = {'@iot.selfLink': data.get('@iot.selfLink')} if '@iot.selfLink' in data else {}

        if result['value']:
            data = data[list(data.keys())[0]]

        if len(data) == 0 or ("value" in data and len(data["value"]) == 0):
            data.pop("@iot.nextLink", None)
            data.pop("@iot.count", None)
            # return JSONResponse(
            #     status_code=status.HTTP_404_NOT_FOUND,
            #     content={
            #         "code": 404,
            #         "type": "error",
            #         "message": "Not Found"
            #     }
            # )
        return data
    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": str(e)
            }
        )
