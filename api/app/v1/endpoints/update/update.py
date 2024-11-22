import json

from app import EPSG, VERSIONING
from app.v1.endpoints.crud import insert_commit


async def set_commit(
    connection, commit_message, current_user, entity_name, entity_id
):
    commit_id = None
    if VERSIONING:
        if commit_message:
            commit_author = (
                current_user["uri"]
                if current_user and current_user["role"] != "sensor"
                else "anonymous"
            )
            commit_encoding_type = "text/plain"
            commit = {
                "message": commit_message,
                "author": commit_author,
                "encodingType": commit_encoding_type,
            }
            if current_user is not None:
                commit["user_id"] = current_user["id"]
            query = f"""
                SELECT id
                FROM sensorthings."{entity_name}"
                WHERE id = $1;
            """
            selected_id = await connection.fetchval(query, entity_id)
            if selected_id:
                commit_id = await insert_commit(connection, commit, "UPDATE")
        else:
            raise Exception("No commit message provided")
    return commit_id


async def update_entity(
    connection, entity_name, entity_id, payload, obs=False
):
    payload = {
        key: json.dumps(value) if isinstance(value, dict) else value
        for key, value in payload.items()
    }
    set_clause = ", ".join(
        [
            (
                f'"{key}" = ${i + 1}'
                if key != "location" and key != "feature"
                else f'"{key}" = ST_GeomFromGeoJSON(${i + 1})'
            )
            for i, key in enumerate(payload.keys())
        ]
    )
    if obs:
        return await connection.fetchrow(
            f"""
            UPDATE sensorthings."{entity_name}"
            SET {set_clause}
            WHERE id = {entity_id}
            RETURNING "phenomenonTime", "datastream_id";
        """,
            *payload.values(),
        )
    await connection.fetchval(
        f"""
            UPDATE sensorthings."{entity_name}"
            SET {set_clause}
            WHERE id = {entity_id};
        """,
        *payload.values(),
    )


async def handle_nested_entities(
    connection, payload, entity_id, key, field, update_table
):
    async with connection.transaction():
        if key in payload:
            if isinstance(payload[key], dict):
                payload[key] = [payload[key]]
            for item in payload[key]:
                if not isinstance(item, dict) or list(item.keys()) != [
                    "@iot.id"
                ]:
                    raise Exception(
                        f"Invalid format: Each item in '{key}' should be a dictionary with a single key '@iot.id'."
                    )
                related_id = item["@iot.id"]
                await connection.execute(
                    f'UPDATE sensorthings."{update_table}" SET {field} = {entity_id} WHERE id = {related_id};'
                )
            payload.pop(key)


def handle_associations(payload, keys):
    for key in keys:
        if key in payload:
            if list(payload[key].keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each thing dictionary should contain only the '@iot.id' key."
                )
            if key != "FeatureOfInterest":
                payload[f"{key.lower()}_id"] = payload[key]["@iot.id"]
            else:
                payload["featuresofinterest_id"] = payload[key]["@iot.id"]
            payload.pop(key)


def validate_payload_keys(payload, keys):
    invalid_keys = [key for key in payload.keys() if key not in keys]
    if invalid_keys:
        raise Exception(
            f"Invalid keys in payload for Location: {', '.join(invalid_keys)}"
        )


def validate_epsg(key):
    crs = key.get("crs")
    if crs is not None:
        epsg_code = int(crs["properties"].get("name").split(":")[1])
        if epsg_code != EPSG:
            raise ValueError(
                f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
            )
