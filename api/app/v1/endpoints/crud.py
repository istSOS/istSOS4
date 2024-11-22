import json


async def set_role(connection, current_user):
    async with connection.transaction():
        query = 'SET ROLE "{username}";'
        await connection.execute(
            query.format(username=current_user["username"])
        )


async def insert_commit(connection, payload, action):
    """
    Inserts a commit record into the database.

    Args:
        payload (dict): Commit data.
        connection: The database connection.
        action (str): Action type (e.g., "DELETE").

    Returns:
        int: The ID of the inserted commit.
    """
    async with connection.transaction():
        payload["actionType"] = action

        for key in list(payload.keys()):
            if isinstance(payload[key], dict):
                payload[key] = json.dumps(payload[key])

        keys = ", ".join(f'"{key}"' for key in payload.keys())
        values_placeholders = ", ".join(f"${i+1}" for i in range(len(payload)))
        query = f"""
            INSERT INTO sensorthings."Commit" ({keys})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        return await connection.fetchval(query, *payload.values())


async def get_datastreams_from_foi(connection, feature_of_interest_id):
    async with connection.transaction():
        query = """
            SELECT DISTINCT datastream_id
            FROM sensorthings."Observation"
            WHERE featuresofinterest_id = $1;
        """
        return await connection.fetch(query, feature_of_interest_id)


async def update_datastream_observedArea(conn, datastream_id, feature_id=None):
    async with conn.transaction():
        if feature_id is None:
            query = """
                WITH distinct_features AS (
                    SELECT DISTINCT ON (foi.id) foi.feature
                    FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                    WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1
                ),
                aggregated_geometry AS (
                    SELECT ST_ConvexHull(ST_Collect(feature)) AS agg_geom
                    FROM distinct_features
                )
                UPDATE sensorthings."Datastream"
                SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                WHERE id = $1;
            """
            await conn.execute(query, datastream_id)
        else:
            query = """
                WITH distinct_features AS (
                    SELECT DISTINCT ON (foi.id) foi.feature
                    FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                    WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1 AND foi.id != $2
                ),
                aggregated_geometry AS (
                    SELECT ST_ConvexHull(ST_Collect(feature)) AS agg_geom
                    FROM distinct_features
                )
                UPDATE sensorthings."Datastream"
                SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                WHERE id = $1;
            """
            await conn.execute(query, datastream_id, feature_id)
