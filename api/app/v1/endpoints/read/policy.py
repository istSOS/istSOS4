import ujson
from app import ANONYMOUS_VIEWER, AUTHORIZATION
from app.db.asyncpg_db import get_pool
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, Query, status
from fastapi.responses import JSONResponse

from .read import set_role

v1 = APIRouter()
user = Header(default=None, include_in_schema=False)

if AUTHORIZATION and not ANONYMOUS_VIEWER:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Policies",
    methods=["GET"],
    tags=["Policies"],
    summary="Get all policies",
    description="Returns all the policies provided by this api (subject to any parameters set)",
    status_code=status.HTTP_200_OK,
)
async def get_policies(
    policy_user: str = Query(
        None,
        alias="policy_user",
        description="The user to get the policies for",
    ),
    policy_name: str = Query(
        None,
        alias="policy_name",
        description="The name of the policy to get",
    ),
    policy_table: str = Query(
        None,
        alias="policy_table",
        description="The table of the policy to get (Location, Thing, HistoricalLocation, Sensor, ObservedProperty, Datastream, FeaturesOfInterest, Observation)",
    ),
    policy_operation: str = Query(
        None,
        alias="policy_operation",
        description="The operation of the policy to get (SELECT, INSERT, UPDATE, DELETE, ALL)",
    ),
    current_user=user,
    pool=Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                params = []
                conditions = []

                if policy_user is not None:
                    conditions.append(f"${len(params) + 1} = ANY (roles)")
                    params.append(policy_user)

                if policy_name is not None:
                    conditions.append(f"policyname = ${len(params) + 1}")
                    params.append(str(policy_name))

                if policy_table is not None:
                    conditions.append(f"tablename = ${len(params) + 1}")
                    params.append(policy_table)

                if policy_operation is not None:
                    conditions.append(f"cmd = ${len(params) + 1}")
                    params.append(policy_operation)

                query = """
                    SELECT row_to_json(t) AS policies
                    FROM (
                        SELECT * FROM pg_policies
                        WHERE 1 = 1
                        {}
                    ) t
                """.format(
                    " AND " + " AND ".join(conditions) if conditions else ""
                )

                policies = await connection.fetch(query, *params)

                policies = [
                    ujson.loads(record["policies"]) for record in policies
                ]

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"value": policies}
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
