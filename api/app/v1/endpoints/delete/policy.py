from app import AUTHORIZATION, POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UndefinedObjectError
from fastapi import APIRouter, Depends, Header, Query, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()
user = Header(default=None, include_in_schema=False)
if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Policies",
    methods=["DELETE"],
    tags=["Policies"],
    summary="Delete a Policy",
    description="Delete a Policy for a user",
    status_code=status.HTTP_200_OK,
)
async def delete_policy(
    policy_user: str = Query(
        alias="policy_user",
        description="The user to delete the policy for",
    ),
    policy_name: str = Query(
        alias="policy_name",
        description="The name of the policy to delete",
    ),
    policy_table: str = Query(
        alias="policy_table",
        description="The table of the policy to delete (Location, Thing, HistoricalLocation, Sensor, ObservedProperty, Datastream, FeaturesOfInterest, Observation)",
    ),
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                query = """
                    SELECT roles FROM pg_policies
                    WHERE policyname = $1;
                """
                roles = await connection.fetchval(query, policy_name)

                if len(roles) == 1:
                    query = 'DROP POLICY {} ON sensorthings."{}";'.format(
                        policy_name, policy_table
                    )

                    await connection.execute(query)

                if len(roles) > 1:
                    roles.remove(policy_user)
                    query = """
                        ALTER POLICY
                        {} ON sensorthings."{}" TO {};
                    """.format(
                        policy_name, policy_table, ", ".join(roles)
                    )
                    await connection.execute(query)

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)

    except UndefinedObjectError as e:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Policy not found"},
        )
    except InsufficientPrivilegeError as e:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient privilege"},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
