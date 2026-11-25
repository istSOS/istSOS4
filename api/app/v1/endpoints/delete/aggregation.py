"""Administrator delete endpoint for aggregation settings."""

from app.v1.endpoints.aggregation_functions import (
    MAX_ID,
    TABLE,
    ensure_configuration_available,
    pool_dependency,
    require_administrator,
    user,
)
from app.v1.endpoints.exceptions import NotFound
from fastapi import APIRouter, Path
from fastapi.responses import Response

v1 = APIRouter(tags=["Aggregations"])


@v1.delete(
    "/Aggregations({aggregation_id})",
    status_code=204,
    summary="Delete an aggregation configuration",
)
async def delete_aggregation_configuration(
    aggregation_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection, connection.transaction():
        await ensure_configuration_available(connection)
        deleted = await connection.fetchval(
            f"DELETE FROM {TABLE} WHERE id=$1 RETURNING id;",
            aggregation_id,
        )
        if deleted is None:
            raise NotFound("Aggregation configuration not found.")
    return Response(status_code=204)
