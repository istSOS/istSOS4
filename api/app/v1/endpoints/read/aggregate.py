"""GET /Datastreams(id)/Aggregates: aggregation service results shaped like Observations.

Reads the six aggregation.observations_* tables directly. $func picks the
statistic returned as "result", $interval the table. Filters on phenomenonTime
compare the bucket end. A NULL statistic becomes -999.9 (sampled gap) or 0
(event bucket without events); validity is in resultQuality.
"""

from datetime import UTC
from typing import Annotated

from app import ANONYMOUS_VIEWER, AUTHORIZATION, TOP_VALUE
from app.db.asyncpg_db import get_pool
from app.sta2rest.odata_query import ast
from app.sta2rest.odata_query.grammar import ODataLexer, ODataParser
from app.utils.utils import build_nextLink
from app.v1.endpoints.aggregation_functions import MAX_ID, base_url
from app.v1.endpoints.exceptions import BadRequest, NotFound
from app.v1.endpoints.functions import set_role
from dateutil.parser import isoparse
from fastapi import APIRouter, Depends, Header, Path, Request
from fastapi.responses import JSONResponse

from .query_parameters import AggregateQueryParams, get_aggregate_query_params

v1 = APIRouter(tags=["Aggregates"])

user = Header(default=None, include_in_schema=False)
if AUTHORIZATION and not ANONYMOUS_VIEWER:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

# Identifiers interpolated into SQL come only from these constant mappings.
INTERVALS = {
    "PT10M": "observations_10min",
    "PT1H": "observations_1hour",
    "P1D": "observations_1day",
    "P7D": "observations_7days",
    "P1M": "observations_1month",
    "P1Y": "observations_1year",
}
FUNCTIONS = {
    "sum": "sum_value",
    "avg": "avg_value",
    "min": "min_value",
    "max": "max_value",
    "count": "count_numeric",
}
OUTPUT = (
    "phenomenonTime",
    "resultTime",
    "result",
    "resultQuality",
    "parameters",
    "Datastream@iot.navigationLink",
)
ALLOWED = {
    "$func",
    "$interval",
    "$filter",
    "$orderby",
    "$top",
    "$skip",
    "$count",
    "$select",
}
COMPARATORS = {
    ast.Eq: "=",
    ast.NotEq: "<>",
    ast.Lt: "<",
    ast.LtE: "<=",
    ast.Gt: ">",
    ast.GtE: ">=",
}
REVERSED = {
    ast.Eq: ast.Eq,
    ast.NotEq: ast.NotEq,
    ast.Lt: ast.Gt,
    ast.LtE: ast.GtE,
    ast.Gt: ast.Lt,
    ast.GtE: ast.LtE,
}
TIME_FIELDS = {"phenomenonTime", "resultTime"}


def result_expression(function):
    if function == "count":
        return "count_numeric"
    # Rows written before the worker stored 0 for empty event buckets are NULL too.
    return (
        f"COALESCE({FUNCTIONS[function]}, CASE WHEN "
        "configuration_snapshot->'configuration'->>'mode' = 'event' THEN 0 ELSE -999.9 END)"
    )


def field_expressions(function):
    return {
        "phenomenonTime": "bucket_end",
        "resultTime": "calculated_at",
        "result": result_expression(function),
        "resultQuality": "result_quality",
    }


class FilterSQL:
    """Translate a parsed OData $filter into SQL with $n parameters.

    Parameters are numbered from $2: $1 is the Datastream id.
    """

    def __init__(self, fields, params):
        self.fields = fields
        self.params = params

    def parameter(self, value):
        self.params.append(value)
        return f"${len(self.params) + 1}"

    def visit(self, node):
        if isinstance(node, ast.BoolOp):
            operator = "AND" if isinstance(node.op, ast.And) else "OR"
            return f"({self.visit(node.left)} {operator} {self.visit(node.right)})"
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return f"(NOT {self.visit(node.operand)})"
        if isinstance(node, ast.Compare):
            return self.compare(node)
        raise BadRequest(
            "Unsupported $filter expression: use comparisons on "
            f"{', '.join(self.fields)} joined by and/or/not."
        )

    def compare(self, node):
        left, right, comparator = node.left, node.right, type(node.comparator)
        if not isinstance(left, ast.Identifier) and isinstance(
            right, ast.Identifier
        ):
            left, right = right, left
            comparator = REVERSED.get(comparator, comparator)
        if (
            not isinstance(left, ast.Identifier)
            or left.name not in self.fields
        ):
            raise BadRequest(
                f"$filter supports only: {', '.join(self.fields)}."
            )
        column = self.fields[left.name]
        if comparator is ast.In:
            if not isinstance(right, ast.List):
                raise BadRequest("'in' requires a list.")
            values = ", ".join(
                self.parameter(self.value(left.name, item))
                for item in right.val
            )
            return f"{column} IN ({values})"
        if comparator not in COMPARATORS:
            raise BadRequest("Unsupported comparison in $filter.")
        if isinstance(right, ast.Null):
            if comparator not in (ast.Eq, ast.NotEq):
                raise BadRequest("null can only be compared with eq or ne.")
            return (
                f"{column} IS {'NOT ' if comparator is ast.NotEq else ''}NULL"
            )
        return f"{column} {COMPARATORS[comparator]} {self.parameter(self.value(left.name, right))}"

    @staticmethod
    def value(field, node):
        if field in TIME_FIELDS:
            if not isinstance(node, (ast.DateTime, ast.String)):
                raise BadRequest(f"{field} must be compared with a datetime.")
            try:
                value = isoparse(node.val)
            except ValueError:
                raise BadRequest(
                    f"Invalid datetime for {field}: {node.val}"
                ) from None
            if value.tzinfo is None:
                raise BadRequest(
                    f"Datetime for {field} needs a timezone, e.g. Z."
                )
            return value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -FilterSQL.value(field, node.operand)
        if not isinstance(node, (ast.Integer, ast.Float)):
            raise BadRequest(f"{field} must be compared with a number.")
        return float(node.py_val) if field == "result" else int(node.py_val)


def parse_filter(text, fields, params):
    try:
        tree = ODataParser().parse(ODataLexer().tokenize(text))
    except Exception:
        raise BadRequest(f"Invalid $filter: {text}") from None
    return FilterSQL(fields, params).visit(tree)


def parse_orderby(text, fields):
    terms = []
    for part in text.split(","):
        words = part.split()
        if not words or len(words) > 2 or words[0] not in fields:
            raise BadRequest(
                f"$orderby supports only: {', '.join(fields)} [asc|desc]."
            )
        direction = words[1].lower() if len(words) == 2 else "asc"
        if direction not in ("asc", "desc"):
            raise BadRequest("$orderby direction must be asc or desc.")
        terms.append(f"{fields[words[0]]} {direction.upper()}")
    return terms


def parse_non_negative(params, name, default):
    raw = params.get(name)
    if raw is None:
        return default
    if not raw.isdigit():
        raise BadRequest(f"{name} must be a non-negative integer.")
    return int(raw)


def parse_request(params):
    """Validate the query parameters; return the pieces of the SQL query."""
    unsupported = sorted(
        key for key in params if key.startswith("$") and key not in ALLOWED
    )
    if unsupported:
        raise BadRequest(
            f"Unsupported parameters for Aggregates: {', '.join(unsupported)} "
            "($expand, $as_of and $from_to are not available)."
        )
    function, interval = params.get("$func"), params.get("$interval")
    if function not in FUNCTIONS:
        raise BadRequest(f"$func is required: one of {', '.join(FUNCTIONS)}.")
    if interval not in INTERVALS:
        raise BadRequest(
            f"$interval is required: one of {', '.join(INTERVALS)}."
        )
    count = params.get("$count", "false").lower()
    if count not in ("true", "false"):
        raise BadRequest("$count must be true or false.")
    select = OUTPUT
    if params.get("$select"):
        select = tuple(name.strip() for name in params["$select"].split(","))
        unknown = [name for name in select if name not in OUTPUT]
        if unknown:
            raise BadRequest(f"$select supports only: {', '.join(OUTPUT)}.")
    fields = field_expressions(function)
    filter_params = []
    where = (
        parse_filter(params["$filter"], fields, filter_params)
        if params.get("$filter")
        else None
    )
    order = (
        parse_orderby(params["$orderby"], fields)
        if params.get("$orderby")
        else []
    )
    return {
        "table": INTERVALS[interval],
        "result": fields["result"],
        "where": where,
        "filter_params": filter_params,
        "order": order + ["bucket_end ASC"],
        "top": parse_non_negative(params, "$top", TOP_VALUE),
        "skip": parse_non_negative(params, "$skip", 0),
        "count": count == "true",
        "select": select,
    }


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def clean(value):
    """Drop binary floating-point noise (8.799999999999997 -> 8.8): 12 significant digits."""
    return float(f"{value:.12g}") if isinstance(value, float) else value


def serialize(row, datastream_id, select):
    item = {
        "phenomenonTime": f"{iso(row['bucket_start'])}/{iso(row['bucket_end'])}",
        "resultTime": iso(row["calculated_at"]),
        "result": clean(row["result"]),
        "resultQuality": row["result_quality"],
        "parameters": {
            "countTotal": row["count_total"],
            "countNumeric": row["count_numeric"],
            "expectedCount": row["expected_count"],
            "coverage": clean(row["coverage"]),
            "evaluatedUntil": iso(row["evaluated_until"]),
            # The bucket is still being filled: values can change in later runs.
            "open": row["evaluated_until"] < row["bucket_end"],
        },
        "Datastream@iot.navigationLink": f"{base_url()}/Datastreams({datastream_id})",
    }
    return {key: item[key] for key in select}


@v1.get(
    "/Datastreams({datastream_id})/Aggregates",
    summary="Get aggregated values of a Datastream",
    description=(
        "Results of the aggregation service, shaped like Observations. Required: "
        "$func (sum, avg, min, max, count) and $interval (PT10M, PT1H, P1D, P7D, "
        "P1M, P1Y). Supports $filter (phenomenonTime = bucket end, resultTime, "
        "result, resultQuality), $orderby, $top, $skip, $count, $select. "
        "A missing value is -999.9 (0 for event series); check resultQuality "
        "for validity and parameters.open for buckets still being filled."
    ),
)
async def get_datastream_aggregates(
    request: Request,
    datastream_id: Annotated[int, Path(ge=1, le=MAX_ID)],
    current_user=user,
    pool=Depends(get_pool),
    # Documents the parameters in Swagger; parse_request validates the raw query.
    params: AggregateQueryParams = Depends(get_aggregate_query_params),
):
    query = parse_request(request.query_params)
    where = "datastream_id = $1" + (
        f" AND {query['where']}" if query["where"] else ""
    )
    params = [datastream_id, *query["filter_params"]]
    table = f"aggregation.{query['table']}"

    async with pool.acquire() as connection, connection.transaction():
        if current_user is not None:
            await set_role(connection, current_user)
        elif ANONYMOUS_VIEWER:
            await set_role(connection, {"username": "guest"})
        # Runs with the caller's role: a Datastream they cannot see is not found.
        if not await connection.fetchval(
            'SELECT EXISTS (SELECT 1 FROM sensorthings."Datastream" WHERE id=$1);',
            datastream_id,
        ):
            raise NotFound(f"Datastream {datastream_id} not found.")
        if not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM aggregation.datastream_configuration WHERE datastream_id=$1);",
            datastream_id,
        ):
            raise NotFound(
                f"No aggregation configured for Datastream {datastream_id}."
            )
        rows = await connection.fetch(
            f"""
            SELECT bucket_start, bucket_end, calculated_at, {query['result']} AS result,
                   result_quality, count_total, count_numeric, expected_count, coverage,
                   evaluated_until
            FROM {table}
            WHERE {where}
            ORDER BY {', '.join(query['order'])}
            LIMIT {query['top'] + 1} OFFSET {query['skip']}
            """,
            *params,
        )
        total = (
            await connection.fetchval(
                f"SELECT count(*) FROM {table} WHERE {where}", *params
            )
            if query["count"]
            else None
        )

    body = {}
    if total is not None:
        body["@iot.count"] = total
    body["value"] = [
        serialize(row, datastream_id, query["select"])
        for row in rows[: query["top"]]
    ]
    if len(rows) > query["top"]:
        full_path = request.url.path + (
            f"?{request.url.query}" if request.url.query else ""
        )
        next_link = build_nextLink(full_path, query["top"] + 1)
        if next_link:
            body["@iot.nextLink"] = next_link
    return JSONResponse(content=body)
