# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module: STA2REST filter visitor

Author: Filippo Finke

This module provides a visitor for the filter AST.
"""

import operator
from typing import Any, Callable, List, Optional, Union

from app import EPSG
from geoalchemy2 import WKTElement
from sqlalchemy import (
    JSON,
    Boolean,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    cast,
)
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql import functions
from sqlalchemy.sql.expression import (
    BinaryExpression,
    BindParameter,
    BooleanClauseList,
    ClauseElement,
    ColumnClause,
    False_,
    Null,
    True_,
    all_,
    and_,
    any_,
    cast,
    extract,
    false,
    literal,
    null,
    or_,
    true,
)
from sqlalchemy.types import Date, Time

from ..models import *
from .odata_query import ast
from .odata_query import exceptions as ex
from .odata_query import visitor

SELECT_MAPPING = {
    "encodingType": "encoding_type",
    "metadata": "sensor_metadata",
    "unitOfMeasurement": "unit_of_measurement",
    "observationType": "observation_type",
    "observedArea": "observed_area",
    "phenomenonTime": "phenomenon_time",
    "resultTime": "result_time",
    "resultQuality": "result_quality",
    "validTime": "valid_time",
    "displayName": "display_name",
    "authId": "auth_id",
    "attributionText": "attribution_text",
    "termsOfUse": "terms_of_use",
    "privacyPolicy": "privacy_policy",
    "creationTime": "creation_time",
    "startTime": "start_time",
    "endTime": "end_time",
    "dataQuality": "data_quality",
    "externalResource": "external_resource",
}


def resolve_field(model, name):
    """Resolve a logical (snake_case) field name to a mapped column attribute.

    The Observation phenomenonTime is split into two TIMESTAMPTZ columns; the
    logical "phenomenon_time" resolves to the start column, which is the scalar
    handle used for select / filter / orderby (output recombines start+end in
    get_select_attr).
    """
    if name == "phenomenon_time" and not hasattr(model, "phenomenon_time"):
        name = "phenomenon_time_start"
    return getattr(model, name)


class FilterVisitor(visitor.NodeVisitor):
    """
    Visitor for the filter AST.
    """

    def __init__(self, root_model: str):
        self.root_model = root_model
        self.join_relationships: List[InstrumentedAttribute] = []

    def visit_All(self, node: ast.All) -> str:
        return all_

    def visit_Any(self, node: ast.Any) -> str:
        return any_

    def visit_Null(self, node: ast.Null) -> Null:
        return null()

    def visit_Integer(self, node: ast.Integer) -> BindParameter:
        return literal(node.py_val)

    def visit_Float(self, node: ast.Float) -> BindParameter:
        return literal(node.py_val)

    def visit_Boolean(self, node: ast.Boolean) -> Union[True_, False_]:
        if node.val == "true":
            return true()
        return false()

    def visit_String(self, node: ast.String) -> BindParameter:
        return literal(node.py_val)

    def visit_DateTime(self, node: ast.DateTime) -> BindParameter:
        return literal(node.py_val)

    def visit_Duration(self, node: ast.Duration) -> BindParameter:
        return literal(node.py_val)

    ####################################################################################
    # Comparison Operators
    ####################################################################################

    def visit_Eq(
        self, node: ast.Eq
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.eq

    def visit_NotEq(
        self, node: ast.NotEq
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.ne

    def visit_Gt(
        self, node: ast.Gt
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.gt

    def visit_Lt(
        self, node: ast.Lt
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.lt

    def visit_GtE(
        self, node: ast.GtE
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.ge

    def visit_LtE(
        self, node: ast.LtE
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return operator.le

    ####################################################################################
    # Logical Operators
    ####################################################################################

    def visit_And(
        self, node: ast.And
    ) -> Callable[[ClauseElement, ClauseElement], BooleanClauseList]:
        return and_

    def visit_Or(
        self, node: ast.Or
    ) -> Callable[[ClauseElement, ClauseElement], BooleanClauseList]:
        return or_

    def visit_Not(
        self, node: ast.Not
    ) -> Callable[[ClauseElement], ClauseElement]:
        return operator.invert

    def visit_USub(
        self, node: ast.USub
    ) -> Callable[[ClauseElement], ClauseElement]:
        return operator.neg

    def visit_In(
        self, node: ast.In
    ) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
        return lambda a, b: a.in_(b)

    def visit_List(self, node: ast.List) -> list:
        return [self.visit(n) for n in node.val]

    ####################################################################################
    # Arithmetic Operators
    ####################################################################################

    def visit_Add(self, node: ast.Add) -> Callable[[Any, Any], Any]:
        return operator.add

    def visit_Sub(self, node: ast.Sub) -> Callable[[Any, Any], Any]:
        return operator.sub

    def visit_Mult(self, node: ast.Mult) -> Callable[[Any, Any], Any]:
        return operator.mul

    def visit_Div(self, node: ast.Div) -> Callable[[Any, Any], Any]:
        return operator.truediv

    def visit_Mod(self, node: ast.Mod) -> Callable[[Any, Any], Any]:
        return operator.mod

    def visit_Identifier(
        self, node: ast.Identifier
    ) -> ColumnClause | BooleanClauseList:
        try:
            name = node.name.lower() if node.name[0].isupper() else node.name
            for old_key, new_key in SELECT_MAPPING.items():
                if old_key == node.name:
                    name = new_key
            return resolve_field(globals()[self.root_model], name)
        except AttributeError:
            raise ex.InvalidFieldException(node.name)

    @staticmethod
    def _relationship_attribute(model, segment):
        """Resolve an OGC navigation property name to a relationship attribute.

        The navigation name is mapped to the internal (singular, lower-case)
        relationship attribute through the same table used by $expand
        (STA2REST.ENTITY_MAPPING), so e.g. Thing/"Datastreams",
        Datastream/"Sensor" and Observation/"FeatureOfInterest" all resolve, and
        both the singular and the plural spelling of a collection map to the
        same attribute. Returns None when `segment` is not a navigation property
        on `model` (i.e. it is a column).
        """
        # Deferred import: breaks the sta2rest <-> visitors <-> filter_visitor
        # import cycle that a module-level import would create.
        from .sta2rest import STA2REST

        attr_name = STA2REST.ENTITY_MAPPING.get(segment, segment)
        attr_name = attr_name.replace("TravelTime", "").lower()
        candidate = getattr(model, attr_name, None)
        if candidate is not None and isinstance(
            getattr(candidate, "property", None), RelationshipProperty
        ):
            return candidate
        return None

    @staticmethod
    def _resolve_path_column(model, segments):
        """Resolve a column on `model`, with an optional trailing JSON path.

        `segments[0]` is the column; any further segments index into it as a
        JSON path (`#>`), matching the existing properties/<key> behaviour.
        """
        col_name = segments[0]
        name = col_name.lower() if col_name[0].isupper() else col_name
        name = SELECT_MAPPING.get(col_name, name)
        try:
            column = resolve_field(model, name)
        except AttributeError:
            raise ex.InvalidFieldException(col_name)
        if len(segments) > 1:
            path = "{" + ", ".join(segments[1:]) + "}"
            column = column.op("#>")(path)
        return column

    def visit_Attribute(self, node: ast.Attribute) -> ColumnClause:
        # Flatten the navigation path into ordered segments, e.g.
        # Datastreams/Sensor/id -> ["Datastreams", "Sensor", "id"].
        attributes = []
        owner = node.owner
        while not isinstance(owner, ast.Identifier):
            attributes.append(owner.attr)
            owner = owner.owner
        segments = [owner.name] + attributes[::-1] + [node.attr]

        # Walk the leading relationship hops, emitting one JOIN per hop, until a
        # segment is no longer a navigation property. SensorThings allows direct
        # multi-hop traversal in $filter (e.g.
        # Things?$filter=Datastreams/Sensor/id eq 1) without any()/all(), so
        # every relationship segment is joined. The final segment is always the
        # column, hence the `len(segments) - 1` bound.
        model = globals()[self.root_model]
        index = 0
        while index < len(segments) - 1:
            rel_attr = self._relationship_attribute(model, segments[index])
            if rel_attr is None:
                break
            self.join_relationships.append(rel_attr)
            model = inspect(rel_attr).property.mapper.class_
            index += 1

        # Remaining segments are a column on the reached entity plus an optional
        # JSON sub-path.
        return self._resolve_path_column(model, segments[index:])

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.op)

        if (
            isinstance(node.left, ast.Identifier)
            and node.left.name == "result"
        ):
            left = FilterVisitor.handle_observation_result(
                self.root_model, op, right
            )
        if (
            isinstance(node.right, ast.Identifier)
            and node.right.name == "result"
        ):
            right = FilterVisitor.handle_observation_result(
                self.root_model, op, left
            )
        return op(left, right)

    def visit_BoolOp(self, node: ast.BoolOp) -> BooleanClauseList:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.op)
        return op(left, right)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> ClauseElement:
        # req/request-data/built-in-filter-operations: apply the unary operator
        # (logical `not` -> SQL NOT, arithmetic `-` -> negation) to its operand.
        # Without this, UnaryOp nodes fell through to generic_visit which
        # returned None, so `not (...)` produced an empty/always-false filter.
        operand = self.visit(node.operand)
        op = self.visit(node.op)
        return op(operand)

    def visit_Compare(self, node: ast.Compare) -> BinaryExpression:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.comparator)

        if (
            isinstance(node.left, ast.Identifier)
            and "systemTimeValidity" in left.name
        ):
            if getattr(op, "__name__", "") == "eq":
                if isinstance(right, list):
                    right = functions.func.tstzrange(
                        functions.func.timestamptz(right[0]),
                        functions.func.timestamptz(right[1]),
                    )
                    return left.op("&&")(right)
                return left.op("@>")(functions.func.timestamptz(right))

        if isinstance(node.left, ast.Attribute) and not isinstance(
            node.right, ast.Attribute
        ):
            if not isinstance(left, InstrumentedAttribute):
                right = functions.func.to_jsonb(cast(right, Text))

        if isinstance(node.right, ast.Attribute) and not isinstance(
            node.left, ast.Attribute
        ):
            if not isinstance(right, InstrumentedAttribute):
                left = functions.func.to_jsonb(cast(left, Text))

        if isinstance(node.left, ast.Identifier) and (
            left.name in {"phenomenonTime", "validTime"}
            or (left.name == "resultTime" and left.table.name == "Datastream")
        ):
            return op(left, functions.func.tstzrange(right, right, "[]"))

        if isinstance(node.right, ast.Identifier) and (
            right.name in {"phenomenonTime", "validTime"}
            or (
                right.name == "resultTime" and right.table.name == "Datastream"
            )
        ):
            return op(functions.func.tstzrange(left, left, "[]"), right)

        if (
            isinstance(node.left, ast.Identifier)
            and node.left.name == "result"
        ):
            left = FilterVisitor.handle_observation_result(
                self.root_model, op, right, left=True
            )
            return or_(*left)

        if (isinstance(node.right, ast.Identifier)) and (
            node.right.name == "result"
        ):
            right = FilterVisitor.handle_observation_result(
                self.root_model, op, left
            )
            return or_(*right)

        return op(left, right)

    def visit_Call(self, node: ast.Call) -> ClauseElement:
        try:
            handler = (
                getattr(self, "func_" + node.func.name.lower())
                if len(node.func.namespace) == 0
                else getattr(
                    self,
                    "func_" + node.func.namespace[0] + node.func.name.lower(),
                )
            )
        except AttributeError:
            raise ex.UnsupportedFunctionException(node.func.name)

        return handler(*node.args)

    ####################################################################################
    # String Functions
    ####################################################################################

    def _substr_function(
        self, field: ast._Node, substr: ast._Node, func: str
    ) -> ClauseElement:
        identifier = self.visit(field)
        substring = self.visit(substr)
        op = getattr(identifier, func)

        return op(substring)

    def func_substringof(
        self, substr: ast._Node, field: ast._Node
    ) -> ClauseElement:
        if isinstance(substr, ast.Identifier) and substr.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return self.visit(field).contains(identifier)
        if isinstance(field, ast.Identifier) and field.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return identifier.contains(self.visit(substr))
        # conformance: req/request-data/built-in-query-functions — substringof
        # OData/STA Table 23 semantics: substringof(p0, p1) means "p1 contains
        # p0", i.e. the SECOND argument (field/haystack) must contain the FIRST
        # (substr/needle). This emits p1 LIKE '%' || p0 || '%' through the same
        # .contains() machinery used by startswith/endswith. The previous code
        # swapped the operands (built "needle LIKE %haystack%"), so a valid
        # substring matched only on equality and otherwise returned [].
        return self._substr_function(field, substr, "contains")

    def func_endswith(
        self, field: ast._Node, substr: ast._Node
    ) -> ClauseElement:
        if isinstance(field, ast.Identifier) and field.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return identifier.endswith(self.visit(substr))
        if isinstance(substr, ast.Identifier) and substr.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return self.visit(field).endswith(identifier)
        if isinstance(field, ast.Identifier):
            return self._substr_function(field, substr, "endswith")
        if isinstance(substr, ast.Identifier):
            return self._substr_function(substr, field, "endswith")

    def func_startswith(
        self, field: ast._Node, substr: ast._Node
    ) -> ClauseElement:
        if isinstance(field, ast.Identifier) and field.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return identifier.startswith(self.visit(substr))
        if isinstance(substr, ast.Identifier) and substr.name == "result":
            identifier = getattr(globals()[self.root_model], "result_string")
            return self.visit(field).startswith(identifier)
        if isinstance(field, ast.Identifier):
            return self._substr_function(field, substr, "startswith")
        if isinstance(substr, ast.Identifier):
            return self._substr_function(substr, field, "startswith")

    def func_length(self, arg: ast._Node) -> functions.Function:
        if isinstance(arg, ast.Identifier) and arg.name == "result":
            return functions.char_length(
                getattr(globals()[self.root_model], "result_string")
            )
        return functions.char_length(self.visit(arg))

    def func_indexof(
        self, first: ast._Node, second: ast._Node
    ) -> functions.Function:
        if isinstance(first, ast.Identifier) and first.name == "result":
            return functions.func.strpos(
                getattr(globals()[self.root_model], "result_string"),
                self.visit(second),
            )
        if isinstance(second, ast.Identifier) and second.name == "result":
            return functions.func.strpos(
                self.visit(first),
                getattr(globals()[self.root_model], "result_string"),
            )
        return functions.func.strpos(self.visit(first), self.visit(second))

    def func_substring(
        self,
        fullstr: ast._Node,
        index: ast._Node,
        nchars: Optional[ast._Node] = None,
    ) -> functions.Function:
        if nchars:
            if (
                isinstance(fullstr, ast.Identifier)
                and fullstr.name == "result"
            ):
                return functions.func.substr(
                    getattr(globals()[self.root_model], "result_string"),
                    self.visit(index) + 1,
                    self.visit(nchars),
                )
            return functions.func.substr(
                self.visit(fullstr),
                self.visit(index) + 1,
                self.visit(nchars),
            )
        else:
            if (
                isinstance(fullstr, ast.Identifier)
                and fullstr.name == "result"
            ):
                return functions.func.substr(
                    getattr(globals()[self.root_model], "result_string"),
                    self.visit(index) + 1,
                )
            return functions.func.substr(
                self.visit(fullstr), self.visit(index) + 1
            )

    def func_tolower(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.lower(
                getattr(globals()[self.root_model], "result_string")
            )
        return functions.func.lower(self.visit(field))

    def func_toupper(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.upper(
                getattr(globals()[self.root_model], "result_string")
            )
        return functions.func.upper(self.visit(field))

    def func_trim(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.btrim(
                getattr(globals()[self.root_model], "result_string")
            )
        return functions.func.btrim(self.visit(field))

    def func_concat(self, *args: ast._Node) -> functions.Function:
        return functions.concat(
            *[
                (
                    getattr(globals()[self.root_model], "result_string")
                    if isinstance(arg, ast.Identifier) and arg.name == "result"
                    else self.visit(arg)
                )
                for arg in args
            ]
        )

    ####################################################################################
    # Date Functions
    ####################################################################################

    def func_year(self, field: ast._Node) -> functions.Function:
        return extract("year", self.visit(field))

    def func_month(self, field: ast._Node) -> functions.Function:
        return extract("month", self.visit(field))

    def func_day(self, field: ast._Node) -> functions.Function:
        return extract("day", self.visit(field))

    def func_hour(self, field: ast._Node) -> functions.Function:
        return extract("hour", self.visit(field))

    def func_minute(self, field: ast._Node) -> functions.Function:
        return extract("minute", self.visit(field))

    def func_second(self, field: ast._Node) -> functions.Function:
        return extract("second", self.visit(field))

    def func_fractionalseconds(self, field: ast._Node) -> functions.Function:
        return extract("microsecond", self.visit(field)) / 1000000.0

    def func_totaloffsetminutes(self, field: ast._Node) -> functions.Function:
        return extract("timezone_hour", self.visit(field)) * 60 + extract(
            "timezone_minute", self.visit(field)
        )

    def func_date(self, field: ast._Node) -> ClauseElement:
        return cast(self.visit(field), Date)

    def func_time(self, field: ast._Node) -> functions.Function:
        return cast(self.visit(field), Time)

    def func_now(self) -> functions.Function:
        return functions.now()

    def func_mindatetime(self):
        return "-infinity"

    def func_maxdatetime(self):
        return "infinity"

    ####################################################################################
    # Math Functions
    ####################################################################################

    def func_ceiling(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.ceil(
                getattr(globals()[self.root_model], "result_number")
            )
        return functions.func.ceil(self.visit(field))

    def func_floor(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.floor(
                getattr(globals()[self.root_model], "result_number")
            )
        return functions.func.floor(self.visit(field))

    def func_round(self, field: ast._Node) -> functions.Function:
        if isinstance(field, ast.Identifier) and field.name == "result":
            return functions.func.round(
                getattr(globals()[self.root_model], "result_number")
            )
        return functions.func.round(self.visit(field))

    ####################################################################################
    # Geospatial Functions
    ####################################################################################

    def func_geodistance(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Distance(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def _geo_argument(self, arg: ast._Node):
        """Resolve a geospatial-function argument to a column or geometry.

        conformance: req/request-data/built-in-query-functions — a geo.* function
        argument may be a geometry PROPERTY (ast.Identifier -> mapped column) or
        an inline geography LITERAL (ast.Geography -> WKT geometry). The literal
        form (e.g. geo.length(geography'LINESTRING(...)'), the Table 23 example)
        previously crashed with "'Geography' object has no attribute 'name'"
        because only the property form was handled.
        """
        if isinstance(arg, ast.Geography):
            return WKTElement(arg.val, srid=EPSG)
        if isinstance(arg, ast.Identifier):
            return getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(arg.name, arg.name),
            )
        return self.visit(arg)

    def func_geolength(self, field: ast._Node) -> functions.Function:
        # conformance: req/request-data/built-in-query-functions — geo.length
        # accepts either a geometry property or a geography literal argument.
        return functions.func.ST_Length(self._geo_argument(field))

    def func_geointersects(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Intersects(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_equals(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Equals(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_disjoint(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Disjoint(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_touches(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Touches(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_within(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Within(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_overlaps(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Overlaps(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_crosses(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Crosses(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_intersects(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Intersects(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_contains(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Contains(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
        )

    def func_st_relate(
        self,
        field: ast.Identifier,
        geography: ast.Geography,
        intersectionMatrixPattern: Optional[ast._Node] = None,
    ) -> functions.Function:
        return functions.func.ST_Relate(
            getattr(
                globals()[self.root_model],
                SELECT_MAPPING.get(field.name, field.name),
            ),
            WKTElement(geography.val, srid=EPSG),
            self.visit(intersectionMatrixPattern),
        )

    @staticmethod
    def handle_observation_result(entity, operator, value, left=False):
        result_conditions = []
        operator_name = f"__{operator.__name__}__"
        comparison_operators = {
            "__eq__",
            "__ne__",
            "__gt__",
            "__lt__",
            "__ge__",
            "__le__",
        }
        arithmetic_operators = {
            "__add__",
            "__sub__",
            "__mul__",
            "__truediv__",
            "__mod__",
        }

        entity_class = globals()[entity]

        def add_condition(attribute, value, left):
            """Helper to append conditions based on left flag."""
            condition = (
                getattr(attribute, operator_name)(value)
                if left
                else getattr(value, operator_name)(attribute)
            )
            result_conditions.append(condition)

        # Handle arithmetic operators
        if operator_name in arithmetic_operators:
            if operator_name == "__mod__":
                return cast(getattr(entity_class, "result_number"), Numeric)
            return getattr(entity_class, "result_number")

        # Handle comparison operators
        if operator_name in comparison_operators:
            if isinstance(value.type, String):
                attribute = getattr(entity_class, "result_string")
                add_condition(attribute, value, left)
                attribute = cast(getattr(entity_class, "result_number"), Text)
                add_condition(attribute, value, left)
            elif isinstance(value.type, (Integer, Float)):
                attribute = getattr(entity_class, "result_number")
                add_condition(attribute, value, left)
            elif isinstance(value.type, Boolean):
                attribute = getattr(entity_class, "result_boolean")
                add_condition(attribute, value, left)
            elif isinstance(value.type, JSON):
                attribute = functions.func.to_jsonb(
                    getattr(entity_class, "result_number")
                )
                add_condition(attribute, value, left)

            return result_conditions
