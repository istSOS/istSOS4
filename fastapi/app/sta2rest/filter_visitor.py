"""
Module: STA2REST filter visitor

Author: Filippo Finke

This module provides a visitor for the filter AST.
"""

import operator
import urllib.parse
from typing import Any, Callable, List, Optional, Union

from geoalchemy2 import WKTElement
from odata_query import ast
from odata_query import exceptions as ex
from odata_query import visitor
from sqlalchemy import JSON, Float, Integer, String, Text, cast
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql import functions
from sqlalchemy.sql.expression import (
    BinaryExpression,
    BindParameter,
    BooleanClauseList,
    ClauseElement,
    ColumnClause,
    False_,
    True_,
    all_,
    and_,
    any_,
    cast,
    extract,
    false,
    literal,
    or_,
    true,
)
from sqlalchemy.types import Date, Time

from ..models import *

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
}


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

    def visit_Integer(self, node: ast.Integer) -> BindParameter:
        return literal(node.py_val)

    def visit_Float(self, node: ast.Float) -> BindParameter:
        return literal(node.py_val)

    def visit_Boolean(self, node: ast.Boolean) -> Union[True_, False_]:
        if node.val == "true":
            return true()
        return false()

    def visit_String(self, node: ast.String) -> BindParameter:
        return literal(urllib.parse.unquote_plus(node.py_val))

    def visit_DateTime(self, node: ast.DateTime) -> BindParameter:
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
            if (
                self.root_model == "Observation"
                or self.root_model == "ObservationTravelTime"
            ) and node.name == "result":
                return or_(
                    getattr(globals()[self.root_model], "result_integer"),
                    getattr(globals()[self.root_model], "result_double"),
                    getattr(globals()[self.root_model], "result_string"),
                    getattr(globals()[self.root_model], "result_boolean"),
                    getattr(globals()[self.root_model], "result_json"),
                )
            name = node.name.lower() if node.name[0].isupper() else node.name
            for old_key, new_key in SELECT_MAPPING.items():
                if old_key == node.name:
                    name = new_key
            return getattr(globals()[self.root_model], name)
        except AttributeError:
            raise ex.InvalidFieldException(node.name)

    def visit_Attribute(self, node: ast.Attribute) -> ColumnClause:
        attributes = []
        owner = node.owner

        while not isinstance(owner, ast.Identifier):
            attributes.append(owner.attr)
            owner = owner.owner

        name = owner.name
        attributes = attributes[::-1]
        rel_attr = self.visit(owner)
        prop_inspect = inspect(rel_attr).property

        # Check if the property is a JSON column
        if isinstance(prop_inspect, ColumnProperty) and isinstance(
            rel_attr.type, JSON
        ):
            name = SELECT_MAPPING.get(name, name)
            table_attr = getattr(globals()[str(rel_attr.table.name)], name)

            for attribute in attributes:
                table_attr = table_attr.op("->")(attribute)
            table_attr = table_attr.op("->>")(node.attr)
            return table_attr
        elif not isinstance(prop_inspect, RelationshipProperty):
            raise ValueError(f"Not a relationship: {node.owner}")
        self.join_relationships.append(rel_attr)
        owner_cls = prop_inspect.entity.class_
        try:
            return getattr(owner_cls, node.attr)
        except AttributeError:
            raise ex.InvalidFieldException(node.attr)

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.op)
        return op(left, right)

    def visit_BoolOp(self, node: ast.BoolOp) -> BooleanClauseList:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.op)
        return op(left, right)

    def visit_Compare(self, node: ast.Compare) -> BinaryExpression:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.comparator)

        if isinstance(left, InstrumentedAttribute):
            if (
                "system_time_validity" in left.key
                and f"__{getattr(op, '__name__')}__" == "__eq__"
            ):
                if isinstance(right, list):
                    right = functions.func.tstzrange(
                        functions.func.timestamptz(right[0]),
                        functions.func.timestamptz(right[1]),
                    )
                    return left.op("&&")(right)
                return left.op("@>")(functions.func.timestamptz(right))
        else:
            if isinstance(left, BooleanClauseList):
                left = FilterVisitor.handle_observation_result(
                    self.root_model, left, op, right
                )
                return or_(*left)
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
        return self._substr_function(field, substr, "contains")

    def func_endswith(
        self, field: ast._Node, substr: ast._Node
    ) -> ClauseElement:
        return self._substr_function(field, substr, "endswith")

    def func_startswith(
        self, field: ast._Node, substr: ast._Node
    ) -> ClauseElement:
        return self._substr_function(field, substr, "startswith")

    def func_length(self, arg: ast._Node) -> functions.Function:
        return functions.char_length(self.visit(arg))

    def func_indexof(
        self, first: ast._Node, second: ast._Node
    ) -> functions.Function:
        return functions.func.strpos(self.visit(first), self.visit(second))

    def func_substring(
        self,
        fullstr: ast._Node,
        index: ast._Node,
        nchars: Optional[ast._Node] = None,
    ) -> functions.Function:
        if nchars:
            return functions.func.substr(
                self.visit(fullstr),
                int(self.visit(index)) + 1,
                self.visit(nchars),
            )
        else:
            return functions.func.substr(
                self.visit(fullstr), int(self.visit(index)) + 1
            )

    def func_tolower(self, field: ast._Node) -> functions.Function:
        return functions.func.lower(self.visit(field))

    def func_toupper(self, field: ast._Node) -> functions.Function:
        return functions.func.upper(self.visit(field))

    def func_trim(self, field: ast._Node) -> functions.Function:
        return functions.func.ltrim(functions.func.rtrim(self.visit(field)))

    def func_concat(self, *args: ast._Node) -> functions.Function:
        return functions.concat(*[self.visit(arg) for arg in args])

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

    # TODO
    # def func_mindatetime(self, field: ast._Node) -> functions.Function:
    #    return functions.mindatetime(field)

    # def func_maxdatetime(self) -> functions.Function:
    #    return functions.maxdatetime()

    ####################################################################################
    # Math Functions
    ####################################################################################

    def func_ceiling(self, field: ast._Node) -> functions.Function:
        return functions.func.ceil(self.visit(field))

    def func_floor(self, field: ast._Node) -> functions.Function:
        return functions.func.floor(self.visit(field))

    def func_round(self, field: ast._Node) -> functions.Function:
        return functions.func.round(self.visit(field))

    ####################################################################################
    # Geospatial Functions
    ####################################################################################

    def func_geodistance(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Distance(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_geolength(self, field: ast.Identifier) -> functions.Function:
        return functions.func.ST_Length(
            getattr(globals()[self.root_model], field.name)
        )

    def func_geointersects(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Intersects(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_equals(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Equals(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_disjoint(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Disjoint(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_touches(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Touches(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_within(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Within(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_overlaps(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Overlaps(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_crosses(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Crosses(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_intersects(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Intersects(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_contains(
        self, field: ast.Identifier, geography: ast.Geography
    ) -> functions.Function:
        return functions.func.ST_Contains(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
        )

    def func_st_relate(
        self,
        field: ast.Identifier,
        geography: ast.Geography,
        intersectionMatrixPattern: Optional[ast._Node] = None,
    ) -> functions.Function:
        return functions.func.ST_Relate(
            getattr(globals()[self.root_model], field.name),
            WKTElement(geography.val, srid=4326),
            self.visit(intersectionMatrixPattern),
        )

    @staticmethod
    def handle_observation_result(entity, list, operator, value):
        result_conditions = []
        operator_name = f"__{operator.__name__}__"
        valid_operators = {
            "__eq__",
            "__ne__",
            "__gt__",
            "__lt__",
            "__ge__",
            "__le__",
        }

        if operator_name in valid_operators:
            try:
                value_type = value.type
                entity_class = globals()[entity]
                if isinstance(value_type, String):
                    filter_query = getattr(entity_class, "result_string")
                    result_conditions.append(
                        getattr(filter_query, operator_name)(value)
                    )
                    for result_type in ["result_integer", "result_double"]:
                        filter_query = cast(
                            getattr(globals()[entity], result_type), Text
                        )
                        result_conditions.append(
                            getattr(filter_query, operator_name)(value)
                        )
                elif isinstance(value_type, (Integer, Float)):
                    filter_query = getattr(entity_class, "result_integer")
                    result_conditions.append(
                        getattr(filter_query, operator_name)(value)
                    )
                    filter_query = getattr(entity_class, "result_double")
                    result_conditions.append(
                        getattr(filter_query, operator_name)(value)
                    )
                elif value == "true" or value == "false":
                    bool_value = value == "true"
                    filter_query = getattr(entity_class, "result_boolean")
                    result_conditions.append(
                        getattr(filter_query, operator_name)(bool_value)
                    )
            except TypeError:
                pass

        return result_conditions
