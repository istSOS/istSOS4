"""
Module: STA2REST filter visitor

Author: Filippo Finke

This module provides a visitor for the filter AST.
"""
from odata_query import ast, visitor, typing, exceptions as ex
from sqlalchemy.sql.expression import all_, any_, and_, or_, text
from sqlalchemy.sql import functions
from typing import Type
from sqlalchemy.orm.decl_api import DeclarativeMeta
from ..models import (
   Location,
   Thing,
   HistoricalLocation,
   ObservedProperty,
   Sensor,
   Datastream,
   FeaturesOfInterest,
   Observation,
   LocationTravelTime,
   ThingTravelTime,
   HistoricalLocationTravelTime,
   ObservedPropertyTravelTime,
   SensorTravelTime,
   DatastreamTravelTime,
   FeaturesOfInterestTravelTime,
   ObservationTravelTime
)
import operator
from typing import Any, Callable, Optional, Union
from sqlalchemy.sql.expression import (
   BinaryExpression,
   BindParameter,
   BooleanClauseList,
   ClauseElement,
   ColumnClause,
   False_,
   Null,
   True_,
   and_,
   cast,
   extract,
   false,
   literal,
   null,
   or_,
   true
)
from sqlalchemy.orm.attributes import InstrumentedAttribute
from geoalchemy2 import WKTElement
from datetime import datetime, timezone
class FilterVisitor(visitor.NodeVisitor):
   """
      Visitor for the filter AST.  
   """

   def __init__(self, root_model: str):
      self.root_model = root_model

   def visit_All(self, node: ast.All) -> str:
      return all_

   def visit_Any(self, node: ast.Any) -> str:
      return any_
   
   def visit_Integer(self, node: ast.Integer) -> BindParameter:
      return node.val
   
   def visit_Float(self, node: ast.Float) -> BindParameter:
      return node.val

   def visit_Boolean(self, node: ast.Boolean) -> Union[True_, False_]:
      if node.val == "true":
         return true()
      else:
         return false()

   def visit_String(self, node: ast.String) -> BindParameter:
      return node.val

   def visit_DateTime(self, node: ast.DateTime)-> BindParameter:
      return node.val

   ####################################################################################
   # Comparison Operators
   ####################################################################################

   def visit_Eq(self, node: ast.Eq) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.eq

   def visit_NotEq(self, node: ast.NotEq) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.ne
   
   def visit_Gt(self, node: ast.Gt) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.gt

   def visit_Lt(self, node: ast.Lt) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.lt

   def visit_GtE(self, node: ast.GtE) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.ge

   def visit_LtE(self, node: ast.LtE) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return operator.le
   
   ####################################################################################
   # Logical Operators
   ####################################################################################

   def visit_And(self, node: ast.And) -> Callable[[ClauseElement, ClauseElement], BooleanClauseList]:
      return and_

   def visit_Or(self, node: ast.Or) -> Callable[[ClauseElement, ClauseElement], BooleanClauseList]:
      return or_
      
   def visit_Not(self, node: ast.Not) -> Callable[[ClauseElement], ClauseElement]:
      return operator.invert

   def visit_In(self, node: ast.In) -> Callable[[ClauseElement, ClauseElement], BinaryExpression]:
      return lambda a, b: a.in_(b)

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

   def visit_Identifier(self, node: ast.Identifier) -> ColumnClause | BooleanClauseList:
      try:
         if (self.root_model == 'Observation' or self.root_model == 'ObservationTravelTime') and node.name == 'result':
            return or_(
               getattr(globals()[self.root_model], 'result_integer'),
               getattr(globals()[self.root_model], 'result_double'),
               getattr(globals()[self.root_model], 'result_string'),
               getattr(globals()[self.root_model], 'result_boolean'),
               getattr(globals()[self.root_model], 'result_json')
            )
         return getattr(globals()[self.root_model], node.name)
      except AttributeError:
         raise ex.InvalidFieldException(node.name)

   def visit_Attribute(self, node: ast.Attribute) -> str:
      return node
   
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
      left = super().visit(node.left)
      right = super().visit(node.right)
      op = super().visit(node.comparator)

      # Check if the right is an attribute
      if isinstance(right, (ast.Identifier)):
         right = right.name

      if isinstance(left, InstrumentedAttribute):
         if "system_time_validity" in left.key and f"__{getattr(op, '__name__')}__" == "__eq__":
            right = datetime.fromisoformat(right.rstrip("Z")).replace(tzinfo=timezone.utc)
            return left.op('@>')(functions.func.timestamptz(right))
      # Check if the left is an attribute
      elif isinstance(left, (ast.Attribute)):
         owner = left.owner.name
         attr = left.attr
         left = f"{owner}.{attr}"
      # Otherwise it is an identifier
      elif isinstance(left, (ast.Identifier)):
         left = left.name
      # Otherwise it is a function
      else:
         if isinstance(left, BooleanClauseList):
            left = FilterVisitor.handle_observation_result(self.root_model, left, op, right)
            return or_(*left)
      return op(left, right)

   def visit_Call(self, node: ast.Call) -> ClauseElement:
      try:
         print(node.func.name)
         handler = getattr(self, "func_" + node.func.name.lower()) if len(node.func.namespace) == 0 else getattr(self, "func_" + node.func.namespace[0] + node.func.name.lower())
      except AttributeError:
         raise ex.UnsupportedFunctionException(node.func.name)

      return handler(*node.args)

   ####################################################################################
   # String Functions
   ####################################################################################

   def _substr_function(self, field: ast._Node, substr: ast._Node, func: str) -> ClauseElement:
      typing.typecheck(field, (ast.Identifier, ast.String), "field")
      typing.typecheck(substr, ast.String, "substring")

      identifier = self.visit(field)
      substring = self.visit(substr)
      op = getattr(identifier, func)

      return op(substring)

   def func_contains(self, field: ast._Node, substr: ast._Node) -> ClauseElement:
      return self._substr_function(field, substr, "contains")

   def func_endswith(self, field: ast._Node, substr: ast._Node) -> ClauseElement:
      return self._substr_function(field, substr, "endswith")

   def func_startswith(self, field: ast._Node, substr: ast._Node) -> ClauseElement:
      return self._substr_function(field, substr, "startswith")

   def func_length(self, arg: ast._Node) -> functions.Function:
      return functions.char_length(self.visit(arg))

   def func_indexof(self, first: ast._Node, second: ast._Node) -> functions.Function:
      return functions.func.strpos(self.visit(first), self.visit(second))

   def func_substring(self, fullstr: ast._Node, index: ast._Node, nchars: Optional[ast._Node] = None) -> functions.Function:
      if nchars:
         return functions.func.substr(
               self.visit(fullstr),
               int(self.visit(index)) + 1,
               self.visit(nchars),
         )
      else:
         return functions.func.substr(self.visit(fullstr), int(self.visit(index)) + 1)
   
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
      return extract("timezone_hour", self.visit(field)) * 60 + extract("timezone_minute", self.visit(field))

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

   def func_geodistance(self, field: ast.Identifier, geography: ast.Geography) -> functions.Function:
      return functions.func.ST_Distance(getattr(globals()[self.root_model], field.name), WKTElement(geography.val, srid=4326))

   def func_geolength(self, field: ast.Identifier) -> functions.Function:
      return functions.func.ST_Length(getattr(globals()[self.root_model], field.name))

   def func_geointersects(self, field: ast.Identifier, geography: ast.Geography) -> functions.Function:
      return functions.func.ST_Intersects(getattr(globals()[self.root_model], field.name), WKTElement(geography.val, srid=4326))

   @staticmethod
   def handle_observation_result(entity, list, operator, value):
      value = str(value)
      result_conditions = []
      operator_name = f"__{getattr(operator, '__name__')}__"
      if operator_name in ['__eq__', '__ne__', '__gt__', '__lt__', '__ge__', '__le__']:
         try:
            float_value = float(str(value))
            for result_type in ['result_integer', 'result_double']:
               filter_query = getattr(globals()[entity], result_type)
               result_conditions.append(getattr(filter_query, operator_name)(float_value))
         except ValueError:
            pass
      if operator_name in ['__eq__', '__ne__']:
         if value == "true" or value == "false":
            bool_value = value == "true"
            filter_query = getattr(globals()[entity], 'result_boolean')
            result_conditions.append(getattr(filter_query, operator_name)(bool_value))
         else:
            try:
               filter_query = getattr(globals()[entity], 'result_string')
               result_conditions.append(getattr(filter_query, operator_name)(value))
            except TypeError:
               pass
      return result_conditions

