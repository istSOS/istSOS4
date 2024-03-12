"""
Module: STA2REST filter visitor

Author: Filippo Finke

This module provides a visitor for the filter AST.
"""
from odata_query import ast, visitor
from sqlalchemy import all_, any_, and_, or_, not_, text
from ..models.models import Location, Thing, HistoricalLocation, ObservedProperty, Sensor, Datastream, Observation, FeaturesOfInterest

class FilterVisitor(visitor.NodeVisitor):
   """
      Visitor for the filter AST.  
   """

   def visit_All(self, node: ast.All) -> str:
      return all_

   def visit_Any(self, node: ast.Any) -> str:
      return any_
         
   def visit_And(self, node: ast.And) -> str:
      return and_

   def visit_Or(self, node: ast.Or) -> str:
      return or_
      
   def visit_Not(self, node: ast.Not) -> str:
      return not_
   
   def visit_Attribute(self, node: ast.Attribute) -> str:
      return node
   
   def visit_BinOp(self, node: ast.BinOp) -> str:
      return node
   
   def visit_BoolOp(self, node: ast.BoolOp) -> str:
      operator = self.visit(node.op)
      left = self.visit(node.left)
      right = self.visit(node.right)
      return operator, left, right
   
   def visit_Compare(self, node: ast.Compare) -> str:
      left = super().visit(node.left)
      comparator = super().visit(node.comparator)
      right = super().visit(node.right)

      # Check if the left is an attribute
      if isinstance(left, (ast.Attribute)):
         owner = left.owner.name
         attr = left.attr
         left = f"{owner}.{attr}"
      # Otherwise it is an identifier
      elif isinstance(left, (ast.Identifier)):
         left = left.name

      # Check if the right is an attribute
      if isinstance(right, (ast.Identifier)):
         right = right.name
         
      return f"{left}{comparator}{right}"
   
   def visit_DateTime(self, node: ast.DateTime) -> str:
      return node.val

   def visit_Float(self, node: ast.Float) -> str:
      return node.val
      
   def visit_Eq(self, node: ast.Eq) -> str:
      return "__eq__"

   def visit_NotEq(self, node: ast.NotEq) -> str:
      return "__ne__"
   
   def visit_Gt(self, node: ast.Gt) -> str:
      return "__gt__"

   def visit_Lt(self, node: ast.Lt) -> str:
      return "__lt__"

   def visit_GtE(self, node: ast.GtE) -> str:
      return "__ge__"

   def visit_LtE(self, node: ast.LtE) -> str:
      return "__le__"
      
   def visit_In(self, node: ast.In) -> str:
      return "in_"
   
   def visit_Integer(self, node: ast.Integer) -> str:
      return node.val
   
   def visit_String(self, node: ast.String) -> str:
      return node.val

   def visit_Identifier(self, node: ast.Identifier) -> str:
      return node