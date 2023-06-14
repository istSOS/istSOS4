from odata_query import ast, exceptions, typing, visitor

class FilterVisitor(visitor.NodeVisitor):
     
     def visit_Add(self, node: ast.Add) -> str:
        print(node)
        return node
     
     def visit_All(self, node: ast.All) -> str:
        print(node)
        return node
     
     def visit_And(self, node: ast.And) -> str:
        print(node)
        return node
     
     def visit_Any(self, node: ast.Any) -> str:
        print(node)
        return node
     
     def visit_Attribute(self, node: ast.Attribute) -> str:
        print(node)
        return node
     
     def visit_BinOp(self, node: ast.BinOp) -> str:
        print(node)
        return node
     
     def visit_BoolOp(self, node: ast.BoolOp) -> str:
        print(node)
        return node
     
     def visit_Boolean(self, node: ast.Boolean) -> str:
        print(node)
        return node
     
     def visit_Call(self, node: ast.Call) -> str:
        print(node)
        return node
     
     def visit_CollectionLambda(self, node: ast.CollectionLambda) -> str:
        print(node)
        return node
     
     def visit_Compare(self, node: ast.Compare) -> str:
        print(node)
        return node
     
     def visit_Date(self, node: ast.Date) -> str:
        print(node)
        return node
     
     def visit_DateTime(self, node: ast.DateTime) -> str:
        print(node)
        return node
     
     def visit_Div(self, node: ast.Div) -> str:
        print(node)
        return node
     
     def visit_Duration(self, node: ast.Duration) -> str:
        print(node)
        return node
     
     def visit_Eq(self, node: ast.Eq) -> str:
        print(node)
        return node
     
     def visit_Float(self, node: ast.Float) -> str:
        print(node)
        return node
     
     def visit_GUID(self, node: ast.GUID) -> str:
        print(node)
        return node
     
     def visit_Gt(self, node: ast.Gt) -> str:
        print(node)
        return node
     
     def visit_GtE(self, node: ast.GtE) -> str:
        print(node)
        return node
     
     def visit_In(self, node: ast.In) -> str:
        print(node)
        return node
     
     def visit_Integer(self, node: ast.Integer) -> str:
        print(node)
        return node
     
     def visit_Lambda(self, node: ast.Lambda) -> str:
        print(node)
        return node
     
     def visit_List(self, node: ast.List) -> str:
        print(node)
        return node
     
     def visit_Lt(self, node: ast.Lt) -> str:
        print(node)
        return node
     
     def visit_LtE(self, node: ast.LtE) -> str:
        print(node)
        return node
     
     def visit_Mod(self, node: ast.Mod) -> str:
        print(node)
        return node
     
     def visit_Mult(self, node: ast.Mult) -> str:
        print(node)
        return node
     
     def visit_Not(self, node: ast.Not) -> str:
        print(node)
        return node
     
     def visit_NotEq(self, node: ast.NotEq) -> str:
        print(node)
        return node
     
     def visit_Null(self, node: ast.Null) -> str:
        print(node)
        return node
     
     def visit_Or(self, node: ast.Or) -> str:
        print(node)
        return node
     
     def visit_String(self, node: ast.String) -> str:
        print(node)
        return node
     
     def visit_Sub(self, node: ast.Sub) -> str:
        print(node)
        return node
     
     def visit_Time(self, node: ast.Time) -> str:
        print(node)
        return node
     
     def visit_USub(self, node: ast.USub) -> str:
        print(node)
        return node
     
     def visit_UnaryOp(self, node: ast.UnaryOp) -> str:
        print(node)
        return node

     def visit_Identifier(self, node: ast.Identifier) -> str:
        print(node)
        return node