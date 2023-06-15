from odata_query import ast, visitor

class FilterVisitor(visitor.NodeVisitor):
     
     def visit_Add(self, node: ast.Add) -> str:
        print("Add")
        print(node)
        return node
     
     def visit_All(self, node: ast.All) -> str:
        print("All")
        print(node)
        return node
     
     def visit_And(self, node: ast.And) -> str:
        print("And")
        print(node)
        return node
     
     def visit_Any(self, node: ast.Any) -> str:
        print("Any")
        print(node)
        return node
     
     def visit_Attribute(self, node: ast.Attribute) -> str:
        print("Attribute")
        print(node)
        return node
     
     def visit_BinOp(self, node: ast.BinOp) -> str:
        print("BinOp")
        print(node)
        return node
     
     def visit_BoolOp(self, node: ast.BoolOp) -> str:
        print("BoolOp")
        print(node)
        return node
     
     def visit_Boolean(self, node: ast.Boolean) -> str:
        print("Boolean")
        print(node)
        return node
     
     def visit_Call(self, node: ast.Call) -> str:
        print("Call")
        print(node)
        return node
     
     def visit_CollectionLambda(self, node: ast.CollectionLambda) -> str:
        print("CollectionLambda")
        print(node)
        return node
     
     def visit_Compare(self, node: ast.Compare) -> str:
        print("Compare")
        print(node)
        return node
     
     def visit_Date(self, node: ast.Date) -> str:
        print("Date")
        print(node)
        return node
     
     def visit_DateTime(self, node: ast.DateTime) -> str:
        print("DateTime")
        print(node)
        return node
     
     def visit_Div(self, node: ast.Div) -> str:
        print("Div")
        print(node)
        return node
     
     def visit_Duration(self, node: ast.Duration) -> str:
        print("Duration")
        print(node)
        return node
     
     def visit_Eq(self, node: ast.Eq) -> str:
        print("Eq")
        print(node)
        return node
     
     def visit_Float(self, node: ast.Float) -> str:
        print("Float")
        print(node)
        return node
     
     def visit_GUID(self, node: ast.GUID) -> str:
        print("GUID")
        print(node)
        return node
     
     def visit_Gt(self, node: ast.Gt) -> str:
        print("Gt")
        print(node)
        return node
     
     def visit_GtE(self, node: ast.GtE) -> str:
        print("GtE")
        print(node)
        return node
     
     def visit_In(self, node: ast.In) -> str:
        print("In")
        print(node)
        return node
     
     def visit_Integer(self, node: ast.Integer) -> str:
        print("Integer")
        print(node)
        return node
     
     def visit_Lambda(self, node: ast.Lambda) -> str:
        print("Lambda")
        print(node)
        return node
     
     def visit_List(self, node: ast.List) -> str:
        print("List")
        print(node)
        return node
     
     def visit_Lt(self, node: ast.Lt) -> str:
        print("Lt")
        print(node)
        return node
     
     def visit_LtE(self, node: ast.LtE) -> str:
        print("LtE")
        print(node)
        return node
     
     def visit_Mod(self, node: ast.Mod) -> str:
        print("Mod")
        print(node)
        return node
     
     def visit_Mult(self, node: ast.Mult) -> str:
        print("Mult")
        print(node)
        return node
     
     def visit_Not(self, node: ast.Not) -> str:
        print("Not")
        print(node)
        return node
     
     def visit_NotEq(self, node: ast.NotEq) -> str:
        print("NotEq")
        print(node)
        return node
     
     def visit_Null(self, node: ast.Null) -> str:
        print("Null")
        print(node)
        return node
     
     def visit_Or(self, node: ast.Or) -> str:
        print("Or")
        print(node)
        return node
     
     def visit_String(self, node: ast.String) -> str:
        print("String")
        print(node)
        return node
     
     def visit_Sub(self, node: ast.Sub) -> str:
        print("Sub")
        print(node)
        return node
     
     def visit_Time(self, node: ast.Time) -> str:
        print("Time")
        print(node)
        return node
     
     def visit_USub(self, node: ast.USub) -> str:
        print("USub")
        print(node)
        return node
     
     def visit_UnaryOp(self, node: ast.UnaryOp) -> str:
        print("UnaryOp")
        print(node)
        return node

     def visit_Identifier(self, node: ast.Identifier) -> str:
        print("Identifier")
        print(node)
        return node