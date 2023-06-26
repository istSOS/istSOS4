from odata_query import ast, visitor

class FilterVisitor(visitor.NodeVisitor):
     
     def visit_Add(self, node: ast.Add) -> str:
        print("Add")
        print(node)
        return node
     
     def visit_All(self, node: ast.All) -> str:
        return "all"
     
     def visit_And(self, node: ast.And) -> str:
        return "&"
     
     def visit_Any(self, node: ast.Any) -> str:
        return "any"
     
     def visit_Attribute(self, node: ast.Attribute) -> str:
        print("Attribute")
        print(node)
        return node
     
     def visit_BinOp(self, node: ast.BinOp) -> str:
        print("BinOp")
        print(node)
        return node
     
     def visit_BoolOp(self, node: ast.BoolOp) -> str:
        operator = self.visit(node.op)
        left = self.visit(node.left)
        right = self.visit(node.right)

        if(isinstance(node.op, ast.And)):
           return f"{left}&{right}"
        else:
         left = left.replace("=", ".")
         right = right.replace("=", ".")
         return f"{operator}=({left},{right})"
     
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
        
        print(node.left, node.comparator, node.right)

        left = super().visit(node.left)
        comparator = super().visit(node.comparator)
        right = super().visit(node.right)

        if isinstance(left, (ast.Attribute)):
           owner = left.owner.name
           attr = left.attr
           left = f"{owner}->>{attr}"
        elif isinstance(left, (ast.Identifier)):
            left = left.name
         
        if isinstance(right, (ast.Identifier)):
            right = right.name

        return f"{left}={comparator}.{right}"
     
     def visit_Date(self, node: ast.Date) -> str:
        print("Date")
        print(node)
        return node
     
     def visit_DateTime(self, node: ast.DateTime) -> str:
        return node.val
     
     def visit_Div(self, node: ast.Div) -> str:
        print("Div")
        print(node)
        return node
     
     def visit_Duration(self, node: ast.Duration) -> str:
        print("Duration")
        print(node)
        return node
     
     def visit_Eq(self, node: ast.Eq) -> str:
        return "eq"
     
     def visit_Float(self, node: ast.Float) -> str:
        return node.val
     
     def visit_GUID(self, node: ast.GUID) -> str:
        print("GUID")
        print(node)
        return node
     
     def visit_Gt(self, node: ast.Gt) -> str:
        return "gt"
     
     def visit_GtE(self, node: ast.GtE) -> str:
        return "gte"
     
     def visit_In(self, node: ast.In) -> str:
        return "in"
     
     def visit_Integer(self, node: ast.Integer) -> str:
        return node.val
     
     def visit_Lambda(self, node: ast.Lambda) -> str:
        print("Lambda")
        print(node)
        return node
     
     def visit_List(self, node: ast.List) -> str:
        print("List")
        print(node)
        return node
     
     def visit_Lt(self, node: ast.Lt) -> str:
        return "lt"
     
     def visit_LtE(self, node: ast.LtE) -> str:
        return "lte"
     
     def visit_Not(self, node: ast.Not) -> str:
        return "not"
     
     def visit_NotEq(self, node: ast.NotEq) -> str:
        return "neq"
     
     def visit_Null(self, node: ast.Null) -> str:
        print("Null")
        print(node)
        return node
     
     def visit_Or(self, node: ast.Or) -> str:
        return "or"
     
     def visit_String(self, node: ast.String) -> str:
        return node.val
     
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
        return node