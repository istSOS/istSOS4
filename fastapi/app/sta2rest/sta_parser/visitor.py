"""
Visitor for the SensorThings API.

Author: Filippo Finke
"""
import ast as ast

class Visitor:
    """
    Visitor class for traversing the SensorThings API abstract syntax tree (AST).

    This class provides a visit method that can be used to traverse the AST.
    It dynamically calls visit methods based on the type of the node being visited.
    If no specific visit method is available for a particular node type, it falls back
    to the generic_visit method.

    Attributes:
        None

    Methods:
        visit(node): Traverse the AST by visiting each node in a depth-first manner.
        generic_visit(node): Default visit method called when no specific visit method is available for a node type.
    """
    def visit(self, node):
        """
        Traverse the AST by visiting each node in a depth-first manner.

        This method dynamically calls visit methods based on the type of the node being visited.

        Args:
            node: The current node being visited.

        Returns:
            The result of visiting the node.

        Raises:
            None
        """
        method_name = 'visit_' + type(node).__name__
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)
    
    def generic_visit(self, node):
        """
        Default visit method called when no specific visit method is available for a node type.

        This method is called for each field of the node and recursively visits any child nodes.

        Args:
            node: The current node being visited.

        Returns:
            None

        Raises:
            None
        """
        # get all the fields of the node
        for field_name, field_value in vars(node).items():
            # if the field is a list of nodes
            if isinstance(field_value, list):
                # visit all the nodes in the list
                for item in field_value:
                    if isinstance(item, ast.Node):
                        self.visit(item)
            # if the field is a node
            elif isinstance(field_value, ast.Node):
                # visit the node
                self.visit(field_value)
