"""
AST for the SensorThings API.

Author: Filippo Finke
"""

class PrettyPrinter(object):
    """
    A class used to pretty print objects.

    Methods:
    __str__(): Returns a string representation of the object.
    """

    def __str__(self):
        """
        Returns a string representation of the object.

        Returns:
        str: The string representation of the object.
        """
        lines = []
        for key, val in vars(self).items():
            if val is None:
                continue

            if isinstance(val, list):
                lines += ['\n  {}:'.format(key)]
                for item in val:
                    lines += ['   {}'.format(line) for line in str(item).split('\n')]
            else:
                lines += '{}: {}'.format(key, val).split('\n')
        return '\n'.join(lines)


class Node(PrettyPrinter):
    """
    A base class for nodes in the AST.

    Inherits from PrettyPrinter.

    Attributes:
    None
    """


class IdentifierNode(Node):
    """
    A class representing an identifier node.

    Inherits from Node.

    Attributes:
    name (str): The name of the identifier.
    """

    def __init__(self, name):
        """
        Initializes an IdentifierNode object.

        Args:
        name (str): The name of the identifier.
        """
        self.name = name


class SelectNode(Node):
    """
    A class representing a select node.

    Inherits from Node.

    Attributes:
    identifiers (list): A list of identifier nodes.
    """

    def __init__(self, identifiers):
        """
        Initializes a SelectNode object.

        Args:
        identifiers (list): A list of identifier nodes.
        """
        self.identifiers = identifiers


class FilterNode(Node):
    """
    A class representing a filter node.

    Inherits from Node.

    Attributes:
    filter (str): The filter string.
    """

    def __init__(self, filter):
        """
        Initializes a FilterNode object.

        Args:
        filter (str): The filter string.
        """
        self.filter = filter


class ExpandNodeIdentifier(Node):
    """
    A class representing an expand node with an identifier.

    Inherits from Node.

    Attributes:
    identifier (str): The identifier string.
    subquery (QueryNode): The subquery associated with the expand node.
    """

    def __init__(self, identifier, subquery=None):
        """
        Initializes an ExpandNodeIdentifier object.

        Args:
        identifier (str): The identifier string.
        subquery (QueryNode, optional): The subquery associated with the expand node.
        """
        self.identifier = identifier
        self.subquery = subquery


class ExpandNode(Node):
    """
    A class representing an expand node.

    Inherits from Node.

    Attributes:
    identifiers (list): A list of identifier nodes.
    """

    def __init__(self, identifiers):
        """
        Initializes an ExpandNode object.

        Args:
        identifiers (list): A list of identifier nodes.
        """
        self.identifiers = identifiers


class OrderByNodeIdentifier(Node):
    """
    A class representing an order by node with an identifier.

    Inherits from Node.

    Attributes:
    identifier (str): The identifier string.
    order (str): The order string.
    """

    def __init__(self, identifier, order):
        """
        Initializes an OrderByNodeIdentifier object.

        Args:
        identifier (str): The identifier string.
        order (str): The order string.
        """
        self.identifier = identifier
        self.order = order


class OrderByNode(Node):
    """
    A class representing an order by node.

    Inherits from Node.

    Attributes:
    identifiers (list): A list of identifier nodes.
    """

    def __init__(self, identifiers):
        """
        Initializes an OrderByNode object.

        Args:
        identifiers (list): A list of identifier nodes.
        """
        self.identifiers = identifiers


class SkipNode(Node):
    """
    A class representing a skip node.

    Inherits from Node.

    Attributes:
    count (int): The count value.
    """

    def __init__(self, count):
        """
        Initializes a SkipNode object.

        Args:
        count (int): The count value.
        """
        self.count = count


class TopNode(Node):
    """
    A class representing a top node.

    Inherits from Node.

    Attributes:
    count (int): The count value.
    """

    def __init__(self, count):
        """
        Initializes a TopNode object.

        Args:
        count (int): The count value.
        """
        self.count = count


class CountNode(Node):
    """
    A class representing a count node.

    Inherits from Node.

    Attributes:
    value (str): The value string.
    """

    def __init__(self, value):
        """
        Initializes a CountNode object.

        Args:
        value (str): The value string.
        """
        self.value = value


class QueryNode(Node):
    """
    A class representing a query node.

    Inherits from Node.

    Attributes:
    select (SelectNode, optional): The select node.
    filter (FilterNode, optional): The filter node.
    expand (ExpandNode, optional): The expand node.
    orderby (OrderByNode, optional): The order by node.
    skip (SkipNode, optional): The skip node.
    top (TopNode, optional): The top node.
    count (CountNode, optional): The count node.
    is_subquery (bool): Indicates if the query is a subquery.
    """

    def __init__(self, select=None, filter=None, expand=None, orderby=None, skip=None, top=None, count=None,
                 is_subquery=False):
        """
        Initializes a QueryNode object.

        Args:
        select (SelectNode, optional): The select node.
        filter (FilterNode, optional): The filter node.
        expand (ExpandNode, optional): The expand node.
        orderby (OrderByNode, optional): The order by node.
        skip (SkipNode, optional): The skip node.
        top (TopNode, optional): The top node.
        count (CountNode, optional): The count node.
        is_subquery (bool): Indicates if the query is a subquery.
        """
        self.select = select
        self.filter = filter
        self.expand = expand
        self.orderby = orderby
        self.skip = skip
        self.top = top
        self.count = count
        self.is_subquery = is_subquery
