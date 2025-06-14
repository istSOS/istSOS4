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
Parser for the SensorThings API query language.

Author: Filippo Finke
"""

from . import ast
from .lexer import Lexer


class Parser:
    def __init__(self, tokens):
        """
        Initialize the Parser instance.

        Args:
            tokens (list): List of tokens generated by the lexer.
        """
        self.tokens = tokens
        self.current_token = None
        self.next_token()
        self.expand_identifiers = []
        self.identifiers = []
        self.expands = []

    def next_token(self):
        """
        Get the next token from the list of tokens.

        If there are no more tokens, set the current_token to None.
        """
        if self.tokens:
            self.current_token = self.tokens.pop(0)
        else:
            self.current_token = None

    def match(self, token_type):
        """
        Match the current token with the specified token type.

        If the current token matches, move to the next token.
        If the current token doesn't match, raise an exception.

        Args:
            token_type (str): The expected token type.

        Raises:
            Exception: If the current token doesn't match the expected type.
        """
        if self.current_token and self.current_token.type == token_type:
            self.next_token()
        else:
            raise Exception(
                f"Expected '{token_type}', but found '{self.current_token.type}' ('{self.current_token.value}')"
            )

    def check_token(self, token_type):
        """
        Check if the current token matches the specified token type.

        Args:
            token_type (str): The token type to check.

        Returns:
            bool: True if the current token matches the token type, False otherwise.
        """
        return self.current_token and self.current_token.type == token_type

    def parse_identifier_list(self):
        """
        Parse a list of identifiers.

        Returns:
            list: A list of ast.IdentifierNode objects representing the identifiers.
        """
        identifiers = []
        identifiers.append(ast.IdentifierNode(self.current_token.value))
        if self.check_token("EXPAND_IDENTIFIER"):
            self.match("EXPAND_IDENTIFIER")
        else:
            self.match("IDENTIFIER")
        while self.check_token("VALUE_SEPARATOR"):
            self.match("VALUE_SEPARATOR")
            identifiers.append(ast.IdentifierNode(self.current_token.value))
            if self.check_token("EXPAND_IDENTIFIER"):
                self.match("EXPAND_IDENTIFIER")
            else:
                self.match("IDENTIFIER")
        return identifiers

    def parse_filter(self, is_in_subquery=False):
        """
        Parse a filter expression.

        Args:
            is_in_subquery (bool, optional): Whether the filter is in a subquery. Defaults to False.

        Returns:
            ast.FilterNode: The parsed filter expression.
        """
        self.match("FILTER")
        filter = ""

        while (
            not self.check_token("OPTIONS_SEPARATOR")
            and not self.check_token("SUBQUERY_SEPARATOR")
            and self.current_token != None
            and not (is_in_subquery and self.check_token("RIGHT_PAREN"))
        ):
            filter += self.current_token.value
            self.next_token()

        return ast.FilterNode(filter)

    def parse_expand(self):
        """
        Parse an expand expression.

        Returns:
            ast.ExpandNode: The parsed expand expression.
        """
        dollar_expand = False
        if self.check_token("EXPAND"):
            self.match("EXPAND")
            dollar_expand = True
        if self.check_token("SEGMENT_SEPARATOR"):
            self.match("SEGMENT_SEPARATOR")
        identifiers = []
        while self.current_token.type != "OPTIONS_SEPARATOR":
            tmp = self.current_token.value
            identifier = ast.ExpandNodeIdentifier(self.current_token.value)
            self.match("EXPAND_IDENTIFIER")
            # Check if there is a subquery
            if self.check_token("LEFT_PAREN"):
                identifier.subquery = self.parse_subquery()
            elif self.check_token("SEGMENT_SEPARATOR"):
                self.expand_identifiers.append(tmp)
                identifier.subquery = self.parse_subquery()

            identifiers.append(identifier)
            if dollar_expand:
                if self.check_token("VALUE_SEPARATOR"):
                    self.match("VALUE_SEPARATOR")
                else:
                    break
            else:
                if self.check_token("VALUE_SEPARATOR"):
                    if self.tokens[0].value in self.expand_identifiers:
                        self.match("VALUE_SEPARATOR")
                        self.match("EXPAND_IDENTIFIER")
                        self.match("SEGMENT_SEPARATOR")
                    else:
                        self.expand_identifiers = []
                        break
                else:
                    break
        return ast.ExpandNode(identifiers)

    def parse_select(self):
        """
        Parse a select expression.

        Returns:
            ast.SelectNode: The parsed select expression.
        """
        self.match("SELECT")
        identifiers = self.parse_identifier_list()
        return ast.SelectNode(identifiers)

    def parse_orderby(self):
        """
        Parse an orderby expression.

        Returns:
            ast.OrderByNode: The parsed orderby expression.
        """
        self.match("ORDERBY")
        # match identifiers separated by commas and check if there is a space and order
        identifiers = []
        while True:
            identifier = self.current_token.value
            if self.check_token("EXPAND_IDENTIFIER"):
                self.match("EXPAND_IDENTIFIER")
            else:
                self.match("IDENTIFIER")
            order = "asc"
            if self.check_token("WHITESPACE"):
                self.match("WHITESPACE")
                order = self.current_token.value
                self.match("ORDER")

            identifiers.append(ast.OrderByNodeIdentifier(identifier, order))

            if not self.check_token("VALUE_SEPARATOR"):
                break

            self.match("VALUE_SEPARATOR")

        return ast.OrderByNode(identifiers)

    def parse_skip(self):
        """
        Parse a skip expression.

        Returns:
            ast.SkipNode: The parsed skip expression.
        """
        self.match("SKIP")
        count = int(self.current_token.value)
        self.match("INTEGER")
        return ast.SkipNode(count)

    def parse_top(self):
        """
        Parse a top expression.

        Returns:
            ast.TopNode: The parsed top expression.

        Raises:
            Exception: If an integer value is expected but not found.
        """
        self.match("TOP")
        if self.check_token("INTEGER"):
            count = int(self.current_token.value)
            self.match("INTEGER")
            return ast.TopNode(count)
        else:
            raise Exception(
                f"Expected integer, but found '{self.current_token.type}' ('{self.current_token.value}')"
            )

    def parse_count(self):
        """
        Parse a count expression.

        Returns:
            ast.CountNode: The parsed count expression.
        """
        self.match("COUNT")
        value = self.current_token.value.lower() == "true"
        self.match("BOOL")
        return ast.CountNode(value)

    def parse_asof(self):
        """
        Parse a asof expression.

        Returns:
            ast.AsOfNode: The parsed asof expression.
        """
        self.match("ASOF")
        value = self.current_token.value
        self.match("DATETIME")

        return ast.AsOfNode(value)

    def parse_fromto(self):
        """
        Parse a fromto expression.

        Returns:
            ast.FromToNode: The parsed fromto expression.
        """
        self.match("FROMTO")
        value1 = self.current_token.value
        self.match("DATETIME")
        self.match("SEGMENT_SEPARATOR")
        value2 = self.current_token.value
        self.match("DATETIME")
        return ast.FromToNode(value1, value2)

    def parse_result_format(self):
        """
        Parse a result format expression.

        Returns:
            ast.ResultFormatNode: The parsed result format expression.
        """
        self.match("RESULT_FORMAT")
        value = self.current_token.value
        self.match("RESULT_FORMAT_VALUE")
        return ast.ResultFormatNode(value)

    def parse_subquery(self):
        """
        Parse a subquery.

        Returns:
            ast.QueryNode: The parsed subquery.
        """
        if self.check_token("LEFT_PAREN"):
            self.match("LEFT_PAREN")

        select = None
        filter = None
        expand = None
        orderby = None
        skip = None
        top = None
        count = None
        asof = None
        fromto = None

        # continue parsing until we reach the end of the query
        while True:
            if self.current_token.type == "SELECT":
                select = self.parse_select()
            elif self.current_token.type == "FILTER":
                filter = self.parse_filter(True)
            elif self.current_token.type == "EXPAND":
                expand = self.parse_expand()
            elif self.current_token.type == "SEGMENT_SEPARATOR":
                expand = self.parse_expand()
            elif self.current_token.type == "ORDERBY":
                orderby = self.parse_orderby()
            elif self.current_token.type == "SKIP":
                skip = self.parse_skip()
            elif self.current_token.type == "TOP":
                top = self.parse_top()
            elif self.current_token.type == "COUNT":
                count = self.parse_count()
            elif self.current_token.type == "ASOF":
                asof = self.parse_asof()
            elif self.current_token.type == "FROMTO":
                fromto = self.parse_fromto()
            else:
                raise Exception(f"Unexpected token: {self.current_token.type}")

            # check for other options
            if self.check_token("SUBQUERY_SEPARATOR"):
                self.match("SUBQUERY_SEPARATOR")
            else:
                break

        if self.check_token("RIGHT_PAREN"):
            self.match("RIGHT_PAREN")

        # Subquery cannot have a $resultFormat option
        return ast.QueryNode(
            select,
            filter,
            expand,
            orderby,
            skip,
            top,
            count,
            asof,
            fromto,
            None,
            True,
        )

    def parse_query(self):
        """
        Parse a query.

        Returns:
            ast.QueryNode: The parsed query.
        """
        select = None
        filter = None
        expand = None
        orderby = None
        skip = None
        top = None
        count = None
        asof = None
        fromto = None
        result_format = None

        # continue parsing until we reach the end of the query
        while self.current_token != None:
            if self.current_token.type == "SELECT":
                select = self.parse_select()
            elif self.current_token.type == "FILTER":
                filter = self.parse_filter()
            elif self.current_token.type == "EXPAND":
                expand = self.parse_expand()
            elif self.current_token.type == "ORDERBY":
                orderby = self.parse_orderby()
            elif self.current_token.type == "SKIP":
                skip = self.parse_skip()
            elif self.current_token.type == "TOP":
                top = self.parse_top()
            elif self.current_token.type == "COUNT":
                count = self.parse_count()
            elif self.current_token.type == "ASOF":
                asof = self.parse_asof()
            elif self.current_token.type == "FROMTO":
                fromto = self.parse_fromto()
            elif self.current_token.type == "RESULT_FORMAT":
                result_format = self.parse_result_format()
            else:
                raise Exception(f"Unexpected token: {self.current_token.type}")

            if self.current_token != None:
                self.match("OPTIONS_SEPARATOR")

        return ast.QueryNode(
            select,
            filter,
            expand,
            orderby,
            skip,
            top,
            count,
            asof,
            fromto,
            result_format,
        )

    def parse(self):
        """
        Parse the query.

        Returns:
            ast.QueryNode: The parsed query.
        """
        return self.parse_query()


# Example usage
if __name__ == "__main__":
    text = """$select=id,name,description,properties&$top=1000&$filter=properties/type eq 'station'&$expand=Locations,Datastreams($select=id,name,unitOfMeasurement;$expand=ObservedProperty($select=name),Observations($select=result,phenomenonTime;$orderby=phenomenonTime desc;$top=1))"""
    lexer = Lexer(text)
    tokens = lexer.tokens

    parser = Parser(tokens)
    ast = parser.parse()
