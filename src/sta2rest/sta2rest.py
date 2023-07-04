"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
from odata_query.grammar import ODataLexer
from odata_query.grammar import ODataParser
from filter_visitor import FilterVisitor
from sta_parser.lexer import Lexer
from sta_parser.visitor import Visitor
from sta_parser.parser import Parser
from sta_parser import ast

odata_filter_lexer = ODataLexer()
odata_filter_parser = ODataParser()

class NodeVisitor(Visitor):
    def visit_IdentifierNode(self, node: ast.IdentifierNode):
        return node.name

    def visit_SelectNode(self, node: ast.SelectNode):
        identifiers = ','.join([self.visit(identifier) for identifier in node.identifiers])
        return f'select={identifiers}'

    def visit_FilterNode(self, node: ast.FilterNode):
        ast = odata_filter_parser.parse(odata_filter_lexer.tokenize(node.filter))
        # Visit the tree to convert the filter
        res = FilterVisitor().visit(ast)
        return res

    def visit_OrderByNodeIdentifier(self, node: ast.OrderByNodeIdentifier):
        return f'{node.identifier}.{node.order}'

    def visit_OrderByNode(self, node: ast.OrderByNode):
        identifiers = ','.join([self.visit(identifier) for identifier in node.identifiers])
        return f'order={identifiers}'

    def visit_SkipNode(self, node: ast.SkipNode):
        return f'offset={node.count}'

    def visit_TopNode(self, node: ast.TopNode):
        return f'limit={node.count}'

    def visit_CountNode(self, node: ast.CountNode):
        return f'count={node.value}'
    
    def visit_ExpandNode(self, node: ast.ExpandNode, parent=None):

        select = None
        filter = ""
        orderby = ""
        skip = ""
        top = ""
        count = ""

        for expand_identifier in node.identifiers:
                expand_identifier.identifier = STA2REST.convert_entity(expand_identifier.identifier)
                prefix = ""
                if parent:
                    prefix = parent
                prefix += expand_identifier.identifier + "."
        
                if expand_identifier.subquery:
                    if expand_identifier.subquery.select:
                        if not select:
                            select = ast.SelectNode([])
                        identifiers = ','.join([self.visit(identifier) for identifier in expand_identifier.subquery.select.identifiers])
                        select.identifiers.append(ast.IdentifierNode(f'{expand_identifier.identifier}({identifiers})'))
                    if expand_identifier.subquery.filter:
                        result = self.visit_FilterNode(expand_identifier.subquery.filter)
                        filter = prefix + result
                    if expand_identifier.subquery.orderby:
                        orderby = prefix + "order=" + ','.join([self.visit(identifier) for identifier in expand_identifier.subquery.orderby.identifiers])
                    if expand_identifier.subquery.skip:
                        skip = prefix + "offset=" + str(expand_identifier.subquery.skip.count)
                    if expand_identifier.subquery.top:
                        top = prefix + "limit=" + str(expand_identifier.subquery.top.count)
                    if expand_identifier.subquery.count:
                        count = prefix + "count=" + str(expand_identifier.subquery.count.value).lower()

                    if expand_identifier.subquery.expand:
                        result = self.visit_ExpandNode(expand_identifier.subquery.expand, prefix)
                        if result['select']:
                            if not select:
                                select = ast.SelectNode([])
                            select.identifiers.extend(result['select'].identifiers)
                        if result['orderby']:
                            if orderby:
                                orderby += "&"
                            orderby += result['orderby']
                        if result['skip']:
                            if skip:
                                skip += "&"
                            skip += result['skip']
                        if result['top']:
                            if top:
                                top += "&"
                            top += result['top']
                        if result['count']:
                            if count:
                                count += "&"
                            count += result['count']
                        if result['filter']:
                            if filter:
                                filter += "&"
                            filter += result['filter']
                
                if not expand_identifier.subquery or not expand_identifier.subquery.select:
                    if not select:
                        select = ast.SelectNode([])
                    select.identifiers.append(ast.IdentifierNode(f'{expand_identifier.identifier}(*)'))
        return {
            'select': select,
            'filter': filter,
            'orderby': orderby,
            'skip': skip,
            'top': top,
            'count': count
        }

    def visit_QueryNode(self, node: ast.QueryNode):
        query_parts = []

        if node.expand:
            result = self.visit(node.expand)
            if result['select']:
                if not node.select:
                    node.select = ast.SelectNode([])
                node.select.identifiers.extend(result['select'].identifiers)
            if result['orderby']:
                query_parts.append(result['orderby'])
            if result['skip']:
                query_parts.append(result['skip'])
            if result['top']:
                query_parts.append(result['top'])
            if result['count']:
                query_parts.append(result['count'])
            if result['filter']:
                query_parts.append(result['filter'])

        if node.select:
            query_parts.append(self.visit(node.select))
        if node.filter:
            query_parts.append(self.visit(node.filter))
        if node.orderby:
            query_parts.append(self.visit(node.orderby))
        if node.skip:
            query_parts.append(self.visit(node.skip))
        if node.top:
            query_parts.append(self.visit(node.top))
        if node.count:
            query_parts.append(self.visit(node.count).lower())
        
        return '&'.join(query_parts)

class STA2REST:

    # Mapping from SensorThings entities to their corresponding database table names
    ENTITY_MAPPING = {
        "Things": "Thing",
        "Locations": "Location",
        "Sensors": "Sensor",
        "ObservedProperties": "ObservedProperty",
        "Datastreams": "Datastream",
        "Observations": "Observation",
        "FeaturesOfInterest": "FeatureOfInterest",
        "HistoricalLocations": "HistoricalLocation",
    }
    @staticmethod
    def convert_entity(entity: str) -> str:
        """
        Converts an entity name from STA format to REST format.

        Args:
            entity (str): The entity name in STA format.

        Returns:
            str: The converted entity name in REST format.
        """
        return STA2REST.ENTITY_MAPPING.get(entity, entity)
  
    @staticmethod
    def convert_query(sta_query: str) -> str:
        lexer = Lexer(sta_query)
        tokens = lexer.tokenize()
        parser = Parser(tokens)
        ast = parser.parse()
        visitor = NodeVisitor()
        return visitor.visit(ast)


if __name__ == "__main__":
    """
    Example usage of the STA2REST module.

    This example converts a STA query to a REST query.
    """
    query = "$expand=Observations($filter=result eq 1)"
    print("QUERY", query)
    print("CONVERTED", STA2REST.convert_query(query))