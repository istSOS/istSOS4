"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
import re
from .filter_visitor import FilterVisitor
from odata_query.grammar import ODataLexer
from odata_query.grammar import ODataParser
from .sta_parser.ast import *
from .sta_parser.lexer import Lexer
from .sta_parser.visitor import Visitor
from .sta_parser.parser import Parser
from ..models.models import Location, Thing, HistoricalLocation, ObservedProperty, Sensor, Datastream, Observation, FeaturesOfInterest
from sqlalchemy.orm import sessionmaker, load_only, contains_eager
from sqlalchemy import create_engine, select, func, asc, desc, and_, or_
import os

# Create the OData lexer and parser
odata_filter_lexer = ODataLexer()
odata_filter_parser = ODataParser()

engine = create_engine(os.getenv('DATABASE_URL'), echo=True)

Session = sessionmaker(bind=engine)

id_query_result = False
id_subquery_result = []

class NodeVisitor(Visitor):

    main_entity = None

    """ 
    Constructor for the NodeVisitor class that accepts the main entity name
    """
    def __init__(self, main_entity=None):
        super().__init__()
        self.main_entity = main_entity

    """
    This class provides a visitor to convert a STA query to a PostgREST query.
    """
    
    def visit_IdentifierNode(self, node: IdentifierNode):
        """
        Visit an identifier node.

        Args:
            node (ast.IdentifierNode): The identifier node to visit.

        Returns:
            str: The converted identifier.
        """

        # Replace / with -> for json columns
        node.name = node.name.replace('/', '.')

        return node.name

    def visit_SelectNode(self, node: SelectNode):
        """
        Visit a select node.

        Args:
            node (ast.SelectNode): The select node to visit.

        Returns:
            str: The converted select node.
        """

        identifiers = [f'{self.main_entity}.{self.visit(identifier)}' for identifier in node.identifiers]
        return identifiers

    def visit_FilterNode(self, node: FilterNode):
        """
        Visit a filter node.

        Args:
            node (ast.FilterNode): The filter node to visit.
        
        Returns:
            str: The converted filter node.
        """

        # Parse the filter using the OData lexer and parser
        ast = odata_filter_parser.parse(odata_filter_lexer.tokenize(node.filter))
        # Visit the tree to convert the filter
        res = FilterVisitor().visit(ast)
        results = []
        pattern = re.compile(r'((?:\w+\.)?\w+)(__eq__|__ne__|__gt__|__lt__|__ge__|__le__)(-?\d+(?:\.\d+)?)')
        logical_op = res[0] if isinstance(res, tuple) else None
        conditions = []
        search_string = ','.join(res[1:]) if isinstance(res, tuple) else res
        matches = pattern.findall(search_string)
        for column, operator, value in matches:
            conditions.append([self.main_entity, column, operator, value])
        return [logical_op, conditions]

    def visit_OrderByNodeIdentifier(self, node: OrderByNodeIdentifier):
        """
        Visit an orderby node identifier.

        Args:
            node (ast.OrderByNodeIdentifier): The orderby node identifier to visit.

        Returns:
            str: The converted orderby node identifier.
        """

        # Convert the identifier to the format name.order
        return f'{node.identifier}.{node.order}'

    def visit_OrderByNode(self, node: OrderByNode):
        """
        Visit an orderby node.

        Args:
            node (ast.OrderByNode): The orderby node to visit.
        
        Returns:
            str: The converted orderby node.
        """
        identifiers = [self.visit(identifier) for identifier in node.identifiers]
        attribute_name, *_, order = identifiers[0].split('.')
        if self.main_entity == 'Observation' and 'result' in identifiers[0]:
            results_attrs = ['resultDouble', 'resultInteger', 'resultBoolean', 'resultString', 'resultJSON']
            return [getattr(globals()[self.main_entity], attr) for attr in results_attrs], order
        return [getattr(globals()[self.main_entity], attribute_name)], order

    def visit_SkipNode(self, node: SkipNode):
        """
        Visit a skip node.

        Args:
            node (ast.SkipNode): The skip node to visit.
        
        Returns:
            str: The converted skip node.
        """
        return node.count

    def visit_TopNode(self, node: TopNode):
        """
        Visit a top node.

        Args:
            node (ast.TopNode): The top node to visit.
        
        Returns:
            str: The converted top node.
        """
        return node.count

    def visit_CountNode(self, node: CountNode):
        """
        Visit a count node.

        Args:
            node (ast.CountNode): The count node to visit.
        
        Returns:
            str: The converted count node.
        """
        return node.value
    
    def visit_ExpandNode(self, node: ExpandNode, parent=None):
        """
        Visit an expand node.
        
        Args:
            node (ast.ExpandNode): The expand node to visit.
            parent (str): The parent entity name.
        
        Returns:
            dict: The converted expand node.
        """

        # dict to store the converted parts of the expand node
        select = None
        filter = []
        filters = []
        orderby = ""
        orderbys = []
        skip = ""
        skips = []
        top = ""
        tops = []
        count = ""
        counts = []

        # Visit the identifiers in the expand node
        for index, expand_identifier in enumerate(node.identifiers):
                # Convert the table name
                expand_identifier.identifier = STA2REST.convert_entity(expand_identifier.identifier)
                # Check if we had a parent entity
                prefix = ""
                if parent:
                    prefix = parent
                prefix += expand_identifier.identifier
                fk_entity = globals()[prefix]
                # Check if we have a subquery
                if expand_identifier.subquery:
                    # check if we have a select, filter, orderby, skip, top or count in the subquery
                    if expand_identifier.subquery.select:
                        if not select:
                            select = SelectNode([])
                        identifier_list = [self.visit(identifier) for identifier in expand_identifier.subquery.select.identifiers]
                        if prefix == 'Observation' and 'result' in identifier_list:
                            if 'result' in identifier_list:
                                identifier_list.remove('result')
                            new_results = ['resultInteger', 'resultBoolean', 'resultString', 'resultDouble', 'resultJSON']
                            identifier_list.extend(new_results)
                        identifiers = ','.join(identifier_list)
                        select.identifiers.append(IdentifierNode(f'{expand_identifier.identifier}({identifiers})'))
                    if expand_identifier.subquery.filter:
                        logical_op, filter_criteria = self.visit_FilterNode(expand_identifier.subquery.filter)
                        filter_expressions_child = []
                        filter_expressions_father = []
                        for criteria in filter_criteria:
                            entity, column, operator, value = criteria
                            filter_expression = None
                            if prefix == 'Observation' and column == 'result':
                                result_conditions = STA2REST.handle_observation_result(operator, value)
                                filter_expression = or_(*result_conditions)
                            else:
                                filter_expression = getattr(getattr(fk_entity, column), operator)(value)
                            filter_expressions_child.append(filter_expression)
                            if hasattr(globals()[entity], f"{prefix.lower()}_id"):
                                filter_expression_father = getattr(getattr(globals()[entity], f"{prefix.lower()}_id"), operator)(value)
                                filter_expressions_father.append(filter_expression_father)
                        filter = [logical_op, filter_expressions_child, filter_expressions_father]
                    if expand_identifier.subquery.orderby:
                        identifiers = [self.visit(identifier) for identifier in expand_identifier.subquery.orderby.identifiers]
                        attribute_name, *_, order = identifiers[0].split('.')
                        attrs = []
                        if prefix == 'Observation' and 'result' in identifiers[0]:
                            results_attrs = ['resultDouble', 'resultInteger', 'resultBoolean', 'resultString', 'resultJSON']
                            attrs = [getattr(fk_entity, attr) for attr in results_attrs]
                        else:
                            results_attrs = attribute_name
                            attrs.append(getattr(fk_entity, attribute_name))
                        orderby = [attrs, order, results_attrs]
                    if expand_identifier.subquery.skip:
                        skip = str(expand_identifier.subquery.skip.count) 
                    if expand_identifier.subquery.top:
                        top = str(expand_identifier.subquery.top.count)
                    if expand_identifier.subquery.count:
                        count = str(expand_identifier.subquery.count.value).lower()

                # If we don't have a subquery, we add the identifier to the select node
                if not expand_identifier.subquery or not expand_identifier.subquery.select:
                    if not select:
                        select = SelectNode([])
                    default_columns = STA2REST.get_default_column_names(expand_identifier.identifier)
                    default_columns = [item for item in default_columns if "NavigationLink" not in item]
                    # join default columns as single string
                    default_columns = ','.join(default_columns)
                    select.identifiers.append(IdentifierNode(f'{expand_identifier.identifier}({default_columns})'))

                if expand_identifier.identifier and (not expand_identifier.subquery or not expand_identifier.subquery.orderby):
                    orderby = [[getattr(globals()[expand_identifier.identifier], 'id')], 'desc', None]

                filters.append(filter)
                filter = []
                orderbys.append(orderby)
                orderby = ""
                skips.append(skip)
                skip = ""
                tops.append(top)
                top = ""
                counts.append(count)
                count = ""
        
        # Return the converted expand node
        return {
            'select': select,
            'filter': filters,
            'orderby': orderbys,
            'skip': skips,
            'top': tops,
            'count': counts
        }

    def visit_QueryNode(self, node: QueryNode):
        """
        Visit a query node.

        Args:
            node (ast.QueryNode): The query node to visit.
        
        Returns:
            str: The converted query node.
        """

        # list to store the converted parts of the query node
        session = Session()
        pk_entity = globals()[self.main_entity]
        query_parts = session.query(pk_entity)
        count_query = [session.query(func.count(getattr(pk_entity, 'id').distinct()))]
        no_limited_count = None
        limited_query = select(getattr(pk_entity, 'id'))
        window = None
        sub_query_parts = None
        subqueries = []
        order_subquery = None
        limited_skipped_subqueries = []
        limit_subquery_value = 100
        skip_subquery_value = 0
        orderby_subqueries = None
        globals()["id_query_result"] = False
        globals()["id_subquery_result"] = []
        # Check if we have an expand node before the other parts of the query
        if node.expand:
            # Visit the expand node
            result = self.visit(node.expand)
            for index in range(len(node.expand.identifiers)):
                fk_entity = globals()[node.expand.identifiers[index].identifier]
                limit_subquery_value = 100
                skip_subquery_value = 0
                foreign_key_attr_name = f"{self.main_entity.lower()}_id"
                if hasattr(fk_entity, foreign_key_attr_name):
                    foreign_key_attr  = getattr(fk_entity, foreign_key_attr_name)
                else:
                    foreign_key_attr  = getattr(fk_entity, 'id')
                window = func.row_number().over(
                    partition_by=foreign_key_attr, order_by=desc(foreign_key_attr)
                ).label("rank")
                sub_query_parts = session.query(fk_entity, window)

                globals()["id_subquery_result"].append((node.expand.identifiers[index].identifier, False))
                # Merge the results with the other parts of the query
                if result['select']:
                    if not node.select:
                        node.select = SelectNode([])
                    match = re.match(r'(.*?)\((.*?)\)', result['select'].identifiers[index].name)
                    entity = match.group(1)
                    fields = match.group(2).split(',')
                    if "id" in fields:
                        globals()["id_subquery_result"][index] = (node.expand.identifiers[index].identifier, True)
                    select_query = [getattr(globals()[entity], field.strip()) for field in fields]
                if result['filter'][index]:
                    logical_op, filter_expressions_child, filter_expressions_father = result['filter'][index]
                    is_limited_query = None
                    if filter_expressions_father:
                        expressions = filter_expressions_father
                        is_limited_query = True
                    else:
                        expressions = filter_expressions_child
                        is_limited_query = False
                    if logical_op:
                        combined_expression = logical_op(*expressions)
                    else:
                        combined_expression = expressions[0]
                    if is_limited_query:
                        limited_query = limited_query.filter(combined_expression)
                    else:
                        sub_query_parts = sub_query_parts.filter(combined_expression)
                if result['orderby'][index]:
                    attrs, order, attr_name = result['orderby'][index]
                    order_subquery = order, attr_name
                    ordering = [asc(attribute) for attribute in attrs] if order == 'asc' else [desc(attribute) for attribute in attrs]
                    sub_query_parts = sub_query_parts.order_by(*ordering)
                if result['skip'][index]:
                    skip_subquery_value = result['skip'][index]
                if result['top'][index]:
                    limit_subquery_value = result['top'][index]
                if result['count'][index]:
                    sub_query_parts = sub_query_parts.count()

                sub_query_parts = sub_query_parts.subquery()
                subqueries.append(sub_query_parts)
                limited_skipped_subqueries.append([sub_query_parts, skip_subquery_value, limit_subquery_value])
                foreign_key_attr_name = f"{node.expand.identifiers[index].identifier.lower()}_id"
                if hasattr(pk_entity, foreign_key_attr_name):
                    foreign_key_value = getattr(pk_entity, foreign_key_attr_name, None)
                    query_parts = query_parts.join(
                        subqueries[index], 
                        foreign_key_value == subqueries[index].c.id
                    )
                    count_query[0] = count_query[0].join(
                        subqueries[index], 
                        foreign_key_value == subqueries[index].c.id
                    )
                else:
                    default_join_condition = getattr(pk_entity, 'id') == subqueries[index].c.get(f"{self.main_entity.lower()}_id", None)
                    query_parts = query_parts.join(subqueries[index], default_join_condition)
                    count_query[0] = count_query[0].join(subqueries[index], default_join_condition)
            
                query_parts = query_parts.options(
                    contains_eager(
                        getattr(pk_entity, node.expand.identifiers[index].identifier.lower(), None), 
                        alias=subqueries[index]
                    ).load_only(*select_query)
                )

        if not node.select:
            node.select = SelectNode([])
            # get default columns for main entity
            default_columns = STA2REST.get_default_column_names(self.main_entity)
            for column in default_columns:
                if column == 'id':
                    globals()["id_query_result"] = True
                node.select.identifiers.append(IdentifierNode(column))

        # Check if we have a select, filter, orderby, skip, top or count in the query
        if node.select:
            select_query = []
            for field in self.visit(node.select):
                field_name = field.split('.')[-1]
                if field_name == 'id':
                    globals()["id_query_result"] = True
                if field_name == 'result':
                    select_query.extend([
                        getattr(pk_entity, 'resultInteger'),
                        getattr(pk_entity, 'resultDouble'),
                        getattr(pk_entity, 'resultString'),
                        getattr(pk_entity, 'resultBoolean'),
                        getattr(pk_entity, 'resultJSON')
                    ])
                else:
                    select_query.append(getattr(pk_entity, field_name))
            query_parts = query_parts.options(load_only(*select_query)) if select_query else query_parts
        if node.filter:
            logical_op, filter_criteria = self.visit(node.filter)
            filter_expressions = []
            for entity, column, operator, value in filter_criteria:
                filter_expression = None
                if '.' not in column:
                    if entity == 'Observation' and column == 'result':
                        result_conditions = STA2REST.handle_observation_result(operator, value)
                        filter_expression = or_(*result_conditions)
                    else:
                        filter_query = getattr(globals()[entity], column)
                        filter_expression = getattr(filter_query, operator)(value)
                else:
                    sub_entity, sub_column = column.split('.')
                    sub_query_parts = sub_query_parts or session.query(globals()[sub_entity])
                    filter_query = getattr(globals()[sub_entity], sub_column)
                    filter_expression = getattr(filter_query, operator)(value)
                if filter_expression is not None:
                    filter_expressions.append(filter_expression)
            combined_expression = logical_op(*filter_expressions) if logical_op else filter_expressions[0]
            if '.' not in filter_criteria[-1][1]:
                limited_query = limited_query.filter(combined_expression)
            else:
                sub_query_parts = sub_query_parts.filter(combined_expression).subquery()
                limited_query = limited_query.join(sub_query_parts)
        if node.orderby:
            attrs, order = self.visit(node.orderby)
            ordering = [asc(attribute) for attribute in attrs] if order == 'asc' else [desc(attribute) for attribute in attrs]
        else:
            ordering = [desc(getattr(pk_entity, 'id'))]
        limited_query = limited_query.order_by(*ordering)
        query_parts = query_parts.order_by(*ordering)
        for sq in subqueries:
            if order_subquery[1] is not None:
                query_parts = query_parts.order_by(asc(getattr(sq.c, order_subquery[1]))) if (order_subquery[0] == 'asc') else query_parts.order_by(desc(getattr(sq.c, order_subquery[1])))
            else:
                query_parts = query_parts.order_by(asc(sq.c.id)) if (order_subquery[0] == 'asc') else query_parts.order_by(desc(sq.c.id))
        skip_base = self.visit(node.skip) if node.skip else 0
        top_limit = self.visit(node.top) if node.top else 100
        no_limited_count = limited_query.subquery()
        limited_query = limited_query.offset(skip_base).limit(top_limit).subquery()
        for lsq in limited_skipped_subqueries:
            query_parts = query_parts.filter(
                and_(
                    lsq[0].c.rank > lsq[1],
                    lsq[0].c.rank <= (int(lsq[1]) + int(lsq[2])),
                    getattr(pk_entity, 'id').in_(select(limited_query.c.id))
                )
            ) 
        if not limited_skipped_subqueries:
            query_parts = query_parts.filter(getattr(pk_entity, 'id').in_(select(limited_query.c.id)))
        count_query[0] = count_query[0].filter(getattr(pk_entity, 'id').in_(select(no_limited_count.c.id))) 

        if not node.count:
           count_query.append(False)
        else:
           count_query.append(True)
        # Join the converted parts of the query
        return query_parts, subqueries, count_query

class STA2REST:
    """
    This class provides utility functions to convert various elements used in SensorThings queries to their corresponding
    representations in a REST API.
    """

    # Mapping from SensorThings entities to their corresponding database table names
    ENTITY_MAPPING = {
        "Things": "Thing",
        "Locations": "Location",
        "Sensors": "Sensor",
        "ObservedProperties": "ObservedProperty",
        "Datastreams": "Datastream",
        "Observations": "Observation",
        "FeaturesOfInterest": "FeaturesOfInterest",
        "HistoricalLocations": "HistoricalLocation",

        "Thing": "Thing",
        "Location": "Location",
        "Sensor": "Sensor",
        "ObservedProperty": "ObservedProperty",
        "Datastream": "Datastream",
        "Observation": "Observation",  
        "FeatureOfInterest": "FeaturesOfInterest",
        "HistoricalLocation": "HistoricalLocation",
    }

    # Default columns for each entity
    DEFAULT_SELECT = {
        "Thing": [
            'id',
            'selfLink',
            'locationsNavigationLink',
            'historicalLocationsNavigationLink',
            'datastreamsLocationsNavigationLink',
            'name',
            'description',
            'properties',
        ],
        "Location": [
            'id',
            'selfLink',
            'thingsNavigationLink',
            'historicalLocationsNavigationLink',
            'name',
            'description',
            'encodingType',
            'location',
            'properties',
        ],
        "Sensor": [
            'id',
            'selfLink',
            'datastreamsNavigationLink',
            'name',
            'description',
            'encodingType',
            'sensor_metadata',
            'properties',
        ],
        "ObservedProperty": [
            'id',
            'selfLink',
            'datastreamsNavigationLink',
            'name',
            'description',
            'definition',
            'properties',
        ],
        "Datastream": [
            'id',
            'selfLink',
            'thingNavigationLink',
            'sensorNavigationLink',
            'observedPropertyNavigationLink',
            'observationsNavigationLink',
            'name',
            'description',
            'unitOfMeasurement',
            'observationType',
            'observedArea',
            'phenomenonTime',
            'resultTime',
            'properties',
        ],
        "Observation": [
            'id',
            'selfLink',
            'featureOfInterestNavigationLink',
            'datastreamNavigationLink',
            'phenomenonTime',
            'resultTime',
            'resultInteger',
            'resultString',
            'resultInteger',
            'resultDouble',
            'resultBoolean',
            'resultJSON',
            'resultQuality',
            'validTime',
            'parameters',
        ],
        "FeaturesOfInterest": [
            'id',
            'selfLink',
            'observationsNavigationLink',
            'name',
            'description',
            'encodingType',
            'feature',
            'properties',
        ],
        "HistoricalLocation": [
            'id',
            'selfLink',
            'locationsNavigationLink',
            'thingNavigationLink',
            'time',  
        ],
    }

    @staticmethod
    def get_default_column_names(entity: str) -> list:
        """
        Get the default column names for a given entity.

        Args:
            entity (str): The entity name.

        Returns:
            list: The default column names.
        """
        return STA2REST.DEFAULT_SELECT.get(entity, ["*"])

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
    def convert_to_database_id(entity: str) -> str:
        # First we convert the entity to lower case
        entity = STA2REST.convert_entity(entity).lower()
        return entity + "_id"

    @staticmethod
    def convert_query(full_path: str) -> str:
        """
        Converts a STA query to a PostgREST query.

        Args:
            sta_query (str): The STA query.
        
        Returns:
            str: The converted PostgREST query.
        """

        # check if we have a query
        path = full_path
        query = None
        single_result = False
        dict_expand = True
        if '?' in full_path:
            # Split the query from the path
            path, query = full_path.split('?')

        # Parse the uri
        uri = STA2REST.parse_uri(path)
        
        if not uri:
            raise Exception("Error parsing uri")
    

        # Check if we have a query
        query_ast = QueryNode(None, None, None, None, None, None, None, False)
        if query:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            query_ast = parser.parse()

        main_entity, main_entity_id = uri['entity']
        entities = uri['entities']

        # if query_ast.as_of:
        #     if len(entities) == 0:
        #         main_entity += "_traveltime"
        #         as_of_filter = f"system_time_validity=cs.[{query_ast.as_of.value},{query_ast.as_of.value}{'}'}]"
        #         query_ast.filter = FilterNode(query_ast.filter.filter + f" and {as_of_filter}" if query_ast.filter else as_of_filter)
        #     else:
        #         raise Exception("AS_OF function available only for single entity")
        
        url = f"/{main_entity}"

        print(f"Main entity: {main_entity}")
        
        if entities:
            dict_expand = False
            if not query_ast.expand:
                query_ast.expand = ExpandNode([])
            
            index = 0

            # Merge the entities with the query
            for entity in entities:
                entity_name = entity[0]
                sub_query = QueryNode(None, None, None, None, None, None, None, True)
                if entity[1]:
                    single_result = True
                    sub_query.filter = FilterNode(f"id eq {entity[1]}")
                # Check if we are the last entity
                if index == len(entities) - 1:
                    # Check if we have a property name
                    if uri['property_name']:
                        # Add the property name to the select node
                        if not query_ast.select:
                            query_ast.select = SelectNode([])
                            query_ast.select.identifiers.append(IdentifierNode(uri['property_name']))

                    # Merge the query with the subquery
                    if query_ast.filter:
                        sub_query.filter = query_ast.filter
                        query_ast.filter = None

                    if query_ast.orderby:
                        sub_query.orderby = query_ast.orderby
                        query_ast.orderby = None

                    if query_ast.skip:
                        sub_query.skip = query_ast.skip
                        query_ast.skip = None

                    if query_ast.top:
                        sub_query.top = query_ast.top
                        query_ast.top = None

                    if query_ast.count:
                        sub_query.count = query_ast.count
                        query_ast.count = None

                query_ast.expand.identifiers.append(ExpandNodeIdentifier(entity_name, sub_query))
                index += 1
        else:
            if uri['property_name']:
                if not query_ast.select:
                    query_ast.select = SelectNode([])
                query_ast.select.identifiers.append(IdentifierNode(uri['property_name']))

        # Check if we have a filter in the query
        if main_entity_id:
            query_ast.filter = FilterNode(query_ast.filter.filter + f" and id eq {main_entity_id}" if query_ast.filter else f"id eq {main_entity_id}")

            if not entities:
                single_result = True

        # Check if query has an expand but not a select and does not have sub entities
        if query_ast.expand and not query_ast.select and not entities:
            # Add default columns to the select node
            default_columns = STA2REST.get_default_column_names(main_entity)
            query_ast.select = SelectNode([])
            for column in default_columns:
                query_ast.select.identifiers.append(IdentifierNode(column))

        print(query_ast)

        # Visit the query ast to convert it
        visitor = NodeVisitor(main_entity)
        query_converted = visitor.visit(query_ast)

        return {
            'query': query_converted[0] if query_converted else url,
            'subqueries': query_converted[1],
            'count_query': query_converted[2],
            'ref': uri['ref'],
            'value': uri['value'],
            'single_result': single_result,
            'id_query_result': globals()["id_query_result"],
            'id_subquery_result': globals()["id_subquery_result"],
            'dict_expand' : dict_expand
        }

    @staticmethod
    def parse_entity(entity: str):
        # Check if we have an id in the entity and match only the number
        match = re.search(r'\(\d+\)', entity)
        id = None
        if match:
            # Get the id from the match without the brackets
            id = match.group(0)[1:-1]
            # Remove the id from the entity
            entity = entity.replace(match.group(0), "")

        # Check if the entity is in the ENTITY_MAPPING
        if entity in STA2REST.ENTITY_MAPPING:
            entity = STA2REST.ENTITY_MAPPING[entity]
        else:
            return None

        return (entity, id)
    
    @staticmethod
    def parse_uri(uri: str) -> str:
        # Split the uri by the '/' character
        parts = uri.split('/')
        # Remove the first part
        parts.pop(0)

        # Check if we have a version number
        version = parts.pop(0)

        # Parse first entity
        main_entity = STA2REST.parse_entity(parts.pop(0))
        if not main_entity:
            raise Exception("Error parsing uri: invalid entity")

        # Check all the entities in the uri
        entities = []
        property_name = None
        ref = False
        value = False
        for entity in parts:
            # Parse the entity
            result = STA2REST.parse_entity(entity)
            if result:
                entities.append(result)
            elif entity == "$ref":
                if property_name:
                    raise Exception("Error parsing uri: $ref after property name")
                ref = True
            elif entity == "$value":
                if property_name:
                    value = True
                else:
                    raise Exception("Error parsing uri: $value without property name")
            else:
                property_name = entity
        # Reverse order of entities
        if entities:
            entities = entities[::-1]
            entities.append(main_entity)
            main_entity = entities[0]
            entities.pop(0)
        return {
            'version': version,
            'entity': main_entity,
            'entities': entities,
            'property_name': property_name,
            'ref': ref,
            'value': value
        }

    @staticmethod
    def handle_observation_result(operator, value):
        result_conditions = []
        if operator in ['__eq__', '__ne__', '__gt__', '__lt__', '__ge__', '__le__']:
            for result_type in ['resultInteger', 'resultDouble']:
                filter_query = getattr(globals()['Observation'], result_type)
                result_conditions.append(getattr(filter_query, operator)(float(value)))
        elif operator in ['__eq__', '__ne__']:
            filter_query_string = getattr(globals()['Observation'], 'resultString')
            result_conditions.append(getattr(filter_query_string, operator)(value))
        return result_conditions

if __name__ == "__main__":
    """
    Example usage of the STA2REST module.

    This example converts a STA query to a REST query.
    """
    query = "/v1.1/Datastreams(1)/Observations(1)/resultTime"
    print("QUERY", query)
    print("CONVERTED", STA2REST.convert_query(query))