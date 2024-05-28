"""
Module: STA2REST

Author: Filippo Finke

This module provides utility functions to convert various elements used in SensorThings queries to their corresponding
representations in a REST API.
"""
import re
import os
from .filter_visitor import FilterVisitor
from odata_query import grammar
from odata_query.grammar import ODataLexer
from odata_query.grammar import ODataParser
from .sta_parser.ast import *
from .sta_parser.lexer import Lexer
from .sta_parser.visitor import Visitor
from .sta_parser.parser import Parser
from sqlalchemy.orm import sessionmaker, load_only, contains_eager
from sqlalchemy.sql.expression import BooleanClauseList, BinaryExpression
from sqlalchemy import create_engine, select, func, asc, desc, and_
from datetime import datetime, timezone
from ..models import (
    Location, Thing, HistoricalLocation, ObservedProperty, Sensor,
    Datastream, FeaturesOfInterest, Observation,
    LocationTravelTime, ThingTravelTime, HistoricalLocationTravelTime,
    ObservedPropertyTravelTime, SensorTravelTime, DatastreamTravelTime,
    FeaturesOfInterestTravelTime, ObservationTravelTime
)
from geoalchemy2 import Geometry, WKTElement

ODATA_FUNCTIONS = {
    # String functions
    "substringof": 2,
    "endswith": 2,
    "startswith": 2,
    "length": 1,
    "indexof": 2,
    "substring": (2, 3),
    "tolower": 1,
    "toupper": 1,
    "trim": 1,
    "concat": 2,
    # Datetime functions
    "year": 1,
    "month": 1,
    "day": 1,
    "hour": 1,
    "minute": 1,
    "second": 1,
    "fractionalseconds": 1,
    "date": 1,
    "time": 1,
    "totaloffsetminutes": 1,
    "now": 0,
    "mindatetime": 0,
    "maxdatetime": 0,
    # Math functions
    "round": 1,
    "floor": 1,
    "ceiling": 1,
    # Geo functions
    "geo.distance": 2,
    "geo.length": 1,
    "geo.intersects": 2,
    "st_equals": 2,
    "st_disjoint": 2,
    "st_touches": 2,
    "st_within": 2,
    "st_overlaps": 2,
    "st_crosses": 2,
    "st_intersects": 2,
    "st_contains": 2,
    "st_relate": (2, 3),
}
grammar.ODATA_FUNCTIONS = ODATA_FUNCTIONS

# Create the OData lexer and parser
odata_filter_lexer = ODataLexer()
odata_filter_parser = ODataParser()

engine = create_engine(os.getenv('DATABASE_URL'))

Session = sessionmaker(bind=engine)

ID_QUERY_RESULT = False
ID_SUBQUERY_RESULT = []

class NodeVisitor(Visitor):

    main_entity = None
    db = None

    """ 
    Constructor for the NodeVisitor class that accepts the main entity name
    """
    def __init__(self, main_entity=None, db=None):
        super().__init__()
        self.main_entity = main_entity
        self.db = db

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
        for old_key, new_key in STA2REST.SELECT_MAPPING.items():
            if old_key == node.name:
                node.name = new_key
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

    def visit_FilterNode(self, node: FilterNode, entity: str):
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
        transformer = FilterVisitor(entity)
        res = transformer.visit(ast)
        return res, transformer.join_relationships

    def visit_OrderByNodeIdentifier(self, node: OrderByNodeIdentifier):
        """
        Visit an orderby node identifier.

        Args:
            node (ast.OrderByNodeIdentifier): The orderby node identifier to visit.

        Returns:
            str: The converted orderby node identifier.
        """
        for old_key, new_key in STA2REST.SELECT_MAPPING.items():
            if old_key == node.identifier:
                node.identifier = new_key
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
        attributes = []
        orders = []
        for identifier in identifiers:
            attribute_name, *_, order = identifier.split('.')
            if self.main_entity == 'Observation' and 'result' in identifier:
                results_attrs = ['result_double', 'result_integer', 'result_boolean', 'result_string', 'result_json']
                attributes.append([getattr(globals()[self.main_entity], attr) for attr in results_attrs])
            else:
                attributes.append([getattr(globals()[self.main_entity], attribute_name)])
            orders.append(order)
        return attributes, orders

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
        filter = None
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
                sub_entity = globals()[prefix]
                # Check if we have a subquery
                if expand_identifier.subquery:
                    # Check for select clause in subquery
                    if expand_identifier.subquery.select:
                        if not select:
                            select = SelectNode([])
                        identifiers = [self.visit(identifier) for identifier in expand_identifier.subquery.select.identifiers]
                        identifiers = ','.join(identifiers)
                        select.identifiers.append(IdentifierNode(f'{expand_identifier.identifier}({identifiers})'))

                    # Check for filter clause in subquery
                    if expand_identifier.subquery.filter:
                        filter = self.visit_FilterNode(expand_identifier.subquery.filter, expand_identifier.identifier)

                    # Check for orderby clause in subquery
                    if expand_identifier.subquery.orderby:
                        identifiers = [self.visit(identifier) for identifier in expand_identifier.subquery.orderby.identifiers]
                        attribute_name, *_, order = identifiers[0].split('.')
                        attrs = []
                        if prefix == 'Observation' and 'result' in identifiers[0]:
                            results_attrs = ['result_double', 'result_integer', 'result_boolean', 'result_string', 'result_json']
                            attrs = [getattr(sub_entity, attr) for attr in results_attrs]
                        else:
                            results_attrs = attribute_name
                            attrs.append(getattr(sub_entity, attribute_name))
                        orderby = [attrs, order, results_attrs]

                    # Check for skip clause in subquery
                    if expand_identifier.subquery.skip:
                        skip = str(expand_identifier.subquery.skip.count) 

                    # Check for top clause in subquery
                    if expand_identifier.subquery.top:
                        top = str(expand_identifier.subquery.top.count)

                    # Check for count clause in subquery
                    # if expand_identifier.subquery.count:
                    #     count = str(expand_identifier.subquery.count.value).lower()

                # If there's no subquery or no select clause in the subquery, add the identifier to the select node
                if not expand_identifier.subquery or not expand_identifier.subquery.select:
                    if not select:
                        select = SelectNode([])
                    default_columns = STA2REST.get_default_column_names(expand_identifier.identifier)
                    default_columns = [item for item in default_columns if "navigation_link" not in item]
                    default_columns = ','.join(default_columns)
                    select.identifiers.append(IdentifierNode(f'{expand_identifier.identifier}({default_columns})'))

                # If there's an identifier and no subquery or no orderby clause in the subquery, set default orderby
                if expand_identifier.identifier and (not expand_identifier.subquery or not expand_identifier.subquery.orderby):
                    orderby = [[getattr(globals()[expand_identifier.identifier], 'id')], 'desc', None]

                filters.append(filter)
                filter = None
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
        with self.db as session:
            main_entity = globals()[self.main_entity]
            main_query = session.query(main_entity)
            main_query_select_pagination = select(getattr(main_entity, 'id')) if not 'TravelTime' in self.main_entity else select(getattr(main_entity, 'id'), getattr(main_entity, 'system_time_validity'))
            count_query = [session.query(func.count(getattr(main_entity, 'id').distinct()))] if not 'TravelTime' in self.main_entity else [session.query(func.count(func.distinct(getattr(main_entity, 'id'), getattr(main_entity, 'system_time_validity'))))]
            main_query_select = None
            window = None
            subquery_parts = None
            order_subquery = None
            subqueries = []
            limited_skipped_subqueries = []

            global ID_QUERY_RESULT, ID_SUBQUERY_RESULT
            ID_QUERY_RESULT = False
            ID_SUBQUERY_RESULT = []

            subqueriesNotExpand = []

            # Check if we have an expand node before the other parts of the query
            if node.expand:
                # Visit the expand node
                result = self.visit(node.expand)
                indexNoExpand = 0
                for index, expand_identifier in enumerate(node.expand.identifiers):
                    if (node.expand.identifiers[index].expand):
                        expand_identifier = node.expand.identifiers[index].identifier
                        sub_entity = globals()[node.expand.identifiers[index].identifier]
                        foreign_key_attr_name = f"{self.main_entity.lower()}_id"
                        if hasattr(sub_entity, foreign_key_attr_name):
                            foreign_key_attr  = getattr(sub_entity, foreign_key_attr_name)
                        else:
                            foreign_key_attr  = getattr(sub_entity, 'id')

                        window = func.row_number().over(
                            partition_by=foreign_key_attr, order_by=desc(foreign_key_attr)
                        ).label("rank")
                        subquery_parts = session.query(sub_entity, window)

                        ID_SUBQUERY_RESULT.append((node.expand.identifiers[index].identifier, False))
                        # Merge the results with the other parts of the query
                        if result['select']:
                            select_query = []
                            match = re.match(r'(.*?)\((.*?)\)', result['select'].identifiers[index].name)
                            entity = match.group(1)
                            fields = match.group(2).split(',')
                            
                            if "id" in fields:
                                ID_SUBQUERY_RESULT[index] = (node.expand.identifiers[index].identifier, True)
                                
                            for field in fields:
                                if (entity in ['Observation', 'ObservationTravelTime']) and field == 'result':
                                    select_query.extend([
                                        getattr(globals()[entity], 'result_integer'),
                                        getattr(globals()[entity], 'result_double'),
                                        getattr(globals()[entity], 'result_string'),
                                        getattr(globals()[entity], 'result_boolean'),
                                        getattr(globals()[entity], 'result_json')
                                    ])
                                else:
                                    select_query.append(getattr(globals()[entity], field.strip()))

                        if result['filter'][index] is not None:
                            filter, join_relationships = result['filter'][index]
                            main_entity_has_expand_attribute = hasattr(globals()[self.main_entity], f"{node.expand.identifiers[index].identifier.lower()}_id")
                            expand_entity_has_main_entity_attribute = hasattr(globals()[node.expand.identifiers[index].identifier], f"{self.main_entity.lower()}_id")

                            # Check if the main entity has an attribute based on the expand
                            if main_entity_has_expand_attribute:
                                main_query_select_pagination = main_query_select_pagination.join(globals()[node.expand.identifiers[index].identifier])
                                for rel in join_relationships:
                                    main_query_select_pagination = main_query_select_pagination.join(rel)
                                main_query_select_pagination = main_query_select_pagination.filter(filter)

                            # Check if the expand entity has an attribute based on the main entity
                            if expand_entity_has_main_entity_attribute:
                                main_query_select_pagination = main_query_select_pagination.join(globals()[node.expand.identifiers[index].identifier])
                                for rel in join_relationships:
                                    main_query_select_pagination = main_query_select_pagination.join(rel)
                                    subquery_parts = subquery_parts.join(rel)
                                main_query_select_pagination = main_query_select_pagination.filter(filter)
                                subquery_parts = subquery_parts.filter(filter)

                        if result['orderby'][index]:
                            attrs, order, attr_name = result['orderby'][index]
                            order_subquery = (order, attr_name)
                            
                            # Create list of ordering attributes based on the specified order
                            ordering = [asc(attribute) if order == 'asc' else desc(attribute) for attribute in attrs]
                            
                            # Apply ordering to the subquery parts
                            subquery_parts = subquery_parts.order_by(*ordering)

                        skip_subquery_value = result['skip'][index] or 0

                        limit_subquery_value = result['top'][index] or 100

                        # if result['count'][index]:
                        #     subquery_parts = subquery_parts.count()

                        # Convert subquery_parts to a subquery
                        subquery_parts = subquery_parts.subquery()

                        # Append subquery to list of subqueries
                        subqueries.append(subquery_parts)

                        # Append subquery with skip and limit values to another list
                        limited_skipped_subqueries.append([subquery_parts, skip_subquery_value, limit_subquery_value])

                        # Determine foreign key attribute name
                        foreign_key_attr_name = node.expand.identifiers[index].identifier.lower()
                        
                        main_query = main_query.join(getattr(main_entity, foreign_key_attr_name))
                        count_query[0] = count_query[0].join(getattr(main_entity, foreign_key_attr_name))
                        if (self.main_entity == "Location" and index > 0):
                            new_expand_identifier = globals()[node.expand.identifiers[index].identifier]
                            old_expand_identifier = globals()[node.expand.identifiers[index - 1].identifier]
                            default_join_condition = None
                            if (node.expand.identifiers[index].identifier == "HistoricalLocation" and node.expand.identifiers[index - 1].identifier == "Thing"):
                                default_join_condition = getattr(new_expand_identifier, 'thing_id') == getattr(old_expand_identifier, 'id')
                            elif (node.expand.identifiers[index - 1].identifier == "HistoricalLocation" and node.expand.identifiers[index].identifier == "Thing"):
                                default_join_condition = getattr(old_expand_identifier, 'thing_id') == getattr(new_expand_identifier, 'id')
                            main_query = main_query.join(subqueries[index], default_join_condition)
                            count_query[0] = count_query[0].join(subqueries[index], default_join_condition)
                        else:
                            main_query = main_query.join(subqueries[index])
                            count_query[0] = count_query[0].join(subqueries[index])
                        
                        # # Check if the main entity has a foreign key attribute
                        # if hasattr(main_entity, foreign_key_attr_name):
                        #     foreign_key_value = getattr(main_entity, foreign_key_attr_name, None)
                            
                        #     # Join main query with subquery using foreign key value
                        #     main_query = main_query.join(
                        #         subqueries[index], 
                        #         foreign_key_value == subqueries[index].c.id
                        #     )
                            
                        #     count_query[0] = count_query[0].join(
                        #         subqueries[index], 
                        #         foreign_key_value == subqueries[index].c.id
                        #     )
                        # else:
                        #     # Define default join condition
                        #     default_join_condition = getattr(main_entity, 'id') == subqueries[index].c.get(f"{self.main_entity.lower()}_id", None)
                            
                        #     # Join main query with subquery using default join condition
                        #     main_query = main_query.join(subqueries[index], default_join_condition)
                        #     count_query[0] = count_query[0].join(subqueries[index], default_join_condition)

                        # Configure options for main query
                        main_query = main_query.options(
                            contains_eager(
                                getattr(main_entity, node.expand.identifiers[index].identifier.lower(), None), 
                                alias=subqueries[index]
                            ).load_only(*select_query)
                        )
                    else:
                        expand_identifier = node.expand.identifiers[index].identifier
                        subquery = session.query(globals()[expand_identifier])
                        if (indexNoExpand > 0):
                            previous_expand_identifier = node.expand.identifiers[index - 1].identifier
                            if hasattr(globals()[expand_identifier], previous_expand_identifier.lower()):
                                subquery = subquery.join(getattr(globals()[expand_identifier], previous_expand_identifier.lower())).join(subqueriesNotExpand[indexNoExpand - 1])
                        if result['filter'][index] is not None:
                            filter, join_relationships = result['filter'][index]
                            subquery = subquery.filter(filter)
                        subquery = subquery.subquery()
                        subqueriesNotExpand.append(subquery)
                        indexNoExpand = indexNoExpand + 1
                main_query_select_pagination = main_query_select_pagination.join(getattr(main_entity, expand_identifier.lower()))
                main_query_select_pagination = main_query_select_pagination.join(subqueriesNotExpand[-1]) if subqueriesNotExpand else main_query_select_pagination

            if not node.select:
                node.select = SelectNode([])
                # get default columns for main entity
                default_columns = STA2REST.get_default_column_names(self.main_entity)
                for column in default_columns:
                    node.select.identifiers.append(IdentifierNode(column))

            # Check if we have a select, filter, orderby, skip, top or count in the query
            if node.select:
                select_query = []
                
                # Iterate over fields in node.select
                for field in self.visit(node.select):
                    field_name = field.split('.')[-1]
                    
                    # Check if field is 'id' and set id_query_result flag
                    if field_name == 'id':
                        ID_QUERY_RESULT = True
                    
                    # Check if main_entity is Observation or ObservationTravelTime and field is 'result'
                    if (self.main_entity in ['Observation', 'ObservationTravelTime']) and field_name == 'result':
                        # Extend select_query with result fields
                        select_query.extend([
                            getattr(main_entity, 'result_integer'),
                            getattr(main_entity, 'result_double'),
                            getattr(main_entity, 'result_string'),
                            getattr(main_entity, 'result_boolean'),
                            getattr(main_entity, 'result_json')
                        ])
                    else:
                        # Append field to select_query
                        select_query.append(getattr(main_entity, field_name))
                
                # Set options for main_query if select_query is not empty
                main_query = main_query.options(load_only(*select_query)) if select_query else main_query

            if node.filter:
                filter, join_relationships = self.visit_FilterNode(node.filter, self.main_entity)
                for rel in join_relationships:
                    main_query_select_pagination = main_query_select_pagination.join(rel)
                main_query_select_pagination = main_query_select_pagination.filter(filter)

            if node.orderby:
                attrs, orders = self.visit(node.orderby)
                for attr, order in zip(attrs, orders):
                    ordering = [asc(a) if order == 'asc' else desc(a) for a in attr]
            else:
                ordering = [desc(getattr(main_entity, 'id'))]
            # Apply ordering to main_query_select_pagination
            main_query_select_pagination = main_query_select_pagination.order_by(*ordering)

            # Apply ordering to main_query
            main_query = main_query.order_by(*ordering)

            # Iterate over subqueries and apply ordering
            for sq in subqueries:
                if order_subquery[1] is not None:
                    order_attr = getattr(sq.c, order_subquery[1])
                    ordering = asc(order_attr) if order_subquery[0] == 'asc' else desc(order_attr)
                else:
                    ordering = asc(sq.c.id) if order_subquery[0] == 'asc' else desc(sq.c.id)
                
                # Apply ordering to main_query
                main_query = main_query.order_by(ordering)

            # Determine skip and top values, defaulting to 0 and 100 respectively if not specified
            skip_value = self.visit(node.skip) if node.skip else 0
            top_value = self.visit(node.top) if node.top else 100

            # Create subquery for pagination
            main_query_select = main_query_select_pagination.subquery()
            main_query_select_pagination = main_query_select_pagination.offset(skip_value).limit(top_value).subquery()

            # Apply filters based on limited_skipped_subqueries
            for lsq in limited_skipped_subqueries:
                main_query = main_query.filter(
                    and_(
                        lsq[0].c.rank > lsq[1],
                        lsq[0].c.rank <= (int(lsq[1]) + int(lsq[2])),
                        getattr(main_entity, 'id').in_(select(main_query_select_pagination.c.id))
                    )
                )

            # Apply additional filters if limited_skipped_subqueries is empty
            if not limited_skipped_subqueries:
                if 'TravelTime' not in self.main_entity:
                    main_query = main_query.filter(getattr(main_entity, 'id').in_(select(main_query_select_pagination.c.id)))
                else:
                    main_query = main_query.filter(
                        and_(
                            getattr(main_entity, 'id') == main_query_select_pagination.c.id,
                            getattr(main_entity, 'system_time_validity') == main_query_select_pagination.c.system_time_validity
                        )
                    )

            # Apply filters to count_query
            if 'TravelTime' not in self.main_entity:
                count_query[0] = count_query[0].filter(getattr(main_entity, 'id').in_(select(main_query_select.c.id)))
            else:
                count_query[0] = count_query[0].filter(
                    and_(
                        getattr(main_entity, 'id') == main_query_select.c.id,
                        getattr(main_entity, 'system_time_validity') == main_query_select.c.system_time_validity
                    )
                )

            if not node.count:
                count_query.append(False)
            else:
                if node.count.value:
                    count_query.append(True)
                else:
                    count_query.append(False)

            return main_query, subqueries, count_query

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
        "Location": [
            'id',
            'self_link',
            'things_navigation_link',
            'historical_locations_navigation_link',
            'name',
            'description',
            'encoding_type',
            'location',
            'location_geojson',
            'properties',
        ],
        "LocationTravelTime": [
            'id',
            'self_link',
            'things_navigation_link',
            'historical_locations_navigation_link',
            'name',
            'description',
            'encoding_type',
            'location',
            'location_geojson',
            'properties',
            'system_time_validity',
        ],
        "Thing": [
            'id',
            'self_link',
            'locations_navigation_link',
            'historical_locations_navigation_link',
            'datastreams_locations_navigation_link',
            'name',
            'description',
            'properties',
        ],
        "ThingTravelTime": [
            'id',
            'self_link',
            'locations_navigation_link',
            'historical_locations_navigation_link',
            'datastreams_locations_navigation_link',
            'name',
            'description',
            'properties',
            'system_time_validity',
        ],
        "HistoricalLocation": [
            'id',
            'self_link',
            'locations_navigation_link',
            'thing_navigation_link',
            'time',  
        ],
        "HistoricalLocationTravelTime": [
            'id',
            'self_link',
            'locations_navigation_link',
            'thing_navigation_link',
            'time',  
            'system_time_validity',
        ],
        "ObservedProperty": [
            'id',
            'self_link',
            'datastreams_navigation_link',
            'name',
            'description',
            'definition',
            'properties',
        ],
        "ObservedPropertyTravelTime": [
            'id',
            'self_link',
            'datastreams_navigation_link',
            'name',
            'description',
            'definition',
            'properties',
            'system_time_validity',
        ],
        "Sensor": [
            'id',
            'self_link',
            'datastreams_navigation_link',
            'name',
            'description',
            'encoding_type',
            'sensor_metadata',
            'properties',
        ],
        "SensorTravelTime": [
            'id',
            'self_link',
            'datastreams_navigation_link',
            'name',
            'description',
            'encoding_type',
            'sensor_metadata',
            'properties',
            'system_time_validity',
        ],
        "Datastream": [
            'id',
            'self_link',
            'thing_navigation_link',
            'sensor_navigation_link',
            'observed_property_navigation_link',
            'observations_navigation_link',
            'name',
            'description',
            'unit_of_measurement',
            'observation_type',
            'observed_area',
            'observed_area_geojson',
            'phenomenon_time',
            'result_time',
            'properties',
        ],
        "DatastreamTravelTime": [
            'id',
            'self_link',
            'thing_navigation_link',
            'sensor_navigation_link',
            'observed_property_navigation_link',
            'observations_navigation_link',
            'name',
            'description',
            'unit_of_measurement',
            'observation_type',
            'observed_area',
            'observed_area_geojson',
            'phenomenon_time',
            'result_time',
            'properties',
            'system_time_validity',
        ],
        "FeaturesOfInterest": [
            'id',
            'self_link',
            'observations_navigation_link',
            'name',
            'description',
            'encoding_type',
            'feature',
            'feature_geojson',
            'properties',
        ],
        "FeaturesOfInterestTravelTime": [
            'id',
            'self_link',
            'observations_navigation_link',
            'name',
            'description',
            'encoding_type',
            'feature',
            'feature_geojson',
            'properties',
            'system_time_validity',
        ],
        "Observation": [
            'id',
            'self_link',
            'feature_of_interest_navigation_link',
            'datastream_navigation_link',
            'phenomenon_time',
            'result_time',
            'result_string',
            'result_integer',
            'result_double',
            'result_boolean',
            'result_json',
            'result_quality',
            'valid_time',
            'parameters',
        ],
        "ObservationTravelTime": [
            'id',
            'self_link',
            'feature_of_interest_navigation_link',
            'datastream_navigation_link',
            'phenomenon_time',
            'result_time',
            'result_string',
            'result_integer',
            'result_double',
            'result_boolean',
            'result_json',
            'result_quality',
            'valid_time',
            'parameters',
            'system_time_validity'
        ],
    }

    SELECT_MAPPING = {
        'encodingType': 'encoding_type',
        'metadata': 'sensor_metadata',
        'unitOfMeasurement': 'unit_of_measurement',
        'observationType': 'observation_type',
        'observedArea': 'observed_area',
        'phenomenonTime': 'phenomenon_time',
        'resultTime': 'result_time',
        'resultQuality': 'result_quality',
        'validTime': 'valid_time'
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
        select = STA2REST.DEFAULT_SELECT.get(entity, ["*"])
        for old_key, new_key in STA2REST.SELECT_MAPPING.items():
            if old_key in select:
                select.remove(old_key)
                select.append(new_key)
        return select

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
    def convert_query(full_path: str, db: Session) -> str:
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
        query_ast = QueryNode(None, None, None, None, None, None, None, None, None, None, False)
        if query:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            query_ast = parser.parse()

        main_entity, main_entity_id = uri['entity']
        entities = uri['entities']

        if query_ast.as_of:
            if len(entities) == 0 and not query_ast.expand:
                main_entity += "TravelTime"
                as_of_filter = f"system_time_validity eq {query_ast.as_of.value}"
                query_ast.filter = FilterNode(query_ast.filter.filter + f" and {as_of_filter}" if query_ast.filter else as_of_filter)
            else:
                raise Exception("AS_OF function available only for single entity")
            # if query_ast.expand:
            #     for identifier in query_ast.expand.identifiers:
            #         identifier.identifier = identifier.identifier + "TravelTime"
            #         identifier.subquery = QueryNode(None, None, None, None, None, None, None, None, None, True) if identifier.subquery is None else identifier.subquery
            #         identifier.subquery.filter = FilterNode(identifier.subquery.filter + f" and {as_of_filter}" if identifier.subquery.filter else as_of_filter)

        if query_ast.from_to:
            if len(entities) == 0 and not query_ast.expand:
                main_entity += "TravelTime"
                from_to_filter = f"system_time_validity eq ({query_ast.from_to.value1}, {query_ast.from_to.value2})"
                query_ast.filter = FilterNode(query_ast.filter.filter + f" and {from_to_filter}" if query_ast.filter else from_to_filter)
            else:
                raise Exception("FROM_TO function available only for single entity")
        
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
                sub_query = QueryNode(None, None, None, None, None, None, None, None, None, None, True)
                if entity[1]:
                    sub_query.filter = FilterNode(f"id eq {entity[1]}")
                # Check if we are the last entity
                if index == len(entities) - 1:
                    # Check if we have a property name
                    if uri['property_name']:
                        single_result = True
                        # Add the property name to the select node
                        if not query_ast.select:
                            query_ast.select = SelectNode([])
                            query_ast.select.identifiers.append(IdentifierNode(uri['property_name']))

                    # Merge the query with the subquery
                    # if query_ast.filter:
                    #     sub_query.filter = query_ast.filter
                    #     query_ast.filter = None

                    # if query_ast.orderby:
                    #     sub_query.orderby = query_ast.orderby
                    #     query_ast.orderby = None

                    # if query_ast.skip:
                    #     sub_query.skip = query_ast.skip
                    #     query_ast.skip = None

                    # if query_ast.top:
                    #     sub_query.top = query_ast.top
                    #     query_ast.top = None

                    # if query_ast.count:
                    #     sub_query.count = query_ast.count
                    #     query_ast.count = None

                query_ast.expand.identifiers.append(ExpandNodeIdentifier(entity_name, sub_query, False))
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

        if uri['single']:
            single_result = True

        if query_ast.expand:
            dict_expand = True

        # Check if query has an expand but not a select and does not have sub entities
        if query_ast.expand and not query_ast.select and not entities:
            # Add default columns to the select node
            default_columns = STA2REST.get_default_column_names(main_entity)
            query_ast.select = SelectNode([])
            for column in default_columns:
                query_ast.select.identifiers.append(IdentifierNode(column))

        print(query_ast)

        # Visit the query ast to convert it
        visitor = NodeVisitor(main_entity, db)
        query_converted = visitor.visit(query_ast)

        # Result format is allowed only for Observations
        if query_ast.result_format and main_entity != 'Observation':
            raise Exception("Illegal operation: $resultFormat is only valid for /Observations")
        
        if query_ast.result_format and query_ast.expand:
            raise Exception("Illegal operation: $expand is not allowed with $resultFormat")

        subqueries = [subquery for subquery in query_converted[1]]
        global ID_QUERY_RESULT, ID_SUBQUERY_RESULT
        return {
            'query': query_converted[0].all() if query_converted else url,
            'count_query': query_converted[2],
            'query_count': query_converted[2][0].scalar(),
            'ref': uri['ref'],
            'value': uri['value'],
            'single_result': single_result,
            'id_query_result': ID_QUERY_RESULT,
            'id_subquery_result': ID_SUBQUERY_RESULT,
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
        version = os.getenv('VERSION')
        parts = uri.split(version)
        parts = parts[1]
        parts = parts.split('/')

        # Remove the first part
        parts.pop(0)

        if(parts[-1] == "$ref"):
            entity_name = parts[-2]
        elif parts[-1] == "$value":
            entity_name = parts[-3]
        else:
            entity_name = parts[-1]
        single = False
        keys_list = list(STA2REST.ENTITY_MAPPING.keys())
        if entity_name in keys_list:
            index = keys_list.index(entity_name)
            if index > 7:
                single = True
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
            entities = entities[::-1]

        return {
            'version': version,
            'entity': main_entity,
            'entities': entities,
            'property_name': property_name,
            'ref': ref,
            'value': value,
            'single': single
        }

if __name__ == "__main__":
    """
    Example usage of the STA2REST module.

    This example converts a STA query to a REST query.
    """
    query = "/v1.1/Datastreams(1)/Observations(1)/resultTime"
    print("QUERY", query)
    print("CONVERTED", STA2REST.convert_query(query))