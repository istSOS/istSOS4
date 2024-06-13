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
from sqlalchemy.orm import sessionmaker, load_only, contains_eager, joinedload
from sqlalchemy.sql.expression import BooleanClauseList, BinaryExpression
from sqlalchemy import create_engine, select, func, asc, desc, and_, literal, text
from sqlalchemy.inspection import inspect
from datetime import datetime, timezone
from ..models import (
    Location, Thing, HistoricalLocation, ObservedProperty, Sensor,
    Datastream, FeaturesOfInterest, Observation,
    LocationTravelTime, ThingTravelTime, HistoricalLocationTravelTime,
    ObservedPropertyTravelTime, SensorTravelTime, DatastreamTravelTime,
    FeaturesOfInterestTravelTime, ObservationTravelTime
)
from sqlalchemy.dialects.postgresql.json import JSONB
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

        identifiers = [
            f'{self.main_entity}.{self.visit(identifier)}' for identifier in node.identifiers]
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
        ast = odata_filter_parser.parse(
            odata_filter_lexer.tokenize(node.filter))
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
        identifiers = [self.visit(identifier)
                       for identifier in node.identifiers]
        attributes = []
        orders = []
        for identifier in identifiers:
            attribute_name, *_, order = identifier.split('.')
            if self.main_entity == 'Observation' and 'result' in identifier:
                results_attrs = ['result_double', 'result_integer',
                                 'result_boolean', 'result_string', 'result_json']
                attributes.append(
                    [getattr(globals()[self.main_entity], attr) for attr in results_attrs])
            else:
                attributes.append(
                    [getattr(globals()[self.main_entity], attribute_name)])
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
            node (ExpandNode): The expand node to visit.
            parent (str): The parent entity name.

        Returns:
            list: A list of tuples containing the sub-query and the identifier of each expand node.
        """

        with self.db as session:
            expand_queries = []

            # Process each identifier in the expand node
            for expand_identifier in node.identifiers:
                # Convert the table name
                expand_identifier.identifier = STA2REST.convert_entity(
                    expand_identifier.identifier)
                sub_entity = globals()[expand_identifier.identifier]

                sub_query = None
                subquery_ranked = None

                # Prepare select fields
                select_fields = []
                if expand_identifier.subquery and expand_identifier.subquery.select:
                    identifiers = [
                        self.visit(identifier)
                        for identifier in expand_identifier.subquery.select.identifiers
                    ]
                else:
                    identifiers = STA2REST.get_default_column_names(
                        expand_identifier.identifier)
                    identifiers = [
                        item for item in identifiers if "navigation_link" not in item]

                for field in identifiers:
                    select_fields.append(getattr(sub_entity, field))

                relationship = None
                fk_name = None
                fk_attr = None
                if parent:
                    relationship = getattr(
                        globals()[parent], expand_identifier.identifier.lower()).property
                    if relationship.direction.name == "ONETOMANY":
                        fk_name = f"{parent.lower()}_id"
                        fk_attr = getattr(sub_entity, fk_name)
                        select_fields.insert(0, fk_attr)
                    elif relationship.direction.name == "MANYTOMANY":
                        fk_name = f"{parent.lower()}_id"
                        fk_attr = relationship.secondary.c[fk_name]
                        select_fields.insert(0, fk_attr)

                # Build sub-query with row number
                sub_query = select(
                    *select_fields,
                    func.row_number().over(
                        partition_by=getattr(
                            sub_entity, "id") if fk_attr is None else fk_attr,
                        order_by=getattr(sub_entity, "id")
                    ).label("rank")
                )

                # Process filter clause if exists
                if expand_identifier.subquery and expand_identifier.subquery.filter:
                    filter, join_relationships = self.visit_FilterNode(
                        expand_identifier.subquery.filter, expand_identifier.identifier
                    )
                    for rel in join_relationships:
                        sub_query = sub_query.join(rel)
                    sub_query = sub_query.filter(filter)

                # Process orderby clause
                ordering = []
                if expand_identifier.subquery and expand_identifier.subquery.orderby:
                    identifiers = [
                        self.visit(identifier)
                        for identifier in expand_identifier.subquery.orderby.identifiers
                    ]
                    for field in identifiers:
                        attr, order = field.split(".")
                        ordering.append(asc(attr.collate('C'))
                                        if order == "asc" else desc(attr.collate('C')))
                else:
                    ordering = [desc(getattr(sub_entity, "id"))]
                sub_query = sub_query.order_by(*ordering)

                # Process skip clause
                skip_value = expand_identifier.subquery.skip.count if expand_identifier.subquery and expand_identifier.subquery.skip else 0

                # Process top clause
                top_value = expand_identifier.subquery.top.count if expand_identifier.subquery and expand_identifier.subquery.top else 100

                # Build the ranked subquery
                subquery_ranked = select(
                    *[col for col in sub_query.columns if col.name != 'rank'])
                subquery_ranked = subquery_ranked.filter(
                    sub_query.c.rank > skip_value,
                    sub_query.c.rank <= (top_value + skip_value),
                ).alias(f"subquery_ranked_{expand_identifier.identifier.lower()}")

                # Construct JSON object arguments
                json_build_object_args = []
                for attr in subquery_ranked.columns:
                    if fk_attr is None or (fk_attr is not None and attr.name != fk_attr.name):
                        json_build_object_args.append(
                            literal(attr.name) if attr.name != 'id' else text("'@iot.id'"))
                        json_build_object_args.append(func.ST_AsGeoJSON(attr).cast(
                            JSONB) if isinstance(attr.type, Geometry) else attr)

                aggregation_type = func.array_agg(func.json_build_object(*json_build_object_args))[1] if relationship.direction.name in [
                    "MANYTOONE"] else func.json_agg(func.json_build_object(*json_build_object_args))

                # Build sub-query JSON aggregation
                if relationship.direction.name in ["MANYTOONE", "ONETOMANY"]:
                    sub_query_json_agg = (
                        select(
                            subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id,
                            aggregation_type.label(
                                expand_identifier.identifier.lower()),
                        )
                        .group_by(subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id)
                        .alias(f"sub_query_json_agg_{expand_identifier.identifier.lower()}")
                    )
                else:
                    sub_query_json_agg = (
                        select(
                            subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id,
                            aggregation_type.label(
                                expand_identifier.identifier.lower()),
                        )
                        .join(relationship.secondary)
                        .group_by(subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id)
                        .alias(f"sub_query_json_agg_{expand_identifier.identifier.lower()}")
                    )
                # Handle nested expand
                if expand_identifier.subquery and expand_identifier.subquery.expand:
                    nested_expand_queries = self.visit_ExpandNode(
                        expand_identifier.subquery.expand, expand_identifier.identifier)
                    for nested_expand_query, nested_identifier in nested_expand_queries:
                        value = getattr(
                            sub_entity, f"{nested_identifier.lower()}_navigation_link")
                        json_build_object_args.append(
                            f"{value.name.split('@')[0]}")
                        if relationship.direction.name == "MANYTOONE":
                            json_build_object_args.append(func.coalesce(
                                nested_expand_query.columns[nested_identifier.lower()], text("'{}'")))
                        else:
                            json_build_object_args.append(func.coalesce(
                                nested_expand_query.columns[nested_identifier.lower()], text("'[]'")))

                        aggregation_type = func.array_agg(func.json_build_object(*json_build_object_args))[1] if relationship.direction.name in [
                            "MANYTOONE"] else func.json_agg(func.json_build_object(*json_build_object_args))

                        sub_query_json_agg = (
                            select(
                                subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id,
                                aggregation_type.label(
                                    expand_identifier.identifier.lower()),
                            )
                            .select_from(sub_entity)
                            .join(nested_expand_query)
                            .group_by(subquery_ranked.c[fk_name] if fk_attr is not None else subquery_ranked.c.id)
                            .alias(f"sub_query_json_agg_{expand_identifier.identifier.lower()}")
                        )

                expand_queries.append(
                    (sub_query_json_agg, expand_identifier.identifier))

            return expand_queries

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
            main_query = None
            query_count = session.query(func.count(getattr(main_entity, 'id').distinct())) if not 'TravelTime' in self.main_entity else session.query(
                func.count(func.distinct(getattr(main_entity, 'id'), getattr(main_entity, 'system_time_validity'))))

            if not node.select:
                node.select = SelectNode([])
                # get default columns for main entity
                default_columns = STA2REST.get_default_column_names(
                    self.main_entity)
                for column in default_columns:
                    node.select.identifiers.append(IdentifierNode(column))

            # Check if we have a select, filter, orderby, skip, top or count in the query
            if node.select:
                select_query = []

                # Iterate over fields in node.select
                for field in self.visit(node.select):
                    field_name = field.split('.')[-1]
                    select_query.append(getattr(main_entity, field_name))

            json_build_object_args = []
            for attr in select_query:
                json_build_object_args.append(literal(
                    attr.name)) if attr.name != 'id' else json_build_object_args.append(text("'@iot.id'"))
                json_build_object_args.append(func.ST_AsGeoJSON(attr).cast(JSONB)) if (
                    type(attr.type) == Geometry) else json_build_object_args.append(attr)

            # Check if we have an expand node before the other parts of the query
            if node.expand:
                new_node = {'expand': {'identifiers': []}}
                for e in node.expand.identifiers:
                    if not e.expand:
                        new_node['expand']['identifiers'].append(e)

                if len(new_node['expand']['identifiers']) > 0:
                    main_query = select(
                        func.json_build_object(*json_build_object_args))
                    sub_queries = []
                    current = None
                    previous = None
                    for i, e in enumerate(new_node['expand']['identifiers']):
                        current = e.identifier
                        sub_query = session.query(globals()[current])
                        if i > 0:
                            previous = new_node['expand']['identifiers'][i - 1].identifier
                            sub_query = sub_query.join(
                                getattr(globals()[current], previous.lower())).join(sub_queries[i - 1])
                        if e.subquery and e.subquery.filter:
                            filter, join_relationships = self.visit_FilterNode(
                                e.subquery.filter, current)
                            sub_query = sub_query.filter(filter)
                        sub_query = sub_query.subquery()
                        sub_queries.append(sub_query)
                    if len(sub_queries) > 0:
                        main_query = main_query.join(getattr(main_entity, current.lower())).join(
                            sub_queries[-1]).distinct(getattr(main_entity, 'id'))
                        query_count = query_count.join(
                            getattr(main_entity, current.lower())).join(sub_queries[-1])

                node.expand.identifiers = [
                    e for e in node.expand.identifiers if e.expand]
                if len(node.expand.identifiers) > 0:
                    # Visit the expand node
                    sub_queries = self.visit_ExpandNode(
                        node.expand, self.main_entity)
                    for sq in sub_queries:
                        sub_query, alias = sq
                        value = getattr(
                            main_entity, f"{alias.lower()}_navigation_link")
                        json_build_object_args.append(
                            f"{value.name.split('@')[0]}")
                        if getattr(main_entity, alias.lower()).property.direction.name == "MANYTOONE":
                            json_build_object_args.append(func.coalesce(
                                sub_query.columns[alias.lower()], text("'{}'")))
                        else:
                            json_build_object_args.append(func.coalesce(
                                sub_query.columns[alias.lower()], text("'[]'")))
                    main_query = select(func.json_build_object(
                        *json_build_object_args)).select_from(main_entity)

                    if (self.main_entity == 'Location' and sub_queries[0][1] == 'HistoricalLocation'):
                        sub_queries = list(reversed(sub_queries))

                    for sq in sub_queries:
                        sub_query, alias = sq
                        if getattr(main_entity, alias.lower()).property.direction.name == "MANYTOMANY":
                            main_query = main_query.join(
                                getattr(main_entity, alias.lower()))
                        if hasattr(main_entity, f"{alias.lower()}_id"):
                            fk_attr = getattr(
                                main_entity, f"{alias.lower()}_id")
                            main_query = main_query.join(
                                sub_query, fk_attr == sub_query.c.id)
                        else:
                            main_query = main_query.join(sub_query)
            else:
                # Set options for main_query if select_query is not empty
                main_query = select(
                    func.json_build_object(*json_build_object_args))

            if node.filter:
                filter, join_relationships = self.visit_FilterNode(
                    node.filter, self.main_entity)
                for rel in join_relationships:
                    main_query = main_query.join(rel)
                main_query = main_query.filter(filter)
                query_count = query_count.filter(filter)

            if node.orderby:
                attrs, orders = self.visit(node.orderby)
                for attr, order in zip(attrs, orders):
                    ordering = [asc(a.collate('C')) if order == 'asc' else desc(a.collate('C'))
                                for a in attr]
            else:
                ordering = [desc(getattr(main_entity, 'id'))]

            # Apply ordering to main_query
            main_query = main_query.order_by(*ordering)

            # Determine skip and top values, defaulting to 0 and 100 respectively if not specified
            skip_value = self.visit(node.skip) if node.skip else 0
            top_value = self.visit(node.top) if node.top else 100

            main_query = main_query.offset(skip_value).limit(top_value)

            if not node.count:
                count_query = False
            else:
                if node.count.value:
                    count_query = True

                else:
                    count_query = False

            return main_query, count_query, query_count


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
            'thing_navigation_link',
            'historicallocation_navigation_link',
            'name',
            'description',
            'encoding_type',
            'location',
            'properties',
        ],
        "LocationTravelTime": [
            'id',
            'self_link',
            'thing_navigation_link',
            'historicallocation_navigation_link',
            'name',
            'description',
            'encoding_type',
            'location',
            'properties',
            'system_time_validity',
        ],
        "Thing": [
            'id',
            'self_link',
            'location_navigation_link',
            'historicallocation_navigation_link',
            'datastream_navigation_link',
            'name',
            'description',
            'properties',
        ],
        "ThingTravelTime": [
            'id',
            'self_link',
            'location_navigation_link',
            'historicallocation_navigation_link',
            'datastream_navigation_link',
            'name',
            'description',
            'properties',
            'system_time_validity',
        ],
        "HistoricalLocation": [
            'id',
            'self_link',
            'location_navigation_link',
            'thing_navigation_link',
            'time',
        ],
        "HistoricalLocationTravelTime": [
            'id',
            'self_link',
            'location_navigation_link',
            'thing_navigation_link',
            'time',
            'system_time_validity',
        ],
        "ObservedProperty": [
            'id',
            'self_link',
            'datastream_navigation_link',
            'name',
            'description',
            'definition',
            'properties',
        ],
        "ObservedPropertyTravelTime": [
            'id',
            'self_link',
            'datastream_navigation_link',
            'name',
            'description',
            'definition',
            'properties',
            'system_time_validity',
        ],
        "Sensor": [
            'id',
            'self_link',
            'datastream_navigation_link',
            'name',
            'description',
            'encoding_type',
            'sensor_metadata',
            'properties',
        ],
        "SensorTravelTime": [
            'id',
            'self_link',
            'datastream_navigation_link',
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
            'observedproperty_navigation_link',
            'observation_navigation_link',
            'name',
            'description',
            'unit_of_measurement',
            'observation_type',
            'observed_area',
            'phenomenon_time',
            'result_time',
            'properties',
        ],
        "DatastreamTravelTime": [
            'id',
            'self_link',
            'thing_navigation_link',
            'sensor_navigation_link',
            'observedproperty_navigation_link',
            'observation_navigation_link',
            'name',
            'description',
            'unit_of_measurement',
            'observation_type',
            'observed_area',
            'phenomenon_time',
            'result_time',
            'properties',
            'system_time_validity',
        ],
        "FeaturesOfInterest": [
            'id',
            'self_link',
            'observation_navigation_link',
            'name',
            'description',
            'encoding_type',
            'feature',
            'properties',
        ],
        "FeaturesOfInterestTravelTime": [
            'id',
            'self_link',
            'observation_navigation_link',
            'name',
            'description',
            'encoding_type',
            'feature',
            'properties',
            'system_time_validity',
        ],
        "Observation": [
            'id',
            'self_link',
            'featuresofinterest_navigation_link',
            'datastream_navigation_link',
            'phenomenon_time',
            'result_time',
            'result',
            'result_quality',
            'valid_time',
            'parameters',
        ],
        "ObservationTravelTime": [
            'id',
            'self_link',
            'featuresofinterest_navigation_link',
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
        if '?' in full_path:
            # Split the query from the path
            path, query = full_path.split('?')

        # Parse the uri
        uri = STA2REST.parse_uri(path)

        if not uri:
            raise Exception("Error parsing uri")

        # Check if we have a query
        query_ast = QueryNode(None, None, None, None, None,
                              None, None, None, None, False)
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
                query_ast.filter = FilterNode(
                    query_ast.filter.filter + f" and {as_of_filter}" if query_ast.filter else as_of_filter)
            else:
                raise Exception(
                    "AS_OF function available only for single entity")
            # if query_ast.expand:
            #     for identifier in query_ast.expand.identifiers:
            #         identifier.identifier = identifier.identifier + "TravelTime"
            #         identifier.subquery = QueryNode(None, None, None, None, None, None, None, None, None, True) if identifier.subquery is None else identifier.subquery
            #         identifier.subquery.filter = FilterNode(identifier.subquery.filter + f" and {as_of_filter}" if identifier.subquery.filter else as_of_filter)

        if query_ast.from_to:
            if len(entities) == 0 and not query_ast.expand:
                main_entity += "TravelTime"
                from_to_filter = f"system_time_validity eq ({query_ast.from_to.value1}, {query_ast.from_to.value2})"
                query_ast.filter = FilterNode(
                    query_ast.filter.filter + f" and {from_to_filter}" if query_ast.filter else from_to_filter)
            else:
                raise Exception(
                    "FROM_TO function available only for single entity")

        print(f"Main entity: {main_entity}")

        if entities:
            if not query_ast.expand:
                query_ast.expand = ExpandNode([])

            index = 0

            # Merge the entities with the query
            for entity in entities:
                entity_name = entity[0]
                sub_query = QueryNode(
                    None, None, None, None, None, None, None, None, None, True)
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
                            query_ast.select.identifiers.append(
                                IdentifierNode(uri['property_name']))

                query_ast.expand.identifiers.append(
                    ExpandNodeIdentifier(entity_name, sub_query, False))
                index += 1
        else:
            if uri['property_name']:
                if not query_ast.select:
                    query_ast.select = SelectNode([])
                query_ast.select.identifiers.append(
                    IdentifierNode(uri['property_name']))

        # Check if we have a filter in the query
        if main_entity_id:
            query_ast.filter = FilterNode(
                query_ast.filter.filter + f" and id eq {main_entity_id}" if query_ast.filter else f"id eq {main_entity_id}")

            if not entities:
                single_result = True

        if uri['single']:
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
        visitor = NodeVisitor(main_entity, db)
        query_converted = visitor.visit(query_ast)

        return {
            'query': db.execute(query_converted[0]).all(),
            'count_query': query_converted[1],
            'query_count': query_converted[2].scalar(),
            'ref': uri['ref'],
            'value': uri['value'],
            'single_result': single_result,
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

        if (parts[-1] == "$ref"):
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
                    raise Exception(
                        "Error parsing uri: $ref after property name")
                ref = True
            elif entity == "$value":
                if property_name:
                    value = True
                else:
                    raise Exception(
                        "Error parsing uri: $value without property name")
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
