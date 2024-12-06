import json

from app import (
    COUNT_ESTIMATE_THRESHOLD,
    COUNT_MODE,
    HOSTNAME,
    REDIS,
    SUBPATH,
    TOP_VALUE,
    VERSION,
    VERSIONING,
)
from app.db.redis_db import redis
from app.db.sqlalchemy_db import engine
from app.models import *
from app.sta2rest import sta2rest
from geoalchemy2 import Geometry
from odata_query.grammar import ODataLexer, ODataParser
from sqlalchemy import (
    asc,
    case,
    desc,
    func,
    literal,
    literal_column,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .filter_visitor import FilterVisitor
from .sta_parser.ast import *
from .sta_parser.visitor import Visitor


class NodeVisitor(Visitor):
    """
    Initialize the visitor with the given parameters.

    Args:
        main_entity: The main entity to be processed.
        full_path: The full path to the entity.
        ref: Flag indicating if the entity is a reference. Defaults to False.
        value: Flag indicating if the entity has a value. Defaults to False.
        single_result: Flag indicating if only a single result is expected. Defaults to False.
        entities: Additional entities to be processed.
    """

    def __init__(
        self,
        main_entity=None,
        full_path=None,
        ref=False,
        value=False,
        single_result=False,
        entities=None,
    ):
        super().__init__()
        self.main_entity = main_entity
        self.full_path = full_path
        self.ref = ref
        self.value = value
        self.single_result = single_result
        self.entities = entities

    """
    This class provides a visitor to convert a STA query to a SQLAlchemy query.
    """

    def visit_IdentifierNode(self, node: IdentifierNode):
        """
        Visit an identifier node.

        Args:
            node (ast.IdentifierNode): The identifier node to visit.

        Returns:
            str: The converted identifier.
        """

        prefix, *suffix = node.name.split("/", maxsplit=1)
        converted_prefix = sta2rest.STA2REST.SELECT_MAPPING.get(prefix, prefix)
        node.name = (
            f"{converted_prefix}/{suffix[0]}" if suffix else converted_prefix
        )
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
            f"{self.main_entity}.{self.visit(identifier)}"
            for identifier in node.identifiers
        ]
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
        ast = ODataParser().parse(ODataLexer().tokenize(node.filter))
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

        node.identifier = sta2rest.STA2REST.SELECT_MAPPING.get(
            node.identifier, node.identifier
        )
        # Convert the identifier to the format name.order
        return f"{node.identifier}.{node.order}"

    def visit_OrderByNode(self, node: OrderByNode, entity: str):
        """
        Visit an orderby node.

        Args:
            node (ast.OrderByNode): The orderby node to visit.

        Returns:
            str: The converted orderby node.
        """

        identifiers = [
            self.visit(identifier) for identifier in node.identifiers
        ]
        attributes, orders = [], []
        for identifier in identifiers:
            attribute_name, *_, order = identifier.split(".")
            attributes.append([getattr(globals()[entity], attribute_name)])
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
        return bool(node.value)

    def visit_ExpandNode(self, node: ExpandNode, parent=None):
        """
        Visit an expand node.

        Args:
            node (ExpandNode): The expand node to visit.
            parent (str): The parent entity name.

        Returns:
            list: A list of tuples containing the sub-query and the identifier of each expand node.
        """

        expand_queries = []

        # Process each identifier in the expand node
        for expand_identifier in node.identifiers:
            relationship_entity = None
            fk_parent = None
            expand_identifier.identifier = sta2rest.STA2REST.convert_entity(
                expand_identifier.identifier
            )
            sub_entity = globals()[expand_identifier.identifier]
            sub_query = None
            show_id = False

            # Process select clause if exists
            select_fields = []
            # Check for identifiers in the subquery's select or use default columns
            identifiers = (
                [
                    self.visit(identifier)
                    for identifier in expand_identifier.subquery.select.identifiers
                ]
                if expand_identifier.subquery
                and expand_identifier.subquery.select
                else [
                    identifier
                    for identifier in sta2rest.STA2REST.get_default_column_names(
                        expand_identifier.identifier
                    )
                    if "navigation_link"
                    not in identifier  # Exclude navigation links for sub-entities
                ]
            )

            show_id = "id" not in identifiers
            if show_id:
                identifiers.append("id")

            for identifier in identifiers:
                select_fields.append(
                    get_select_attr(
                        getattr(sub_entity, identifier),
                        getattr(sub_entity, identifier).name,
                        nested=True,
                    )
                )

            if parent:
                # Determine the relationship entity for the parent class and identifier
                parent_class = globals()[parent.replace("TravelTime", "")]
                attribute_name = expand_identifier.identifier.replace(
                    "TravelTime", ""
                ).lower()
                relationship_entity = getattr(
                    parent_class, attribute_name
                ).property

                # Check for ONETOMANY relationship and get the foreign key attribute
                if relationship_entity.direction.name == "ONETOMANY":
                    fk_attr = f"{parent.replace('TravelTime', '').lower()}_id"
                    fk_parent = getattr(sub_entity, fk_attr)

            # Process orderby clause if exists
            ordering = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.orderby
            ):
                attrs, orders = self.visit_OrderByNode(
                    expand_identifier.subquery.orderby,
                    expand_identifier.identifier,
                )
                ordering = get_orderby_attr(attrs, orders)
            else:
                ordering = [asc(getattr(sub_entity, "id"))]

            # Process skip clause  if exists
            skip_value = (
                self.visit_SkipNode(expand_identifier.subquery.skip)
                if expand_identifier.subquery
                and expand_identifier.subquery.skip
                else 0
            )

            # Process top clause  if exists
            top_value = (
                self.visit_TopNode(expand_identifier.subquery.top) + 1
                if expand_identifier.subquery
                and expand_identifier.subquery.top
                else TOP_VALUE + 1
            )

            # Process count clause  if exists
            is_count = (
                self.visit_CountNode(expand_identifier.subquery.count)
                if expand_identifier.subquery
                and expand_identifier.subquery.count
                else False
            )

            # Handle nested expand  if exists
            labels = {}
            json_expands = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.expand
            ):
                nested_expand_queries = self.visit_ExpandNode(
                    expand_identifier.subquery.expand,
                    expand_identifier.identifier,
                )
                for nested_expand_query in nested_expand_queries:
                    entity = nested_expand_query[2].replace("TravelTime", "")
                    attribute_sub_entity = (
                        nested_expand_query[5]
                        .replace("TravelTime", "")
                        .lower()
                    )
                    relationship_nested = getattr(
                        globals()[entity],
                        attribute_sub_entity,
                    ).property

                    navigation_link_attr = (
                        f"{attribute_sub_entity}_navigation_link"
                    )

                    navigation_link_value = getattr(
                        sub_entity, navigation_link_attr
                    )
                    label_name = navigation_link_value.name.split("@")[0]
                    if relationship_nested.direction.name != "MANYTOONE":
                        labels[label_name] = nested_expand_query[8]

                    json_expands.append(
                        get_expand_function(
                            relationship_nested,
                            get_query_compiled(nested_expand_query[0]),
                            nested_expand_query,
                            label_name,
                        )
                    )

            if fk_parent is not None:
                select_fields.append(fk_parent)

            # Combine select fields and json expands
            query_fields = (
                select_fields + json_expands if json_expands else select_fields
            )

            sub_query = select(*query_fields).order_by(*ordering)

            # Process filter clause if exists
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.filter
            ):
                filter, join_relationships = self.visit_FilterNode(
                    expand_identifier.subquery.filter,
                    expand_identifier.identifier,
                )
                sub_query = sub_query.filter(filter)
                if join_relationships:
                    for relationship in join_relationships:
                        sub_query = sub_query.join(relationship)

            columns_to_select = []
            for column in sub_query.columns:
                if column.name not in labels:
                    columns_to_select.append(column)
                else:
                    columns_to_select.append(
                        column.op("->")(column.name).label(column.name)
                    )
                    columns_to_select.append(
                        column.op("->")(column.name + "@iot.nextLink").label(
                            column.name + "@iot.nextLink"
                        )
                    )
                    if labels[column.name]:
                        columns_to_select.append(
                            column.op("->")(column.name + "@iot.count").label(
                                column.name + "@iot.count"
                            )
                        )

            if columns_to_select is not None:
                sub_query = select(*columns_to_select).select_from(sub_query)

            expand_queries.append(
                [
                    sub_query,
                    fk_parent,
                    parent,
                    skip_value,
                    top_value,
                    expand_identifier.identifier,
                    (
                        True
                        if relationship_entity.direction.name == "MANYTOMANY"
                        else False
                    ),
                    show_id,
                    is_count,
                ]
            )
        return expand_queries

    def visit_QueryNode(self, node: QueryNode):
        """
        Visit a query node.

        Args:
            node (ast.QueryNode): The query node to visit.

        Returns:
            str: The converted query node.
        """

        result_format = (
            "DataArray"
            if node.result_format and node.result_format.value == "dataArray"
            else None
        )

        main_entity = globals()[self.main_entity]
        main_query = None
        query_count = (
            select(func.count(getattr(main_entity, "id").distinct()))
            if "TravelTime" not in self.main_entity
            else select(
                func.count(
                    func.distinct(
                        getattr(main_entity, "id"),
                        getattr(main_entity, "system_time_validity"),
                    )
                )
            )
        )

        query_estimate_count = (
            select(getattr(main_entity, "id").distinct())
            if "TravelTime" not in self.main_entity
            else select(
                func.distinct(
                    getattr(main_entity, "id"),
                    getattr(main_entity, "system_time_validity"),
                )
            )
        )

        if self.ref:
            node.select = SelectNode([])
            node.select.identifiers.append(IdentifierNode("self_link"))

        # Process select clause if exists
        if not node.select:
            node.select = SelectNode([])
            default_columns = sta2rest.STA2REST.get_default_column_names(
                self.main_entity
                if not result_format
                else self.main_entity + result_format
            )
            for column in default_columns:
                node.select.identifiers.append(IdentifierNode(column))

        if node.select:
            select_query = []

            # Iterate over fields in node.select when fields have a nested path
            for field in self.visit(node.select):
                field_name = field.split(".", 1)[-1]
                if "/" in field_name:
                    field, *field_parts = field_name.split("/", 1)
                    field_parts = (
                        field_parts[0].split("/") if field_parts else []
                    )
                    json_path = getattr(main_entity, field)
                    for part in field_parts:
                        json_path = json_path.op("->")(part)
                    select_query.append(json_path)
                else:
                    select_query.append(getattr(main_entity, field_name))

        components = [
            sta2rest.STA2REST.REVERSE_SELECT_MAPPING.get(
                identifier.name, identifier.name
            )
            for identifier in node.select.identifiers
        ]

        select_args = []
        for attr in select_query:
            name = (
                attr.name
                if isinstance(attr, InstrumentedAttribute)
                else attr.right.value
            )
            select_args.append(get_select_attr(attr, name, as_of=node.as_of))

        joins = []
        filters = []
        labels = {}
        # Check if we have an expand node before the other parts of the query
        if node.expand:
            # Extract identifiers that act as filters
            filter_identifiers_path = {
                "expand": {
                    "identifiers": [
                        e for e in node.expand.identifiers if not e.expand
                    ]
                }
            }
            # Update node.expand.identifiers to keep only those with a nested expand
            node.expand.identifiers = [
                e for e in node.expand.identifiers if e.expand
            ]

            if filter_identifiers_path["expand"]["identifiers"]:
                identifiers = filter_identifiers_path["expand"]["identifiers"]

                main_query = select(*select_args)

                for i, current_identifier in enumerate(identifiers):
                    if (
                        current_identifier.subquery
                        and current_identifier.subquery.filter
                    ):
                        filter_clause, join_relationships = (
                            self.visit_FilterNode(
                                current_identifier.subquery.filter,
                                current_identifier.identifier,
                            )
                        )
                        filters.append(filter_clause)
                        main_query = main_query.filter(filter_clause)
                        query_count = query_count.filter(filter_clause)
                        query_estimate_count = query_estimate_count.filter(
                            filter_clause
                        )
                        if join_relationships:
                            for relationship in join_relationships:
                                joins.append(relationship)
                                main_query = main_query.join(relationship)
                                query_count = query_count.join(relationship)
                                query_estimate_count = (
                                    query_estimate_count.join(relationship)
                                )

                    # Determine current and nested identifiers
                    identifier = (
                        current_identifier.identifier
                        if i > 0
                        else self.main_entity
                    )
                    nested_identifier = (
                        identifiers[i - 1].identifier
                        if i > 0
                        else current_identifier.identifier
                    )

                    # Retrieve the relationship property for the current identifiers
                    entity_class = globals()[
                        identifier.replace("TravelTime", "")
                    ]
                    nested_entity_class = globals()[nested_identifier]
                    relationship = getattr(
                        entity_class,
                        nested_identifier.replace("TravelTime", "").lower(),
                    ).property

                    filter_condition = None
                    if relationship.direction.name == "MANYTOONE":
                        filter_condition = getattr(
                            globals()[identifier],
                            f"{nested_identifier.replace('TravelTime', '').lower()}_id",
                        ) == getattr(nested_entity_class, "id")
                    elif relationship.direction.name == "ONETOMANY":
                        filter_condition = getattr(
                            nested_entity_class,
                            f"{identifier.replace('TravelTime', '').lower()}_id",
                        ) == getattr(globals()[identifier], "id")
                    else:
                        filter_condition = getattr(
                            entity_class, "id"
                        ) == relationship.secondary.columns.get(
                            f"{identifier.replace('TravelTime', '').lower()}_id"
                        )
                        filters.append(filter_condition)
                        main_query = main_query.filter(filter_condition)
                        query_count = query_count.filter(filter_condition)
                        query_estimate_count = query_estimate_count.filter(
                            filter_condition
                        )
                        filter_condition = getattr(
                            nested_entity_class, "id"
                        ) == relationship.secondary.columns.get(
                            f"{nested_identifier.replace('TravelTime', '').lower()}_id"
                        )
                    filters.append(filter_condition)
                    main_query = main_query.filter(filter_condition)
                    query_count = query_count.filter(filter_condition)
                    query_estimate_count = query_estimate_count.filter(
                        filter_condition
                    )

            # here we create the sub queries for the expand identifiers
            if node.expand.identifiers:
                # Visit the expand node
                expand_queries = self.visit_ExpandNode(
                    node.expand, self.main_entity
                )
                for expand_query in expand_queries:
                    entity = self.main_entity.replace("TravelTime", "")
                    attribute_sub_entity = (
                        expand_query[5].replace("TravelTime", "").lower()
                    )
                    relationship = getattr(
                        globals()[entity],
                        attribute_sub_entity,
                    ).property

                    navigation_link_attr = (
                        f"{attribute_sub_entity}_navigation_link"
                    )

                    navigation_link_value = getattr(
                        main_entity, navigation_link_attr
                    )
                    label_name = navigation_link_value.name.split("@")[0]

                    if relationship.direction.name != "MANYTOONE":
                        labels[label_name] = expand_query[8]

                    select_args.append(
                        get_expand_function(
                            relationship,
                            get_query_compiled(expand_query[0]),
                            expand_query,
                            label_name,
                        )
                    )

                main_query = select(*select_args)

                if filters:
                    for filter in filters:
                        main_query = main_query.filter(filter)
                if joins:
                    for join in joins:
                        main_query = main_query.join(join)
        else:
            if result_format == "DataArray":
                select_args.append(
                    getattr(main_entity, "datastream_id").label(
                        "datastream_id"
                    )
                )
                select_args.append(
                    cast(components, ARRAY(String)).label("components")
                )

            main_query = select(*select_args)

            if result_format == "DataArray":
                main_query = main_query.order_by(
                    "datastream_id", getattr(main_entity, "id").desc()
                ).distinct("datastream_id")

        # Process filter clause if exists
        if node.filter:
            filter, join_relationships = self.visit_FilterNode(
                node.filter, self.main_entity
            )
            main_query = main_query.filter(filter)
            query_count = query_count.filter(filter)
            query_estimate_count = query_estimate_count.filter(filter)
            if join_relationships:
                for relationship in join_relationships:
                    main_query = main_query.join(relationship)
                    query_count = query_count.join(relationship)
                    query_estimate_count = query_estimate_count.join(
                        relationship
                    )

        # Process orderby clause if exists
        ordering = []
        if node.orderby:
            attrs, orders = self.visit_OrderByNode(
                node.orderby, self.main_entity
            )
            ordering = get_orderby_attr(attrs, orders)
        else:
            ordering = [asc(getattr(main_entity, "id"))]
            if VERSIONING and node.from_to:
                ordering.append(
                    asc(getattr(main_entity, "system_time_validity"))
                )

        main_query = main_query.order_by(*ordering)

        # Process skip clause  if exists
        skip_value = self.visit_SkipNode(node.skip) if node.skip else 0

        # Process top clause  if exists
        top_value = (
            self.visit_TopNode(node.top) + 1 if node.top else TOP_VALUE + 1
        )

        # Process count clause  if exists
        is_count = self.visit_CountNode(node.count) if node.count else False

        count_queries = []
        if is_count:
            if COUNT_MODE in {"LIMIT_ESTIMATE", "ESTIMATE_LIMIT"}:
                estimate_query_str = str(
                    query_estimate_count.compile(
                        dialect=engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
                limited_count_query_str = str(
                    select(func.count())
                    .select_from(
                        query_estimate_count.limit(COUNT_ESTIMATE_THRESHOLD)
                    )
                    .compile(
                        dialect=engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
                if COUNT_MODE == "LIMIT_ESTIMATE":
                    count_queries.append(limited_count_query_str)
                    count_queries.append(str(estimate_query_str))
                elif COUNT_MODE == "ESTIMATE_LIMIT":
                    count_queries.append(str(estimate_query_str))
                    count_queries.append(limited_count_query_str)
            else:
                count_queries.append(
                    str(
                        query_count.compile(
                            dialect=engine.dialect,
                            compile_kwargs={"literal_binds": True},
                        )
                    )
                )

        if result_format == "DataArray" and node.expand:
            if top_value > 1:
                top_value -= 1

        main_query = main_query.limit(top_value).offset(skip_value)
        columns_to_select = []
        for column in main_query.columns:
            if column.name not in labels:
                columns_to_select.append(column)
            else:
                columns_to_select.append(
                    column.op("->")(column.name).label(column.name)
                )
                columns_to_select.append(
                    column.op("->")(column.name + "@iot.nextLink").label(
                        column.name + "@iot.nextLink"
                    )
                )
                if labels[column.name]:
                    columns_to_select.append(
                        column.op("->")(column.name + "@iot.count").label(
                            column.name + "@iot.count"
                        )
                    )

        if columns_to_select is not None:
            main_query = (
                select(*columns_to_select)
                .select_from(main_query)
                .alias("main_query")
            )
        else:
            main_query = main_query.alias("main_query")

        if result_format == "DataArray":
            if not node.expand:
                main_query = select(
                    func.concat(
                        HOSTNAME,
                        SUBPATH,
                        VERSION,
                        "/Datastreams(",
                        main_query.c.datastream_id,
                        ")",
                    ).label("Datastream@iot.navigationLink"),
                    main_query.c.components,
                    literal("1").cast(Integer).label("dataArray@iot.count"),
                    func.json_build_array(*main_query.columns[:-2]).label(
                        "dataArray"
                    ),
                ).alias("main_query")
            else:

                entity_id = self.entities[0][1]

                main_query = select(
                    func.json_build_object(
                        "Datastream@iot.navigationLink",
                        func.concat(
                            HOSTNAME,
                            SUBPATH,
                            VERSION,
                            "/Datastreams(",
                            entity_id,
                            ")",
                        ),
                        "components",
                        cast(components, ARRAY(String)),
                        "dataArray@iot.count",
                        func.count(),
                        "dataArray",
                        func.json_agg(
                            func.json_build_array(*main_query.columns),
                        ),
                    ).label("json")
                ).alias("main_query")

        main_query = select(
            func.row_to_json(literal_column("main_query")).label("json")
        ).select_from(main_query)

        as_of_value = node.as_of.value if node.as_of else None

        from_to_value = True if node.from_to else False

        if self.value:
            value = None
            if isinstance(select_query[0], InstrumentedAttribute):
                value = select_query[0].name
            else:
                value = select_query[0].right
            main_query = select(
                main_query.c.json.op("->")(text(f"'{value}'"))
            ).select_from(main_query)

        main_query_str = str(
            main_query.compile(
                dialect=engine.dialect,
                compile_kwargs={"literal_binds": True},
            )
        )

        main_query = {
            "main_entity": self.main_entity,
            "main_query": main_query_str,
            "top_value": top_value,
            "is_count": is_count,
            "count_queries": count_queries,
            "as_of_value": as_of_value,
            "from_to_value": from_to_value,
            "single_result": self.single_result,
        }

        if REDIS:
            redis.set(self.full_path, json.dumps(main_query))

        return main_query


def get_select_attr(attr, label, nested=False, as_of=None):
    if isinstance(attr.type, Geometry):
        return func.ST_AsGeoJSON(attr).cast(JSONB).label(label)
    elif isinstance(attr.type, TSTZRANGE):
        lower_bound = func.lower(attr)
        upper_bound = func.upper(attr)
        if attr.name == "phenomenonTime" and attr.table.name == "Observation":
            return case(
                (
                    lower_bound == upper_bound,
                    func.to_char(lower_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                ),
                else_=func.to_char(lower_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                + "/"
                + func.to_char(upper_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            ).label(label)
        if VERSIONING and attr.name == "systemTimeValidity":
            return case(
                (
                    lower_bound == upper_bound,
                    func.to_char(lower_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                ),
                else_=func.to_char(lower_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                + "/"
                + case(
                    (
                        (upper_bound == "infinity"),
                        "infinity",
                    ),
                    else_=func.to_char(
                        upper_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    ),
                ),
            ).label(label)
        else:
            return case(
                (
                    attr.isnot(None),
                    func.to_char(lower_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    + "/"
                    + func.to_char(upper_bound, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                ),
                else_=None,
            ).label(label)
    elif isinstance(attr.type, TIMESTAMP):
        return func.to_char(attr, 'YYYY-MM-DD"T"HH24:MI:SS"Z"').label(label)
    elif "Link" in label:
        link_url = (
            HOSTNAME
            + SUBPATH
            + VERSION
            + attr
            + (
                f"?$as_of={as_of.value}"
                if as_of and label != "Commit@iot.navigationLink"
                else ""
            )
        )
        return link_url.label(label)
    return attr.label("@iot.id" if label == "id" and not nested else label)


def get_orderby_attr(attrs, orders):
    ordering = []
    for attr, order in zip(attrs, orders):
        for a in attr:
            collation = "C" if isinstance(a.type, (String, Text)) else None
            if order == "asc":
                ordering.append(
                    asc(a.collate(collation)) if collation else asc(a)
                )
            else:
                ordering.append(
                    desc(a.collate(collation)) if collation else desc(a)
                )
    return ordering


def get_query_compiled(query):
    return query.compile(
        dialect=engine.dialect,
        compile_kwargs={"literal_binds": True},
    )


def get_expand_function(relationship, compiled_query, sub_query, label):
    if relationship.direction.name != "MANYTOMANY":
        return expand_function(
            compiled_query,
            sub_query,
            label,
        )
    return expand_many2many_function(
        compiled_query,
        sub_query,
        label,
        relationship,
    )


def expand_function(compiled_query, sub_query, label_name):
    return func.sensorthings.expand(
        str(compiled_query),
        ("{}".format(sub_query[1].name) if sub_query[1] is not None else "id"),
        (
            text(
                '"{}".id::integer'.format(
                    globals()[sub_query[2]].__tablename__
                )
            )
            if sub_query[1] is not None
            else text(
                '"{}".{}_id::integer'.format(
                    globals()[sub_query[2]].__tablename__,
                    sub_query[5].replace("TravelTime", ""),
                )
            )
        ),
        sub_query[4],
        sub_query[3],
        (True if sub_query[1] is not None else False),
        sub_query[7],
        label_name,
        HOSTNAME
        + SUBPATH
        + VERSION
        + getattr(globals()[sub_query[2]], "self_link")
        + "/",
        sub_query[8],
        COUNT_MODE,
        COUNT_ESTIMATE_THRESHOLD,
    ).label(label_name)


def expand_many2many_function(
    compiled_query, sub_query, label_name, relationship
):
    return func.sensorthings.expand_many2many(
        str(compiled_query),
        f'{relationship.secondary.schema}."{relationship.secondary.name}"',
        text('"{}".id::integer'.format(globals()[sub_query[2]].__tablename__)),
        "{}_id".format(sub_query[5].replace("TravelTime", "")),
        "{}_id".format(sub_query[2].replace("TravelTime", "")),
        sub_query[4],
        sub_query[3],
        sub_query[7],
        label_name,
        HOSTNAME
        + SUBPATH
        + VERSION
        + getattr(globals()[sub_query[2]], "self_link")
        + "/",
        sub_query[8],
        COUNT_MODE,
        COUNT_ESTIMATE_THRESHOLD,
    ).label(label_name)
