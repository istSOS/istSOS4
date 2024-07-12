import os
from app.sta2rest import sta2rest
from geoalchemy2 import Geometry
from odata_query.grammar import ODataLexer, ODataParser
from sqlalchemy import asc, case, desc, func, literal, select, text
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.sqltypes import String, Text

from ..models import *
from ..models.database import engine
from .filter_visitor import FilterVisitor
from .sta_parser.ast import *
from .sta_parser.visitor import Visitor


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
        node.name = node.name.replace("/", ".")
        for old_key, new_key in sta2rest.STA2REST.SELECT_MAPPING.items():
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
        for old_key, new_key in sta2rest.STA2REST.SELECT_MAPPING.items():
            if old_key == node.identifier:
                node.identifier = new_key
        # Convert the identifier to the format name.order
        return f"{node.identifier}.{node.order}"

    def visit_OrderByNode(self, node: OrderByNode):
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
        attributes = []
        orders = []
        for identifier in identifiers:
            attribute_name, *_, order = identifier.split(".")
            attributes.append(
                [getattr(globals()[self.main_entity], attribute_name)]
            )
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

        expand_queries = []

        # Process each identifier in the expand node
        for expand_identifier in node.identifiers:
            # Convert the table name
            expand_identifier.identifier = sta2rest.STA2REST.convert_entity(
                expand_identifier.identifier
            )
            sub_entity = globals()[expand_identifier.identifier]
            sub_query = None
            sub_query_ranked = None

            # Prepare select fields
            select_fields = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.select
            ):
                identifiers = [
                    self.visit(identifier)
                    for identifier in expand_identifier.subquery.select.identifiers
                ]
            else:
                identifiers = sta2rest.STA2REST.get_default_column_names(
                    expand_identifier.identifier
                )
                identifiers = [
                    item
                    for item in identifiers
                    if "navigation_link" not in item
                ]

            for field in identifiers:
                select_fields.append(getattr(sub_entity, field))

            relationship = None
            fk_parent = None
            select_from = False

            if parent:
                relationship = getattr(
                    globals()[parent], expand_identifier.identifier.lower()
                ).property
                if relationship.direction.name == "ONETOMANY":
                    fk_parent = getattr(sub_entity, f"{parent.lower()}_id")
                elif relationship.direction.name == "MANYTOMANY":
                    fk_parent = relationship.secondary.c[
                        f"{parent.lower()}_id"
                    ]
                    select_from = True

                if fk_parent is not None:
                    select_fields.insert(0, fk_parent)

            fk_child_arr = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.expand
            ):
                for e in expand_identifier.subquery.expand.identifiers:
                    identifier = sta2rest.STA2REST.ENTITY_MAPPING.get(
                        e.identifier, e.identifier
                    )
                    if hasattr(
                        globals()[expand_identifier.identifier],
                        identifier.lower(),
                    ):
                        relationship_nested = getattr(
                            globals()[expand_identifier.identifier],
                            identifier.lower(),
                        ).property
                        if relationship_nested.direction.name == "MANYTOONE":
                            fk = getattr(
                                sub_entity, f"{identifier.lower()}_id"
                            )
                            if fk not in select_fields:
                                select_fields.insert(0, fk)
                                fk_child_arr.append(fk)

            # Build sub-query with row number
            sub_query = select(
                *select_fields,
                func.row_number()
                .over(
                    partition_by=(
                        getattr(sub_entity, "id")
                        if fk_parent is None
                        else fk_parent
                    ),
                    order_by=getattr(sub_entity, "id"),
                )
                .label("rank"),
            )

            # Process filter clause if exists
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.filter
            ):
                filter, join_relationships = self.visit_FilterNode(
                    expand_identifier.subquery.filter,
                    expand_identifier.identifier,
                )
                for rel in join_relationships:
                    sub_query = sub_query.join(rel)
                sub_query = sub_query.filter(filter)

            # Process orderby clause
            ordering = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.orderby
            ):
                identifiers = [
                    self.visit(identifier)
                    for identifier in expand_identifier.subquery.orderby.identifiers
                ]
                for field in identifiers:
                    attr, order = field.split(".")
                    collation = (
                        "C" if isinstance(attr, (String, Text)) else None
                    )
                    if order == "asc":
                        ordering.append(
                            asc(attr.collate(collation))
                            if collation
                            else asc(attr)
                        )
                    else:
                        ordering.append(
                            desc(attr.collate(collation))
                            if collation
                            else desc(attr)
                        )
            else:
                ordering = [asc(getattr(sub_entity, "id"))]
            sub_query = sub_query.order_by(*ordering)

            # Process skip clause
            skip_value = (
                expand_identifier.subquery.skip.count
                if expand_identifier.subquery
                and expand_identifier.subquery.skip
                else 0
            )

            # Process top clause
            top_value = (
                expand_identifier.subquery.top.count
                if expand_identifier.subquery
                and expand_identifier.subquery.top
                else int(os.getenv("TOP_VALUE", 100))
            )

            if select_from:
                sub_query = sub_query.select_from(
                    relationship.secondary.outerjoin(sub_entity)
                )

            sub_query.alias(
                f"sub_query_{expand_identifier.identifier.lower()}"
            )

            # Build the ranked subquery
            sub_query_ranked = (
                select(
                    *[col for col in sub_query.columns if col.name != "rank"]
                )
                .filter(
                    sub_query.c.rank > skip_value,
                    sub_query.c.rank <= (top_value + skip_value),
                )
                .alias(
                    f"subquery_ranked_{expand_identifier.identifier.lower()}"
                )
            )

            # Construct JSON object arguments
            json_build_object_args = []
            fk_child_names = [fk.name for fk in fk_child_arr]
            for attr in sub_query_ranked.columns:
                if (str(attr.name) not in fk_child_names) and (
                    fk_parent is None
                    or (fk_parent is not None and attr.name != fk_parent.name)
                ):
                    json_build_object_args.append(
                        literal(attr.name, type_=String())
                        if attr.name != "id"
                        else text("'@iot.id'")
                    )
                    if isinstance(attr.type, Geometry):
                        json_build_object_args.append(
                            func.ST_AsGeoJSON(attr).cast(JSONB)
                        )
                    elif isinstance(attr.type, TSTZRANGE):
                        json_build_object_args.append(
                            case(
                                (
                                    func.lower(attr).isnot(None)
                                    & func.upper(attr).isnot(None),
                                    func.concat(
                                        func.lower(attr),
                                        "/",
                                        func.upper(attr),
                                    ),
                                ),
                                else_=None,
                            )
                        )
                    else:
                        json_build_object_args.append(attr)

            aggregation_type = (
                func.array_agg(
                    func.json_build_object(*json_build_object_args)
                )[1]
                if relationship.direction.name == "MANYTOONE"
                else func.json_agg(
                    func.json_build_object(*json_build_object_args)
                )
            )

            # Build sub-query JSON aggregation
            if relationship.direction.name in ["MANYTOONE", "ONETOMANY"]:
                select_from_clause = sub_query_ranked
            else:
                select_from_clause = sub_query_ranked.outerjoin(
                    relationship.secondary
                )

            sub_query_json_agg = (
                select(
                    (
                        sub_query_ranked.c[fk_parent.name]
                        if fk_parent is not None
                        else sub_query_ranked.c.id
                    ),
                    aggregation_type.label(
                        expand_identifier.identifier.lower()
                    ),
                )
                .select_from(select_from_clause)
                .group_by(
                    sub_query_ranked.c[fk_parent.name]
                    if fk_parent is not None
                    else sub_query_ranked.c.id
                )
                .alias(
                    f"sub_query_json_agg_{expand_identifier.identifier.lower()}"
                )
            )

            # Handle nested expand
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.expand
            ):
                nested_expand_queries = self.visit_ExpandNode(
                    expand_identifier.subquery.expand,
                    expand_identifier.identifier,
                )
                select_from_clause = sub_query_ranked
                for (
                    nested_expand_query,
                    nested_identifier,
                ) in nested_expand_queries:
                    value = getattr(
                        sub_entity,
                        f"{nested_identifier.lower()}_navigation_link",
                    )
                    json_build_object_args.append(
                        f"{value.name.split('@')[0]}"
                    )
                    relationship_nested = getattr(
                        globals()[expand_identifier.identifier],
                        nested_identifier.lower(),
                    ).property
                    coalesce_text = (
                        "'{}'"
                        if relationship_nested.direction.name == "MANYTOONE"
                        else "'[]'"
                    )
                    json_build_object_args.append(
                        func.coalesce(
                            nested_expand_query.columns[
                                nested_identifier.lower()
                            ],
                            text(coalesce_text),
                        )
                    )

                    aggregation_type = (
                        func.array_agg(
                            func.json_build_object(*json_build_object_args)
                        )[1]
                        if relationship.direction.name == "MANYTOONE"
                        else func.json_agg(
                            func.json_build_object(*json_build_object_args)
                        )
                    )
                    select_from_clause = select_from_clause.outerjoin(
                        nested_expand_query
                    )

                sub_query_json_agg = (
                    select(
                        (
                            sub_query_ranked.c[fk_parent.name]
                            if fk_parent is not None
                            else sub_query_ranked.c.id
                        ),
                        aggregation_type.label(
                            expand_identifier.identifier.lower()
                        ),
                    )
                    .select_from(select_from_clause)
                    .group_by(
                        sub_query_ranked.c[fk_parent.name]
                        if fk_parent is not None
                        else sub_query_ranked.c.id
                    )
                    .alias(
                        f"sub_query_json_agg_{expand_identifier.identifier.lower()}"
                    )
                )

            expand_queries.append(
                (sub_query_json_agg, expand_identifier.identifier)
            )

        return expand_queries

    async def visit_QueryNode(self, node: QueryNode):
        """
        Visit a query node.

        Args:
            node (ast.QueryNode): The query node to visit.

        Returns:
            str: The converted query node.
        """

        # list to store the converted parts of the query node

        result_format = "DataArray" if node.result_format and node.result_format.value == "dataArray" else None


        async with self.db as session:
            main_entity = globals()[self.main_entity]
            main_query = None
            if int(os.getenv("ESTIMATE_COUNT", 0)):
                query_count = (
                    select(getattr(main_entity, "id").distinct())
                    if "TravelTime" not in self.main_entity
                    else select(
                        func.distinct(
                            getattr(main_entity, "id"),
                            getattr(main_entity, "system_time_validity"),
                        )
                    )
                )
            else:
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

            if not node.select:
                node.select = SelectNode([])
                # get default columns for main entity
                default_columns = sta2rest.STA2REST.get_default_column_names(
                    self.main_entity if not result_format else self.main_entity + result_format
                )
                for column in default_columns:
                    node.select.identifiers.append(IdentifierNode(column))

            # Check if we have a select, filter, orderby, skip, top or count in the query
            if node.select:
                select_query = []

                # Iterate over fields in node.select
                for field in self.visit(node.select):
                    field_name = field.split(".")[-1]
                    select_query.append(getattr(main_entity, field_name))

            components = [sta2rest.STA2REST.REVERSE_SELECT_MAPPING.get(identifier.name, identifier.name) for identifier in node.select.identifiers]

            json_build_object_args = []
            for attr in select_query:
                if not node.result_format:
                    (
                        json_build_object_args.append(
                            literal(attr.name, type_=String())
                        )
                        if attr.name != "id"
                        else json_build_object_args.append(text("'@iot.id'"))
                    )
                if isinstance(attr.type, Geometry):
                    json_build_object_args.append(
                        func.ST_AsGeoJSON(attr).cast(JSONB)
                    )
                elif isinstance(attr.type, TSTZRANGE):
                    json_build_object_args.append(
                        case(
                            (
                                func.lower(attr).isnot(None)
                                & func.upper(attr).isnot(None),
                                func.concat(
                                    func.lower(attr),
                                    "/",
                                    func.upper(attr),
                                ),
                            ),
                            else_=None,
                        )
                    )
                else:
                    json_build_object_args.append(attr)

            # Check if we have an expand node before the other parts of the query
            if node.expand:
                expand_identifiers_path = {
                    "expand": {
                        "identifiers": [
                            e for e in node.expand.identifiers if not e.expand
                        ]
                    }
                }
                node.expand.identifiers = [
                    e for e in node.expand.identifiers if e.expand
                ]

                sub_queries_no_expand = []
                if expand_identifiers_path["expand"]["identifiers"]:
                    if node.result_format and node.result_format.value == "dataArray":
                    
                        select_query.append(getattr(main_entity, "datastream_id"))

                        top_value = self.visit(node.top) if node.top else 100
                        skip_value = self.visit(node.skip) if node.skip else 0

                        sub_query = select(
                            *select_query,
                            func.row_number()
                            .over(
                                partition_by=(
                                    getattr(main_entity, "datastream_id")
                                ),
                                order_by=getattr(main_entity, "id"),
                            )
                            .label("rank"),
                        )

                        sub_query_ranked = (
                            select(
                                *[col for col in sub_query.columns if col.name != "rank"]
                            )
                            .filter(
                                sub_query.c.rank > skip_value,
                                sub_query.c.rank <= (top_value + skip_value),
                            )
                        )

                        main_query = select(
                            func.json_build_object(
                                "Datastream@iot.navigationLink",
                                func.concat(
                                    os.getenv('HOSTNAME', ''),
                                    os.getenv('SUBPATH', ''),
                                    os.getenv('VERSION', ''),
                                    '/Datastreams(', 
                                    sub_query_ranked.columns.datastream_id,
                                    ')'
                                ),
                                'components',
                                components,
                                'dataArray@iot.count',
                                func.count(),
                                'dataArray',
                                func.json_agg(
                                    func.json_build_array(*sub_query_ranked.columns[:-1])
                                )
                            )
                        ).group_by("datastream_id")
                    else:
                        main_query = select(
                            func.json_build_object(*json_build_object_args)
                        )
                    current = None
                    previous = None

                    for i, e in enumerate(
                        expand_identifiers_path["expand"]["identifiers"]
                    ):
                        current = e.identifier
                        sub_query = select(globals()[current])

                        if i > 0:
                            previous = expand_identifiers_path["expand"][
                                "identifiers"
                            ][i - 1].identifier
                            relationship = getattr(
                                globals()[current], previous.lower()
                            ).property.direction.name
                            sub_query = (
                                sub_query.join(
                                    getattr(
                                        globals()[current], previous.lower()
                                    )
                                ).join(sub_queries_no_expand[i - 1])
                                if relationship == "MANYTOMANY"
                                else sub_query.join(
                                    sub_queries_no_expand[i - 1]
                                )
                            )

                        if e.subquery and e.subquery.filter:
                            filter, join_relationships = self.visit_FilterNode(
                                e.subquery.filter, current
                            )
                            sub_query = sub_query.filter(filter)

                        sub_queries_no_expand.append(sub_query.subquery())

                    if sub_queries_no_expand and not node.expand.identifiers:
                        relationship = getattr(
                            main_entity, current.lower()
                        ).property.direction.name
                        main_query = (
                            main_query.join(
                                getattr(main_entity, current.lower())
                            ).join(sub_queries_no_expand[-1])
                            if relationship == "MANYTOMANY"
                            else main_query.join(sub_queries_no_expand[-1])
                        )
                        query_count = query_count.join(
                            getattr(main_entity, current.lower())
                        ).join(sub_queries_no_expand[-1])

                if node.expand.identifiers:
                    # Visit the expand node
                    sub_queries = self.visit_ExpandNode(
                        node.expand, self.main_entity
                    )
                    for sub_query, alias in sub_queries:
                        value = getattr(
                            main_entity, f"{alias.lower()}_navigation_link"
                        )
                        json_build_object_args.append(
                            f"{value.name.split('@')[0]}"
                        )

                        # Determine the JSON structure based on the relationship type
                        relationship = getattr(
                            main_entity, alias.lower()
                        ).property.direction.name
                        coalesce_text = (
                            "'{}'" if relationship == "MANYTOONE" else "'[]'"
                        )
                        json_build_object_args.append(
                            func.coalesce(
                                sub_query.columns[alias.lower()],
                                text(coalesce_text),
                            )
                        )

                    # Build the main query
                    main_query = select(
                        func.json_build_object(*json_build_object_args)
                    )
                    if sub_queries_no_expand:
                        relationship = getattr(
                            main_entity, current.lower()
                        ).property.direction.name
                        main_query = (
                            main_query.join(
                                getattr(main_entity, current.lower())
                            ).join(sub_queries_no_expand[-1])
                            if relationship == "MANYTOMANY"
                            else main_query.join(sub_queries_no_expand[-1])
                        )
                        query_count = query_count.join(
                            getattr(main_entity, current.lower())
                        ).join(sub_queries_no_expand[-1])

                    # Reverse the sub_queries order for specific case
                    if (
                        self.main_entity == "Location"
                        and sub_queries[0][1] == "HistoricalLocation"
                    ):
                        sub_queries.reverse()

                    # Join the main query with subqueries
                    fk_main_entity = f"{self.main_entity.lower()}_id"
                    main_entity_id = getattr(main_entity, "id")

                    for sub_query, alias in sub_queries:
                        relationship_type = getattr(
                            main_entity, alias.lower()
                        ).property.direction.name

                        # Determine join condition based on relationship type
                        join_condition = (
                            main_entity_id == sub_query.c[fk_main_entity]
                            if relationship_type in ["MANYTOMANY", "ONETOMANY"]
                            else getattr(main_entity, f"{alias.lower()}_id")
                            == sub_query.c.id
                        )

                        main_query = main_query.outerjoin(
                            sub_query, join_condition
                        )
            else:
                # Set options for main_query if select_query is not empty
                if node.result_format and node.result_format.value == "dataArray":
                    select_query.append(getattr(main_entity, "datastream_id"))

                    top_value = 1

                    sub_query = select(
                        *select_query,
                        func.row_number()
                        .over(
                            partition_by=(
                                getattr(main_entity, "datastream_id")
                            ),
                            order_by=getattr(main_entity, "id"),
                        )
                        .label("rank"),
                    )

                    sub_query_ranked = (
                        select(
                            *[col for col in sub_query.columns if col.name != "rank"]
                        )
                        .filter(
                            sub_query.c.rank <= (top_value),
                        )
                    )

                    main_query = select(
                        func.json_build_object(
                            "Datastream@iot.navigationLink",
                            func.concat(
                                os.getenv('HOSTNAME', ''),
                                os.getenv('SUBPATH', ''),
                                os.getenv('VERSION', ''),
                                '/Datastreams(', 
                                sub_query_ranked.columns.datastream_id,
                                ')'
                            ),
                            "components",
                            components,
                            "dataArray@iot.count",
                            func.count(sub_query_ranked.columns.datastream_id),
                            'dataArray',
                            func.json_agg(
                                func.json_build_array(*sub_query_ranked.columns[:-1])
                            )  
                        )
                    ).group_by("datastream_id")
                else:
                    main_query = select(
                        func.json_build_object(*json_build_object_args)
                    )

            if node.filter:
                filter, join_relationships = self.visit_FilterNode(
                    node.filter, self.main_entity
                )
                for rel in join_relationships:
                    main_query = main_query.join(rel)
                main_query = main_query.filter(filter)
                query_count = query_count.filter(filter)

            ordering = []
            if node.orderby:
                attrs, orders = self.visit(node.orderby)
                for attr, order in zip(attrs, orders):
                    for a in attr:
                        collation = (
                            "C" if isinstance(a.type, (String, Text)) else None
                        )
                        if order == "asc":
                            ordering.append(
                                asc(a.collate(collation))
                                if collation
                                else asc(a)
                            )
                        else:
                            ordering.append(
                                desc(a.collate(collation))
                                if collation
                                else desc(a)
                            )
            else:
                ordering = [asc(getattr(main_entity, "id"))]

            # Apply ordering to main_query
            if not (node.result_format and node.result_format.value == "dataArray"):
                # TODO: Fix ordering for dataArray format queries
                main_query = main_query.order_by(*ordering)

            # Determine skip and top values, defaulting to 0 and 100 respectively if not specified
            skip_value = self.visit(node.skip) if node.skip else 0
            top_value = (
                self.visit(node.top) + 1
                if node.top
                else int(os.getenv("TOP_VALUE", 100)) + 1
            )

            main_query = main_query.offset(skip_value).limit(top_value)

            if not node.count:
                count_query = False
            else:
                if node.count.value:
                    count_query = True

                else:
                    count_query = False

            main_query = await session.execute(main_query)
            main_query = main_query.scalars().all()
            if count_query:
                if int(os.getenv("ESTIMATE_COUNT", 0)):
                    compiled_query_text = str(
                        query_count.compile(
                            dialect=engine.dialect,
                            compile_kwargs={"literal_binds": True},
                        )
                    )
                    query_estimate_count_sql = text(
                        f"SELECT sensorthings.count_estimate(:compiled_query_text) as estimated_count"
                    )
                    query_count = await session.execute(
                        query_estimate_count_sql,
                        {"query_text": compiled_query_text},
                    )
                    query_count = query_count.scalar()
                else:
                    query_count = await session.execute(query_count)
                    query_count = query_count.scalar()

        return main_query, count_query, query_count
