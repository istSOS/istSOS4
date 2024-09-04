from datetime import datetime

import ujson
from app import (
    COUNT_ESTIMATE_THRESHOLD,
    COUNT_MODE,
    EXPAND_MODE,
    HOSTNAME,
    PARTITION_CHUNK,
    SUBPATH,
    TOP_VALUE,
    VERSION,
    VERSIONING,
)
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
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql.sqltypes import Integer, String, Text

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

    def __init__(
        self,
        main_entity=None,
        db=None,
        full_path=None,
        ref=False,
        value=False,
        single_result=False,
        entities=None,
    ):
        super().__init__()
        self.main_entity = main_entity
        self.db = db
        self.full_path = full_path
        self.ref = ref
        self.value = value
        self.single_result = single_result
        self.entities = entities

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

        prefix, *suffix = node.name.split("/", 1)
        new_name = sta2rest.STA2REST.SELECT_MAPPING.get(prefix, prefix)
        node.name = new_name + (f"/{suffix[0]}" if suffix else "")
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
            relationship_entity = None
            fk_parent = None
            # Convert the table name
            expand_identifier.identifier = sta2rest.STA2REST.convert_entity(
                expand_identifier.identifier
            )
            sub_entity = globals()[expand_identifier.identifier]
            sub_query = None
            show_id = False

            # Process select clause if exists
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
            if "id" not in identifiers:
                identifiers.append("id")
                show_id = True

            for field in identifiers:
                tmpField = getattr(sub_entity, field)
                if isinstance(tmpField.type, Geometry):
                    select_fields.append(
                        (func.ST_AsGeoJSON(tmpField).cast(JSONB)).label(
                            tmpField.name
                        )
                    )
                elif isinstance(tmpField.type, TSTZRANGE):
                    select_fields.append(
                        (
                            case(
                                (
                                    func.lower(tmpField).isnot(None)
                                    & func.upper(tmpField).isnot(None),
                                    func.concat(
                                        func.lower(tmpField),
                                        "/",
                                        func.upper(tmpField),
                                    ),
                                ),
                                else_=None,
                            ).label(tmpField.name)
                        )
                    )
                else:
                    if "Link" in tmpField.name:
                        select_fields.append(
                            (HOSTNAME + SUBPATH + VERSION + tmpField).label(
                                tmpField.name
                            )
                        )
                    else:
                        select_fields.append(tmpField)

            if parent:
                relationship_entity = getattr(
                    globals()[parent.replace("TravelTime", "")],
                    expand_identifier.identifier.replace(
                        "TravelTime", ""
                    ).lower(),
                ).property
                if relationship_entity.direction.name == "ONETOMANY":
                    fk_parent = getattr(
                        sub_entity,
                        parent.replace("TravelTime", "").lower() + "_id",
                    )

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

            # Process skip clause
            skip_value = (
                expand_identifier.subquery.skip.count
                if expand_identifier.subquery
                and expand_identifier.subquery.skip
                else 0
            )

            # Process top clause
            top_value = (
                expand_identifier.subquery.top.count + 1
                if expand_identifier.subquery
                and expand_identifier.subquery.top
                else TOP_VALUE + 1
            )

            # Process count clause
            is_count = (
                expand_identifier.subquery.count.value
                if expand_identifier.subquery
                and expand_identifier.subquery.count
                else False
            )

            # Handle nested expand
            json_expands = []
            if (
                expand_identifier.subquery
                and expand_identifier.subquery.expand
            ):
                nested_expand_queries = self.visit_ExpandNode(
                    expand_identifier.subquery.expand,
                    expand_identifier.identifier,
                )
                for nested_sub_query in nested_expand_queries:
                    compiled_query_text = nested_sub_query[0].compile(
                        dialect=engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                    relationship_nested = getattr(
                        globals()[
                            nested_sub_query[2].replace("TravelTime", "")
                        ],
                        nested_sub_query[5].replace("TravelTime", "").lower(),
                    ).property

                    navigation_link_attr = (
                        nested_sub_query[5].replace("TravelTime", "").lower()
                        + "_navigation_link"
                    )
                    navigation_link_value = getattr(
                        sub_entity, navigation_link_attr
                    )
                    label_name = navigation_link_value.name.split("@")[0]

                    if relationship_nested.direction.name != "MANYTOMANY":
                        json_expands.append(
                            func.sensorthings.expand(
                                str(compiled_query_text),
                                "{}".format(
                                    (
                                        nested_sub_query[1].name
                                        if nested_sub_query[1] is not None
                                        else "id"
                                    ),
                                ),
                                text(
                                    '"{}".id::integer'.format(
                                        globals()[
                                            nested_sub_query[2]
                                        ].__tablename__
                                    )
                                ),
                                nested_sub_query[4] - 1,
                                nested_sub_query[3],
                                (
                                    True
                                    if nested_sub_query[1] is not None
                                    else False
                                ),
                                nested_sub_query[7],
                            ).label(label_name)
                        )
                        if (
                            EXPAND_MODE == "ADVANCED"
                            and relationship_nested.direction.name
                            == "ONETOMANY"
                        ):
                            json_expands.append(
                                (
                                    HOSTNAME
                                    + SUBPATH
                                    + VERSION
                                    + getattr(
                                        globals()[nested_sub_query[2]],
                                        "self_link",
                                    )
                                    + "/"
                                    + func.sensorthings.next_link_expand(
                                        str(compiled_query_text),
                                        "{}".format(
                                            nested_sub_query[1].name,
                                        ),
                                        text(
                                            '"{}".id::integer'.format(
                                                globals()[
                                                    nested_sub_query[2]
                                                ].__tablename__
                                            )
                                        ),
                                        nested_sub_query[4],
                                        nested_sub_query[3],
                                        label_name,
                                    )
                                ).label(label_name + "@iot.nextLink"),
                            )
                            if nested_sub_query[8]:
                                json_expands.append(
                                    (
                                        func.sensorthings.count_expand(
                                            str(compiled_query_text),
                                            "{}".format(
                                                nested_sub_query[1].name,
                                            ),
                                            text(
                                                '"{}".id::integer'.format(
                                                    globals()[
                                                        nested_sub_query[2]
                                                    ].__tablename__
                                                )
                                            ),
                                            COUNT_MODE,
                                            COUNT_ESTIMATE_THRESHOLD,
                                        )
                                    ).label(label_name + "@iot.count"),
                                )
                    else:
                        json_expands.append(
                            func.sensorthings.expand_many2many(
                                str(compiled_query_text),
                                f'{relationship_nested.secondary.schema}."{relationship_nested.secondary.name}"',
                                text(
                                    '"{}".id::integer'.format(
                                        globals()[
                                            nested_sub_query[2]
                                        ].__tablename__
                                    )
                                ),
                                "{}_id".format(
                                    nested_sub_query[5].replace(
                                        "TravelTime", ""
                                    )
                                ),
                                "{}_id".format(
                                    nested_sub_query[2].replace(
                                        "TravelTime", ""
                                    )
                                ),
                                nested_sub_query[4] - 1,
                                nested_sub_query[3],
                                nested_sub_query[7],
                            ).label(label_name)
                        )
                        if EXPAND_MODE == "ADVANCED":
                            json_expands.append(
                                (
                                    HOSTNAME
                                    + SUBPATH
                                    + VERSION
                                    + getattr(
                                        globals()[nested_sub_query[2]],
                                        "self_link",
                                    )
                                    + "/"
                                    + func.sensorthings.next_link_expand_many2many(
                                        str(compiled_query_text),
                                        f'{relationship_nested.secondary.schema}."{relationship_nested.secondary.name}"',
                                        text(
                                            '"{}".id::integer'.format(
                                                globals()[
                                                    nested_sub_query[2]
                                                ].__tablename__
                                            )
                                        ),
                                        "{}_id".format(
                                            nested_sub_query[5].replace(
                                                "TravelTime", ""
                                            )
                                        ),
                                        "{}_id".format(
                                            nested_sub_query[2].replace(
                                                "TravelTime", ""
                                            )
                                        ),
                                        nested_sub_query[4],
                                        nested_sub_query[3],
                                    )
                                ).label(label_name + "@iot.nextLink"),
                            )
                            if nested_sub_query[8]:
                                json_expands.append(
                                    (
                                        func.sensorthings.count_expand_many2many(
                                            str(compiled_query_text),
                                            f'{relationship_nested.secondary.schema}."{relationship_nested.secondary.name}"',
                                            text(
                                                '"{}".id::integer'.format(
                                                    globals()[
                                                        nested_sub_query[2]
                                                    ].__tablename__
                                                )
                                            ),
                                            "{}_id".format(
                                                nested_sub_query[5].replace(
                                                    "TravelTime", ""
                                                )
                                            ),
                                            "{}_id".format(
                                                nested_sub_query[2].replace(
                                                    "TravelTime", ""
                                                )
                                            ),
                                            COUNT_MODE,
                                            COUNT_ESTIMATE_THRESHOLD,
                                        )
                                    ).label(label_name + "@iot.count"),
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
                    for rel in join_relationships:
                        sub_query = sub_query.join(rel)

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

    async def visit_QueryNode(self, node: QueryNode):
        """
        Visit a query node.

        Args:
            node (ast.QueryNode): The query node to visit.

        Returns:
            str: The converted query node.
        """
        # list to store the converted parts of the query node

        result_format = (
            "DataArray"
            if node.result_format and node.result_format.value == "dataArray"
            else None
        )

        async with self.db as session:
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

            if not node.select:
                node.select = SelectNode([])
                # get default columns for main entity
                default_columns = sta2rest.STA2REST.get_default_column_names(
                    self.main_entity
                    if not result_format
                    else self.main_entity + result_format
                )
                for column in default_columns:
                    node.select.identifiers.append(IdentifierNode(column))

            # Check if we have a select, filter, orderby, skip, top or count in the query
            if node.select:
                select_query = []

                # Iterate over fields in node.select
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

            json_build_object_args = []
            for attr in select_query:
                name = (
                    attr.name
                    if isinstance(attr, InstrumentedAttribute)
                    else attr.right.value
                )
                if isinstance(attr.type, Geometry):
                    json_build_object_args.append(
                        func.ST_AsGeoJSON(attr).cast(JSONB).label(name)
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
                        ).label(name)
                    )
                else:
                    if "Link" in name:
                        if node.as_of and name != "Commit@iot.navigationLink":
                            json_build_object_args.append(
                                (
                                    HOSTNAME
                                    + SUBPATH
                                    + VERSION
                                    + attr
                                    + "?$as_of="
                                    + node.as_of.value
                                ).label(name)
                            )
                        else:
                            json_build_object_args.append(
                                (HOSTNAME + SUBPATH + VERSION + attr).label(
                                    name
                                )
                            )
                    else:
                        if name != "id":
                            json_build_object_args.append(attr.label(name))
                        else:
                            json_build_object_args.append(
                                attr.label("@iot.id")
                            )

            # Check if we have an expand node before the other parts of the query
            if node.expand:
                # get the expand identifiers that do NOT have a nested expand
                expand_identifiers_path = {
                    "expand": {
                        "identifiers": [
                            e for e in node.expand.identifiers if not e.expand
                        ]
                    }
                }
                # in this list there are all the expand identifiers that have a nested expand
                node.expand.identifiers = [
                    e for e in node.expand.identifiers if e.expand
                ]

                if expand_identifiers_path["expand"]["identifiers"]:
                    identifiers = expand_identifiers_path["expand"][
                        "identifiers"
                    ]

                    main_query = select(*json_build_object_args)

                    for i, e in enumerate(identifiers):
                        if e.subquery and e.subquery.filter:
                            filter, join_relationships = self.visit_FilterNode(
                                e.subquery.filter, e.identifier
                            )
                            main_query = main_query.filter(filter)
                            query_count = query_count.filter(filter)
                            query_estimate_count = query_estimate_count.filter(
                                filter
                            )
                            if join_relationships:
                                for rel in join_relationships:
                                    main_query = main_query.join(rel)
                                    query_count = query_count.join(rel)
                                    query_estimate_count = (
                                        query_estimate_count.join(rel)
                                    )

                        if i > 0:
                            identifier = e.identifier
                            nested_identifier = identifiers[i - 1].identifier
                        else:
                            identifier = self.main_entity
                            nested_identifier = e.identifier

                        relationship = getattr(
                            globals()[identifier.replace("TravelTime", "")],
                            nested_identifier.replace(
                                "TravelTime", ""
                            ).lower(),
                        ).property
                        if relationship.direction.name == "MANYTOONE":
                            filter = getattr(
                                globals()[identifier],
                                nested_identifier.replace(
                                    "TravelTime", ""
                                ).lower()
                                + "_id",
                            ) == getattr(globals()[nested_identifier], "id")
                        elif relationship.direction.name == "ONETOMANY":
                            filter = getattr(
                                globals()[nested_identifier],
                                identifier.replace("TravelTime", "").lower()
                                + "_id",
                            ) == getattr(globals()[identifier], "id")
                        else:
                            filter = getattr(
                                globals()[identifier], "id"
                            ) == relationship.secondary.columns.get(
                                identifier.replace("TravelTime", "").lower()
                                + "_id"
                            )
                            main_query = main_query.filter(filter)
                            query_count = query_count.filter(filter)
                            query_estimate_count = query_estimate_count.filter(
                                filter
                            )
                            filter = getattr(
                                globals()[nested_identifier], "id"
                            ) == relationship.secondary.columns.get(
                                nested_identifier.replace(
                                    "TravelTime", ""
                                ).lower()
                                + "_id"
                            )

                        main_query = main_query.filter(filter)
                        query_count = query_count.filter(filter)
                        query_estimate_count = query_estimate_count.filter(
                            filter
                        )

                        if node.as_of:
                            filter_node = FilterNode(
                                f"system_time_validity eq {node.as_of.value}"
                            )
                            filter, _ = self.visit_FilterNode(
                                filter_node, nested_identifier
                            )
                            main_query = main_query.filter(filter)
                            query_count = query_count.filter(filter)
                            query_estimate_count = query_estimate_count.filter(
                                filter
                            )

                # here we create the sub queries for the expand identifiers
                if node.expand.identifiers:
                    # Visit the expand node
                    sub_queries = self.visit_ExpandNode(
                        node.expand, self.main_entity
                    )
                    for sub_query in sub_queries:
                        compiled_query_text = sub_query[0].compile(
                            dialect=engine.dialect,
                            compile_kwargs={"literal_binds": True},
                        )

                        relationship_type = getattr(
                            globals()[
                                self.main_entity.replace("TravelTime", "")
                            ],
                            sub_query[5].replace("TravelTime", "").lower(),
                        ).property

                        navigation_link_attr = (
                            sub_query[5].replace("TravelTime", "").lower()
                            + "_navigation_link"
                        )
                        navigation_link_value = getattr(
                            main_entity, navigation_link_attr
                        )
                        label_name = navigation_link_value.name.split("@")[0]

                        if relationship_type.direction.name != "MANYTOMANY":
                            json_build_object_args.append(
                                func.sensorthings.expand(
                                    str(compiled_query_text),
                                    "{}".format(
                                        (
                                            sub_query[1].name
                                            if sub_query[1] is not None
                                            else "id"
                                        ),
                                    ),
                                    text(
                                        '"{}".id::integer'.format(
                                            globals()[
                                                sub_query[2]
                                            ].__tablename__
                                        )
                                    ),
                                    sub_query[4] - 1,
                                    sub_query[3],
                                    (
                                        True
                                        if sub_query[1] is not None
                                        else False
                                    ),
                                    sub_query[7],
                                ).label(label_name)
                            )
                            if (
                                EXPAND_MODE == "ADVANCED"
                                and relationship_type.direction.name
                                == "ONETOMANY"
                            ):
                                json_build_object_args.append(
                                    (
                                        HOSTNAME
                                        + SUBPATH
                                        + VERSION
                                        + getattr(main_entity, "self_link")
                                        + "/"
                                        + func.sensorthings.next_link_expand(
                                            str(compiled_query_text),
                                            "{}".format(
                                                sub_query[1].name,
                                            ),
                                            text(
                                                '"{}".id::integer'.format(
                                                    globals()[
                                                        sub_query[2]
                                                    ].__tablename__
                                                )
                                            ),
                                            sub_query[4],
                                            sub_query[3],
                                            label_name,
                                        )
                                    ).label(label_name + "@iot.nextLink"),
                                )
                                if sub_query[8]:
                                    json_build_object_args.append(
                                        (
                                            func.sensorthings.count_expand(
                                                str(compiled_query_text),
                                                "{}".format(
                                                    sub_query[1].name,
                                                ),
                                                text(
                                                    '"{}".id::integer'.format(
                                                        globals()[
                                                            sub_query[2]
                                                        ].__tablename__
                                                    )
                                                ),
                                                COUNT_MODE,
                                                COUNT_ESTIMATE_THRESHOLD,
                                            )
                                        ).label(label_name + "@iot.count"),
                                    )
                        else:
                            json_build_object_args.append(
                                func.sensorthings.expand_many2many(
                                    str(compiled_query_text),
                                    f'{relationship_type.secondary.schema}."{relationship_type.secondary.name}"',
                                    text(
                                        '"{}".id::integer'.format(
                                            globals()[
                                                sub_query[2]
                                            ].__tablename__
                                        )
                                    ),
                                    "{}_id".format(
                                        sub_query[5].replace("TravelTime", "")
                                    ),
                                    "{}_id".format(
                                        sub_query[2].replace("TravelTime", "")
                                    ),
                                    sub_query[4] - 1,
                                    sub_query[3],
                                    sub_query[7],
                                ).label(label_name)
                            )
                            if EXPAND_MODE == "ADVANCED":
                                json_build_object_args.append(
                                    (
                                        HOSTNAME
                                        + SUBPATH
                                        + VERSION
                                        + getattr(main_entity, "self_link")
                                        + "/"
                                        + func.sensorthings.next_link_expand_many2many(
                                            str(compiled_query_text),
                                            f'{relationship_type.secondary.schema}."{relationship_type.secondary.name}"',
                                            text(
                                                '"{}".id::integer'.format(
                                                    globals()[
                                                        sub_query[2]
                                                    ].__tablename__
                                                )
                                            ),
                                            "{}_id".format(
                                                sub_query[5].replace(
                                                    "TravelTime", ""
                                                )
                                            ),
                                            "{}_id".format(
                                                sub_query[2].replace(
                                                    "TravelTime", ""
                                                )
                                            ),
                                            sub_query[4],
                                            sub_query[3],
                                        )
                                    ).label(label_name + "@iot.nextLink"),
                                )
                                if sub_query[8]:
                                    json_build_object_args.append(
                                        (
                                            func.sensorthings.count_expand_many2many(
                                                str(compiled_query_text),
                                                f'{relationship_type.secondary.schema}."{relationship_type.secondary.name}"',
                                                text(
                                                    '"{}".id::integer'.format(
                                                        globals()[
                                                            sub_query[2]
                                                        ].__tablename__
                                                    )
                                                ),
                                                "{}_id".format(
                                                    sub_query[5].replace(
                                                        "TravelTime", ""
                                                    )
                                                ),
                                                "{}_id".format(
                                                    sub_query[2].replace(
                                                        "TravelTime", ""
                                                    )
                                                ),
                                                COUNT_MODE,
                                                COUNT_ESTIMATE_THRESHOLD,
                                            )
                                        ).label(label_name + "@iot.count"),
                                    )
                    main_query = select(*json_build_object_args)
            else:
                if result_format == "DataArray":
                    json_build_object_args.append(
                        getattr(main_entity, "datastream_id").label(
                            "datastream_id"
                        )
                    )
                    json_build_object_args.append(
                        cast(components, ARRAY(String)).label("components")
                    )

                main_query = select(*json_build_object_args)

                if result_format == "DataArray":
                    main_query = main_query.order_by(
                        "datastream_id", getattr(main_entity, "id").desc()
                    ).distinct("datastream_id")

            if node.filter:
                filter, join_relationships = self.visit_FilterNode(
                    node.filter, self.main_entity
                )
                main_query = main_query.filter(filter)
                query_count = query_count.filter(filter)
                query_estimate_count = query_estimate_count.filter(filter)
                if join_relationships:
                    for rel in join_relationships:
                        main_query = main_query.join(rel)
                        query_count = query_count.join(rel)
                        query_estimate_count = query_estimate_count.join(rel)

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
            main_query = main_query.order_by(*ordering)

            # Determine skip and top values, defaulting to 0 and 100 respectively if not specified
            skip_value = self.visit(node.skip) if node.skip else 0

            top_value = self.visit(node.top) + 1 if node.top else TOP_VALUE + 1

            is_count = bool(node.count and node.count.value)

            if is_count:
                if COUNT_MODE in {"LIMIT_ESTIMATE", "ESTIMATE_LIMIT"}:
                    compiled_query_text = str(
                        query_estimate_count.compile(
                            dialect=engine.dialect,
                            compile_kwargs={"literal_binds": True},
                        )
                    )

                    if COUNT_MODE == "LIMIT_ESTIMATE":
                        query_estimate = await session.execute(
                            select(func.count()).select_from(
                                query_estimate_count.limit(
                                    COUNT_ESTIMATE_THRESHOLD
                                )
                            ),
                        )
                        query_count = query_estimate.scalar()
                        if query_count == COUNT_ESTIMATE_THRESHOLD:
                            query_estimate = await session.execute(
                                text(
                                    "SELECT sensorthings.count_estimate(:compiled_query_text) as estimated_count"
                                ),
                                {"compiled_query_text": compiled_query_text},
                            )
                            query_count = query_estimate.scalar()
                    elif COUNT_MODE == "ESTIMATE_LIMIT":
                        query_estimate = await session.execute(
                            text(
                                "SELECT sensorthings.count_estimate(:compiled_query_text) as estimated_count"
                            ),
                            {"compiled_query_text": compiled_query_text},
                        )
                        query_count = query_estimate.scalar()
                        if query_count < COUNT_ESTIMATE_THRESHOLD:
                            query_estimate = await session.execute(
                                select(func.count()).select_from(
                                    query_estimate_count.limit(
                                        COUNT_ESTIMATE_THRESHOLD
                                    )
                                ),
                            )
                            query_count = query_estimate.scalar()
                else:
                    query_count = await session.execute(query_count)
                    query_count = query_count.scalar()

            iot_count = (
                '"@iot.count": ' + str(query_count) + ","
                if is_count and not self.single_result
                else ""
            )

            if result_format == "DataArray" and node.expand:
                if top_value > 1:
                    top_value -= 1

            main_query = (
                select(main_query.columns)
                .limit(top_value)
                .offset(skip_value)
                .alias("main_query")
            )

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
                        literal("1")
                        .cast(Integer)
                        .label("dataArray@iot.count"),
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

            as_of_value = (
                node.as_of.value
                if node.as_of
                else datetime.now() if VERSIONING else ""
            )

            from_to_value = True if node.from_to else False

            if self.value:
                value = None
                if isinstance(select_query[0], InstrumentedAttribute):
                    value = select_query[0].name
                else:
                    value = select_query[0].right
                main_query = select(
                    main_query.c.json.op("->")(value)
                ).select_from(main_query)

            main_query = stream_results(
                self.main_entity,
                main_query,
                session,
                top_value,
                iot_count,
                as_of_value,
                from_to_value,
                self.single_result,
                self.full_path,
            )

        return main_query


async def stream_results(
    entity,
    query,
    session,
    top,
    iot_count,
    as_of_value,
    from_to_value,
    single_result,
    full_path,
):
    async with session.begin():
        result = await session.stream(query)
        start_json = ""
        is_first_partition = True
        has_rows = False

        async for partition in result.scalars().partitions(PARTITION_CHUNK):
            partition_len = len(partition)
            has_rows = True

            if partition_len > top - 1:
                partition = partition[:-1]

            if (
                VERSIONING
                and single_result
                and partition_len == 1
                and entity != "Commit"
                and not from_to_value
            ):
                partition[0]["@iot.as_of"] = as_of_value

            partition_json = ujson.dumps(
                partition,
                default=datetime.isoformat,
                escape_forward_slashes=False,
            )[1:-1]

            if is_first_partition:
                if partition_len > 0 and not single_result:
                    start_json = "{"

                next_link = build_nextLink(full_path, partition_len)
                next_link_json = (
                    f'"@iot.nextLink": "{next_link}",'
                    if next_link and not single_result
                    else ""
                )
                as_of = (
                    f'"@iot.as_of": "{as_of_value}",'
                    if VERSIONING and not single_result and not from_to_value
                    else ""
                )
                start_json += as_of + iot_count + next_link_json
                start_json += (
                    '"value": ['
                    if (partition_len > 0 and not single_result)
                    else ""
                )

                yield start_json + partition_json
                is_first_partition = False
            else:
                yield "," + partition_json

        if not has_rows and not single_result:
            yield '{"value": []}'

        if has_rows and not single_result:
            yield "]}"


def build_nextLink(full_path, count_links):
    nextLink = f"{HOSTNAME}{full_path}"
    new_top_value = TOP_VALUE

    # Handle $top
    if "$top" in nextLink:
        start_index = nextLink.find("$top=") + 5
        end_index = len(nextLink)
        for char in ("&", ";", ")"):
            char_index = nextLink.find(char, start_index)
            if char_index != -1 and char_index < end_index:
                end_index = char_index
        top_value = int(nextLink[start_index:end_index])
        new_top_value = top_value
        nextLink = (
            nextLink[:start_index] + str(new_top_value) + nextLink[end_index:]
        )
    else:
        if "?" in nextLink:
            nextLink = nextLink + f"&$top={new_top_value}"
        else:
            nextLink = nextLink + f"?$top={new_top_value}"

    # Handle $skip
    if "$skip" in nextLink:
        start_index = nextLink.find("$skip=") + 6
        end_index = len(nextLink)
        for char in ("&", ";", ")"):
            char_index = nextLink.find(char, start_index)
            if char_index != -1 and char_index < end_index:
                end_index = char_index
        skip_value = int(nextLink[start_index:end_index])
        new_skip_value = skip_value + new_top_value
        nextLink = (
            nextLink[:start_index] + str(new_skip_value) + nextLink[end_index:]
        )
    else:
        new_skip_value = new_top_value
        nextLink = nextLink + f"&$skip={new_skip_value}"

    # Only return the nextLink if there's more data to fetch
    if new_top_value < count_links:
        return nextLink

    return None
