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

"""SQL helpers for building SensorThings expansion queries."""

from dataclasses import dataclass, replace

from sqlalchemy import cast, func, select, text
from sqlalchemy.sql.sqltypes import Integer

from app import (
    COUNT_ESTIMATE_THRESHOLD,
    COUNT_MODE,
    HOSTNAME,
    SUBPATH,
    VERSION,
)
from app import models


@dataclass
class ExpansionOptions:
    """OData options and parent-row references for one expansion."""

    select_node: str
    filter_node: str
    orderby_node: str
    expand_node: str
    parent_id: object = None
    parent_link: object = None


@dataclass
class ExpansionSpec:
    """Compiled child query and relationship metadata for an expansion."""

    relationship: object
    compiled_query: object
    sub_query: object
    label: str
    options: ExpansionOptions


@dataclass
class PagedExpansionQuery:
    """Build a root page first, then evaluate volatile expansion functions."""

    main_entity: object
    ordering: list
    expansions: list
    base_column_count: int

    def apply(self, query, top_value, skip_value):
        """Apply pagination and defer root expansions until after that page."""
        if not self.expansions:
            return query.limit(top_value).offset(skip_value).subquery()

        hidden_columns = [
            getattr(self.main_entity, "id").label("__sta_expand_parent_id"),
            getattr(self.main_entity, "self_link").label(
                "__sta_expand_parent_self_link"
            ),
        ]
        many_to_one_fk_labels = {}
        for expansion in self.expansions:
            if expansion.relationship.direction.name == "MANYTOONE":
                fk_name = (
                    expansion.sub_query[5].replace("TravelTime", "").lower()
                    + "_id"
                )
                fk_label = f"__sta_expand_fk_{fk_name}"
                if fk_label not in many_to_one_fk_labels.values():
                    hidden_columns.append(
                        getattr(self.main_entity, fk_name).label(fk_label)
                    )
                many_to_one_fk_labels[fk_name] = fk_label
        hidden_columns.append(
            func.row_number()
            .over(order_by=self.ordering)
            .label("__sta_page_order")
        )

        page_query = (
            query.add_columns(*hidden_columns)
            .limit(top_value)
            .offset(skip_value)
            .subquery()
        )
        expanded_columns = []
        for expansion in self.expansions:
            if expansion.relationship.direction.name == "MANYTOONE":
                fk_name = (
                    expansion.sub_query[5].replace("TravelTime", "").lower()
                    + "_id"
                )
                parent_id = page_query.c[many_to_one_fk_labels[fk_name]]
            else:
                parent_id = page_query.c["__sta_expand_parent_id"]
            options = replace(
                expansion.options,
                parent_id=parent_id,
                parent_link=(
                    HOSTNAME
                    + SUBPATH
                    + VERSION
                    + page_query.c["__sta_expand_parent_self_link"]
                    + "/"
                ),
            )
            expanded_columns.append(
                get_expand_function(
                    expansion.relationship,
                    expansion.compiled_query,
                    expansion.sub_query,
                    expansion.label,
                    options,
                )
            )
        return (
            select(
                *list(page_query.columns)[: self.base_column_count],
                *expanded_columns,
            )
            .select_from(page_query)
            .order_by(page_query.c["__sta_page_order"])
            .subquery()
        )


def get_expand_function(
    relationship, compiled_query, sub_query, label, options
):
    """Build the SQL function call for the relationship’s cardinality."""
    if relationship.direction.name != "MANYTOMANY":
        return expand_function(compiled_query, sub_query, label, options)
    return expand_many2many_function(
        compiled_query, sub_query, label, relationship, options
    )


def expand_function(compiled_query, sub_query, label_name, options):
    """Build a one-to-many or many-to-one expansion function call."""
    parent_model = getattr(models, sub_query[2])
    parent_id = options.parent_id
    if parent_id is None:
        parent_id = (
            text(f'"{parent_model.__tablename__}".id::integer')
            if sub_query[1] is not None
            else text(
                f'"{parent_model.__tablename__}".'
                f'{sub_query[5].replace("TravelTime", "")}_id::integer'
            )
        )
    parent_id = cast(parent_id, Integer)
    parent_link = options.parent_link
    if parent_link is None:
        parent_link = (
            HOSTNAME
            + SUBPATH
            + VERSION
            + getattr(parent_model, "self_link")
            + "/"
        )
    foreign_key = sub_query[1].name if sub_query[1] is not None else "id"
    return func.sensorthings.expand(
        str(compiled_query),
        foreign_key,
        parent_id,
        sub_query[4],
        sub_query[3],
        sub_query[1] is not None,
        sub_query[7],
        label_name,
        parent_link,
        sub_query[8],
        COUNT_MODE,
        COUNT_ESTIMATE_THRESHOLD,
        options.select_node,
        options.filter_node,
        options.orderby_node,
        options.expand_node,
    ).label(label_name)


def expand_many2many_function(
    compiled_query, sub_query, label_name, relationship, options
):
    """Build the many-to-many expansion function call."""
    parent_model = getattr(models, sub_query[2])
    parent_id = options.parent_id
    if parent_id is None:
        parent_id = text(f'"{parent_model.__tablename__}".id::integer')
    parent_id = cast(parent_id, Integer)
    parent_link = options.parent_link
    if parent_link is None:
        parent_link = (
            HOSTNAME
            + SUBPATH
            + VERSION
            + getattr(parent_model, "self_link")
            + "/"
        )
    return func.sensorthings.expand_many2many(
        str(compiled_query),
        f'{relationship.secondary.schema}."{relationship.secondary.name}"',
        parent_id,
        f"{sub_query[5].replace('TravelTime', '')}_id",
        f"{sub_query[2].replace('TravelTime', '')}_id",
        sub_query[4],
        sub_query[3],
        sub_query[7],
        label_name,
        parent_link,
        sub_query[8],
        COUNT_MODE,
        COUNT_ESTIMATE_THRESHOLD,
        options.select_node,
        options.filter_node,
        options.orderby_node,
        options.expand_node,
    ).label(label_name)
