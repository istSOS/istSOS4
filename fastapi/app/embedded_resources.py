def generate_json_extraction_queries(schema_dict, object_fields):
    """
    Given a dictionary describing a PostgreSQL schema and a dictionary of object fields,
    generate SQL queries to extract the data in JSON format.

    Args:
        schema_dict (dict): A dictionary describing the PostgreSQL schema.
        object_fields (dict): A dictionary of objects to be selected along with their related objects and fields.

    Returns:
        str: A string containing the SQL queries to extract the data in JSON format.
    """
    queries = []
    for object_name, object_fields_dict in object_fields.items():
        object_query = f"SELECT jsonb_build_object('{object_name}', jsonb_agg(to_jsonb({object_name}.{{"
        for field_name in object_fields_dict.get("fields", []):
            object_query += f"'{field_name}', {object_name}.{field_name},"
        for related_object_name, related_object_fields_dict in object_fields_dict.get("related_objects", {}).items():
            related_object_query = f"jsonb_build_object('{related_object_name}', jsonb_agg(to_jsonb({related_object_name}.{{"
            for field_name in related_object_fields_dict.get("fields", []):
                related_object_query += f"'{field_name}', {related_object_name}.{field_name},"
            for fk_name, fk_dict in schema_dict.get(object_name, {}).get("fks", {}).items():
                if fk_name == related_object_name:
                    fk_table_name = fk_dict.get("table")
                    fk_column_name = fk_dict.get("column")
                    related_object_query += f"'{object_name}', {object_name}.id"
                    related_object_query += f"}}) || '{{\"{object_name}\":\"{fk_table_name}\"}}'::jsonb))"
                    related_object_query += f" FROM {fk_table_name} {related_object_name} WHERE {related_object_name}.{fk_column_name} = {object_name}.id"
                    object_query += related_object_query
        object_query += "}))) FROM {object_name};"
        queries.append(object_query)
    return "\n".join(queries)



# Example schema dictionary
schema_dict = {
    "users": {
        "cols": {
            "id": "SERIAL PRIMARY KEY",
            "name": "VARCHAR(255)"
        }
    },
    "orders": {
        "cols": {
            "id": "SERIAL PRIMARY KEY",
            "user_id": "INTEGER REFERENCES users(id)"
        },
        "fks": {
            "user": {
                "table": "users",
                "column": "id"
            }
        }
    }
}

# Generate SQL queries for extracting data in JSON format
queries = generate_json_extraction_queries(schema_dict)
print(queries)

# Example object fields dictionary
object_fields = {
    "users": {
        "fields": ["id", "name"],
        "related_objects": {
            "orders": {
                "fields": ["id", "order_date"],
                "related_objects": {
                    "products": {
                        "fields": ["id", "name", "price"]
                    }
                }
            }
        }
    }
}

# Generate SQL queries for extracting data in JSON format
queries = generate_json_extraction_queries(schema_dict, object_fields)
print(queries)

SELECT jsonb_build_object('users', jsonb_agg(to_jsonb(users.{'
    "id": users.id, "name": users.name,
    "orders": jsonb_agg(to_jsonb(orders.{'
        "id": orders.id, "order_date": orders.order_date,
        "products": jsonb_agg(to_jsonb(products.{'
            "id": products.id, "name": products.name, "price": products.price
        }) || '{"orders":"orders"}'::jsonb))
    || '{"users":"users"}'::jsonb))
FROM users;
