from thanosql._client import ThanoSQL
from thanosql.resources import Table, TableObject


def create_schema(client: ThanoSQL, name: str) -> str:
    res = client.schema.create(name=name)
    return res["schema"]


def delete_schema(client: ThanoSQL, name: str) -> dict:
    query = f"DROP SCHEMA {name} CASCADE"
    return client.query.execute(query=query)


def create_table(
    client: ThanoSQL,
    name: str,
    schema: str | None = None,
    table: TableObject | None = None,
) -> Table:
    res = client.table.create(name=name, schema=schema, table=table)
    res = client.table.get(name=name, schema=schema)
    return res


def create_table_template(
    client: ThanoSQL,
    name: str,
    table: TableObject | None = None,
    version: str | None = None,
    compatibility: str | None = None,
) -> dict:
    res = client.table.template.create(
        name=name, table_template=table, version=version, compatibility=compatibility
    )
    res = client.table.template.get(name=name, version=version)
    return res["table_templates"][0]


def create_view(
    client: ThanoSQL,
    name: str,
    column_names: str | list[str],
    table_name: str,
    schema: str | None = None,
) -> dict:
    if schema:
        name = f"{schema}.{name}"

    if isinstance(column_names, str):
        query = f"CREATE VIEW {name} AS SELECT {column_names} FROM {table_name}"
    elif isinstance(column_names, list):
        columns_string = ", ".join(column_names)
        query = f"CREATE VIEW {name} AS SELECT {columns_string} FROM {table_name}"

    res = client.query.execute(query=query)
    res = client.view.get(name=name, schema=schema)
    return res["view"]
