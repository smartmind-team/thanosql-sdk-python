import logging

import pytest

from typing import Generator

from tests.faker import fake
from tests.utils.table import (
    create_schema,
    create_table,
    create_table_template,
    create_view,
    delete_schema,
)
from thanosql._client import ThanoSQL
from thanosql._error import ThanoSQLNotFoundError
from thanosql.resources import BaseColumn, Table, TableObject, TableTemplate, View

basic_table_columns = [("integer", "id"), ("varchar", "name"), ("integer", "price")]
basic_view_columns = [basic_table_columns[0][1], basic_table_columns[1][1]]


@pytest.fixture(scope="session")
def client() -> ThanoSQL:
    return ThanoSQL()


@pytest.fixture(scope="module")
def new_schema(client: ThanoSQL) -> Generator[str, None, None]:
    name = f"test_new_schema_{fake.unique.pystr(8).lower()}"
    yield create_schema(client=client, name=name)
    try:
        delete_schema(client=client, name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"schema {name} is already deleted")


@pytest.fixture(scope="module")
def empty_table(client: ThanoSQL) -> Generator[Table, None, None]:
    name = f"test_empty_table_{fake.unique.pystr(8).lower()}"
    yield create_table(client=client, name=name, schema="public", table=TableObject())
    try:
        client.table.delete(name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"table {name} is already deleted")


@pytest.fixture(scope="module")
def basic_table(client: ThanoSQL) -> Generator[Table, None, None]:
    table_object = TableObject(
        columns=[BaseColumn(type=col[0], name=col[1]) for col in basic_table_columns]
    )
    name = f"test_basic_table_{fake.unique.pystr(8).lower()}"
    yield create_table(client=client, name=name, table=table_object)
    try:
        client.table.delete(name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"table {name} is already deleted")


@pytest.fixture(scope="module")
def empty_table_name(empty_table: Table) -> str:
    return empty_table.name


@pytest.fixture(scope="module")
def basic_table_name(basic_table: Table) -> str:
    return basic_table.name


@pytest.fixture(scope="module")
def empty_table_template(client: ThanoSQL) -> Generator[TableTemplate, None, None]:
    name = f"test_empty_template_{fake.unique.pystr(min_chars=8, max_chars=8).lower()}"
    yield create_table_template(client=client, name=name, table=TableObject())
    try:
        client.table.template.delete(name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"table template {name} is already deleted")


@pytest.fixture(scope="module")
def empty_table_template_name(empty_table_template: dict) -> str:
    return empty_table_template["name"]


@pytest.fixture(scope="module")
def empty_view(client: ThanoSQL, empty_table_name: str) -> Generator[View, None, None]:
    name = f"test_empty_view_{fake.unique.pystr(8).lower()}"
    yield create_view(
        client=client, name=name, column_names="*", table_name=empty_table_name
    )
    try:
        client.view.delete(name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"view {name} is already deleted")


@pytest.fixture(scope="module")
def basic_view(client: ThanoSQL, basic_table_name: str) -> Generator[View, None, None]:
    name = f"test_basic_view_{fake.unique.pystr(8).lower()}"
    yield create_view(
        client=client,
        name=name,
        column_names=basic_view_columns,
        table_name=basic_table_name,
    )
    try:
        client.view.delete(name=name)
    except ThanoSQLNotFoundError:
        logging.info(f"view {name} is already deleted")


@pytest.fixture(scope="module")
def empty_view_name(empty_view: View) -> str:
    return empty_view.name


@pytest.fixture(scope="module")
def basic_view_name(basic_view: View) -> str:
    return basic_view.name
