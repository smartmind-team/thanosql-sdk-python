import pytest

from tests.faker import fake
from tests.utils.table import create_schema, create_table, create_view
from thanosql._client import ThanoSQL
from thanosql.resources import BaseColumn, TableObject, TableServiceObject

basic_table_columns = [("integer", "id"), ("varchar", "name"), ("integer", "price")]
basic_view_columns = [basic_table_columns[0][1], basic_table_columns[1][1]]


@pytest.fixture(scope="session")
def client() -> ThanoSQL:
    return ThanoSQL()


@pytest.fixture(scope="module")
def new_schema(client: ThanoSQL) -> str:
    name = f"test_new_schema_{fake.unique.pystr(8).lower()}"
    return create_schema(client=client, name=name)


@pytest.fixture(scope="module")
def empty_table(client: ThanoSQL) -> TableServiceObject:
    name = f"test_empty_table_{fake.unique.pystr(8).lower()}"
    return create_table(client=client, name=name, schema="public", table=TableObject())


@pytest.fixture(scope="module")
def basic_table(client: ThanoSQL) -> TableServiceObject:
    table_object = TableObject(
        columns=[BaseColumn(type=col[0], name=col[1]) for col in basic_table_columns]
    )
    name = f"test_basic_table_{fake.unique.pystr(8).lower()}"
    return create_table(client=client, name=name, table=table_object)


@pytest.fixture(scope="module")
def empty_table_name(empty_table: TableServiceObject) -> str:
    return empty_table.name


@pytest.fixture(scope="module")
def basic_table_name(basic_table: TableServiceObject) -> str:
    return basic_table.name


@pytest.fixture(scope="module")
def empty_view(client: ThanoSQL, empty_table_name: str) -> dict:
    name = f"test_empty_view_{fake.unique.pystr(8).lower()}"
    return create_view(
        client=client, name=name, column_names="*", table_name=empty_table_name
    )


@pytest.fixture(scope="module")
def basic_view(client: ThanoSQL, basic_table_name: str) -> dict:
    name = f"test_basic_view_{fake.unique.pystr(8).lower()}"
    return create_view(
        client=client,
        name=name,
        column_names=basic_view_columns,
        table_name=basic_table_name,
    )


@pytest.fixture(scope="module")
def empty_view_name(empty_view: dict) -> str:
    return empty_view["name"]


@pytest.fixture(scope="module")
def basic_view_name(basic_view: dict) -> str:
    return basic_view["name"]
