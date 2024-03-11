from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.faker import fake
from thanosql._error import ThanoSQLNotFoundError

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from thanosql._client import ThanoSQL


def test_get_views_success(client: ThanoSQL):
    res = client.view.list()
    assert isinstance(res, dict)
    assert "views" in res


def test_get_views_nonexistent_schema(client: ThanoSQL):
    with pytest.raises(ThanoSQLNotFoundError):
        client.view.list(schema=fake.unique.pystr(8))


def test_get_views_limit(client: ThanoSQL, empty_view: dict, basic_view: dict):
    res = client.view.list(limit=2)
    assert isinstance(res, dict)
    assert "views" in res

    # at least empty_view and basic_view, at most 2 because of the limit
    assert len(res["views"]) == 2


@pytest.mark.parametrize("name", ["empty_view_name", "basic_view_name"])
def test_get_view_success(client: ThanoSQL, name: str, request: FixtureRequest):
    name = request.getfixturevalue(name)
    res = client.view.get(name=name)
    assert isinstance(res, dict)
    assert "view" in res
    assert {"name", "schema", "columns", "definition"} == set(res["view"].keys())
    assert res["view"]["name"] == name


def test_get_view_nonexistent(client: ThanoSQL):
    with pytest.raises(ThanoSQLNotFoundError):
        client.view.get(name=fake.unique.pystr(10))


@pytest.mark.parametrize(
    "view_name, table_name",
    [("empty_view_name", "empty_table_name"), ("basic_view_name", "basic_table_name")],
)
def test_delete_view_success(
    client: ThanoSQL, view_name: str, table_name: str, request: FixtureRequest
):
    view_name = request.getfixturevalue(view_name)
    res = client.view.delete(name=view_name)
    assert isinstance(res, dict)

    with pytest.raises(ThanoSQLNotFoundError):
        client.view.get(name=view_name)

    # also delete the tables used to create the views
    # the assertions are covered by the delete table tests already
    table_name = request.getfixturevalue(table_name)
    client.table.delete(name=table_name)
