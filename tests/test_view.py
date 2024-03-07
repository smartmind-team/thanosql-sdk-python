from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.faker import fake
from thanosql._error import ThanoSQLNotFoundError

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL
    from _pytest.fixtures import FixtureRequest


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
    assert len(res["views"]) == 2


@pytest.mark.parametrize("name", ["empty_view_name", "basic_view_name"])
def test_get_view_success(client: ThanoSQL, name: str, request: FixtureRequest):
    name = request.getfixturevalue(name)
    res = client.view.get(name=name)
    assert isinstance(res, dict)
    assert "view" in res
    assert set(["name", "schema", "columns", "definition"]) == set(res["view"].keys())
    assert res["view"]["name"] == name


def test_get_view_nonexistent(client: ThanoSQL):
    with pytest.raises(ThanoSQLNotFoundError):
        client.view.get(name=fake.unique.pystr(10))


@pytest.mark.parametrize("name", ["empty_view_name", "basic_view_name"])
def test_delete_view_success(client: ThanoSQL, name: str, request: FixtureRequest):
    name = request.getfixturevalue(name)
    res = client.view.delete(name=name)
    assert isinstance(res, dict)
    
    with pytest.raises(ThanoSQLNotFoundError):
        client.view.get(name=name)
