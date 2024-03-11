from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.utils.table import delete_schema
from thanosql._error import ThanoSQLAlreadyExistsError

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


def test_get_schemas_success(client: ThanoSQL):
    res = client.schema.list()
    assert isinstance(res, dict)
    assert "schemas" in res

    # at least "public" and "qm"
    assert len(res["schemas"]) >= 2


def test_create_schema(client: ThanoSQL, new_schema: str):
    # schema creation is implicitly tested when calling fixture
    # trying to create a schema of the same name again should result in an error
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.schema.create(name=new_schema)

    # all that's left is cleaning up, which does not have a dedicated API
    res = delete_schema(client=client, name=new_schema)
    assert res["error_result"] == None
