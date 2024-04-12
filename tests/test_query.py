from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest

from tests.faker import fake
from thanosql._error import (
    ThanoSQLAlreadyExistsError,
    ThanoSQLNotFoundError,
    ThanoSQLValueError,
)
from thanosql.resources import QueryLog, QueryTemplate, Records

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL

plain_query_template_name = (
    f"test_plain_{fake.unique.pystr(min_chars=8, max_chars=8).lower()}"
)
basic_query_template_name = (
    f"test_basic_{fake.unique.pystr(min_chars=8, max_chars=8).lower()}"
)
changed_query_template_name = (
    f"test_changed_{fake.unique.pystr(min_chars=8, max_chars=8).lower()}"
)

plain_query_template_string = "LIST THANOSQL MODEL"
basic_query_template_string = "SELECT * FROM {{ table_name }}"
changed_query_template_string = "SELECT {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %} FROM {{ table_name }} LIMIT 0"
invalid_query_template_string = "DELETE MODEL {% model_name %}"

query_test_table_name = (
    f"test_query_{fake.unique.pystr(min_chars=8, max_chars=8).lower()}"
)
query_test_selected_columns = ["name", "price"]


def test_create_query_template_invalid(client: ThanoSQL):
    # name too long
    with pytest.raises(ThanoSQLValueError):
        client.query.template.create(name=fake.unique.pystr(min_chars=35, max_chars=40))

    # invalid jinja template
    with pytest.raises(ThanoSQLValueError):
        client.query.template.create(query=invalid_query_template_string)


def test_create_query_template_success(client: ThanoSQL):
    # execute with dry run without any name or query (empty request)
    # check whether auto-naming is working
    res = client.query.template.create(dry_run=True)
    assert isinstance(res, QueryTemplate)
    assert res.query == ""
    assert len(res.parameters) == 0

    temp_name = res.name
    assert temp_name.startswith("query_template_")

    # make sure it is not saved in db
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.template.get(name=temp_name)

    # save to db
    res = client.query.template.create(
        name=plain_query_template_name, query=plain_query_template_string
    )
    assert isinstance(res, QueryTemplate)
    assert res.id != 0
    assert res.name == plain_query_template_name
    assert res.query == plain_query_template_string
    assert len(res.parameters) == 0

    # no need to do thorough checks as we already did in the previous template
    res = client.query.template.create(
        name=basic_query_template_name, query=basic_query_template_string
    )
    assert res.query == basic_query_template_string
    assert res.parameters == ["table_name"]

    # make sure we cannot create templates with the same name again
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.query.template.create(
            name=plain_query_template_name, query=plain_query_template_string
        )

    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.query.template.create(
            name=basic_query_template_name, query=basic_query_template_string
        )


def test_get_query_template_not_found(client: ThanoSQL):
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.template.get(name=fake.unique.pystr(min_chars=8, max_chars=10))


@pytest.mark.parametrize("name", [plain_query_template_name, basic_query_template_name])
def test_get_query_template_success(client: ThanoSQL, name: str):
    # test whether get is working and whether the template creation is successful
    res = client.query.template.get(name=name)
    assert isinstance(res, QueryTemplate)


def test_get_query_templates_default(client: ThanoSQL):
    res = client.query.template.list()
    assert isinstance(res, list)
    # at least the two templates we just created
    assert len(res) >= 2
    assert isinstance(res[0], QueryTemplate)


def test_update_query_template_invalid(client: ThanoSQL):
    # not found
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.template.update(
            current_name=fake.unique.pystr(min_chars=8, max_chars=10)
        )

    # new name too long
    with pytest.raises(ThanoSQLValueError):
        client.query.template.update(
            current_name=basic_query_template_name,
            new_name=fake.unique.pystr(min_chars=35, max_chars=40),
        )

    # new name already used
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.query.template.update(
            current_name=basic_query_template_name, new_name=plain_query_template_name
        )

    # invalid jinja template
    with pytest.raises(ThanoSQLValueError):
        client.query.template.update(
            current_name=basic_query_template_name, query=invalid_query_template_string
        )


def test_update_query_template_success(client: ThanoSQL):
    # update name only
    # make sure name is changed but the rest stays the same
    res = client.query.template.update(
        current_name=plain_query_template_name, new_name=changed_query_template_name
    )
    assert res.name == changed_query_template_name
    assert res.query == plain_query_template_string
    assert len(res.parameters) == 0

    # make sure the new name is callable while the old name is not
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.template.get(name=plain_query_template_name)

    res = client.query.template.get(name=changed_query_template_name)
    assert isinstance(res, QueryTemplate)

    # update query only
    # make sure the name is not changed but query and parameters got changed
    res = client.query.template.update(
        current_name=basic_query_template_name, query=changed_query_template_string
    )
    assert res.name == basic_query_template_name
    assert res.query == changed_query_template_string
    assert set(res.parameters) == {"columns", "table_name"}


def test_get_query_logs_default(client: ThanoSQL):
    res = client.query.log.list()
    # at least we did 2 queries in previous tests
    assert len(res["query_logs"]) >= 2
    assert res["total"] >= 2


def test_post_query_invalid(client: ThanoSQL, basic_table_name: str):
    # schema does not exist
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.execute(
            schema=fake.unique.pystr(8),
            table_name=fake.unique.pystr(8),
            query=plain_query_template_string,
        )

    # table already exists
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.query.execute(
            table_name=basic_table_name, query=plain_query_template_string
        )

    # invalid template input directly
    with pytest.raises(ThanoSQLValueError):
        params = {"model_name": "random_model"}
        client.query.execute(query=invalid_query_template_string, parameters=params)

    # invalid template name
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.execute(template_name=fake.unique.pystr(8))

    # invalid template id -> we most probably will not reach this number
    # but in case someday there will be this many query templates,
    # the invalid id needs to be adjusted
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.execute(template_id=99999)

    # invalid parameter dict
    with pytest.raises(ThanoSQLValueError):
        params = {"table_name": "random_table", "cols": ["column_1", "column_2"]}
        client.query.execute(template_name=basic_query_template_name, parameters=params)


def test_post_query_success(client: ThanoSQL, basic_table_name, empty_table_name):
    params = {"table_name": basic_table_name, "columns": query_test_selected_columns}
    completed_changed_query = f"SELECT name, price FROM {basic_table_name} LIMIT 0"

    # direct entry template
    res = client.query.execute(
        query=changed_query_template_string, parameters=params
    )
    assert isinstance(res, QueryLog)
    assert res.error_result is None
    assert res.destination_schema == "qm"
    assert res.query == completed_changed_query

    # there should be empty record([]) even if max_results is > 0 as we set LIMIT 0 in the query
    assert len(res.records.data) == 0

    # make sure the qm table is created
    qm_table_name = res.destination_table_name
    res = client.table.get(name=qm_table_name, schema="qm")
    assert res

    # direct entry without template or parameters
    res = client.query.execute(
        query=plain_query_template_string, table_name=query_test_table_name, max_results=0
    )
    assert isinstance(res, QueryLog)
    assert res.error_result is None
    assert res.destination_schema == "public"
    assert res.query == plain_query_template_string
    assert res.destination_table_name == query_test_table_name

    # since max_results is 0, there should be no records at all
    assert res.records is None

    # make sure the table is created in public schema
    res = client.table.get(name=query_test_table_name)
    assert res

    # using saved query template
    res = client.query.execute(
        template_name=basic_query_template_name,
        parameters=params,
        table_name=empty_table_name,
        overwrite=True,
    )
    assert isinstance(res, QueryLog)
    assert res.error_result is None
    assert res.destination_schema == "public"
    assert res.query == completed_changed_query
    assert res.destination_table_name == empty_table_name


def test_query_log_records(client: ThanoSQL, empty_table):
    # we want to check if the records section of QueryLog is working properly
    # since we didn't create any tables in this module, we will use the
    # empty_table fixture to ensure that there is at least one table in
    # the list of public tables in information_schema
    query = """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    """
    res = client.query.execute(query=query)
    assert isinstance(res.records, Records)

    # check if to_df is working and check that records is nonempty
    df = res.records.to_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df.index) > 0


@pytest.mark.parametrize(
    "name", [changed_query_template_name, basic_query_template_name]
)
def test_delete_query_template_success(client: ThanoSQL, name: str):
    res = client.query.template.delete(name=name)
    assert "message" in res

    # make sure the query template is no longer "gettable"
    # also to check whether not found error is raised properly
    with pytest.raises(ThanoSQLNotFoundError):
        client.query.template.get(name=name)
