from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.faker import fake
from thanosql._error import ThanoSQLAlreadyExistsError, ThanoSQLNotFoundError, ThanoSQLValueError
from thanosql.resources import TableObject

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL

latest_version = "2.0"


def test_create_table_template_invalid(client: ThanoSQL, empty_table_template: dict):
    # check that we cannot create table template with too long of a name
    with pytest.raises(ThanoSQLValueError):
        client.table.template.create(name=fake.unique.pystr(min_chars=35, max_chars=40).lower(), table_template=TableObject())
    
    # check that we cannot create table template with invalid characters
    with pytest.raises(ThanoSQLValueError):
        client.table.template.create(name="!#%", table_template=TableObject())
    
    # creation of empty table template version 1.0 is done in conftest set-up
    assert "name" in empty_table_template
    name = empty_table_template["name"]
    
    # check that we cannot create table template with the same name and version
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.table.template.create(name=name, table_template=TableObject())
        
    # invalid version format should not work
    with pytest.raises(ThanoSQLValueError):
        client.table.template.create(name=name, table_template=TableObject(), version="2.0.0")


def test_create_table_template_success(client: ThanoSQL, empty_table_template_name: str):    
    # creating the same template with different (valid) version should work
    res = client.table.template.create(name=empty_table_template_name, table_template=TableObject(), version=latest_version)
    assert {"message", "table_template_name"} == set(res.keys())


def test_get_table_template_not_found(client: ThanoSQL):
    # random name that is yet to be used for creation should not work
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.get(name=fake.unique.pystr(8))


def test_get_table_template_not_found(client: ThanoSQL, empty_table_template_name: str):
    # random name that is yet to be used for creation should not work
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.get(name=fake.unique.pystr(8))
        
    # random version should not work
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.get(name=empty_table_template_name, version="3.0")


def test_get_table_template(client: ThanoSQL, empty_table_template_name: str):
    # latest should return the latest version only
    res = client.table.template.get(name=empty_table_template_name, version="latest")
    assert {"table_templates", "versions"} == set(res.keys())
    assert len(res["table_templates"]) == 1
    assert res["table_templates"][0]["version"] == latest_version
    
    # only the specified version should be returned
    res = client.table.template.get(name=empty_table_template_name, version="1.0")
    assert {"table_templates", "versions"} == set(res.keys())
    assert len(res["table_templates"]) == 1
    assert res["table_templates"][0]["version"] == "1.0"
    
    # without any version in the request, all table templates should be returned
    res = client.table.template.get(name=empty_table_template_name)
    assert {"table_templates", "versions"} == set(res.keys())
    assert len(res["table_templates"]) == 2
    assert len(res["versions"]) == 2
    assert res["versions"][0] == latest_version
    assert res["versions"][1] == "1.0"


def test_get_table_templates_default(client: ThanoSQL):
    res = client.table.template.list()
    assert "table_templates" in res
    # at least the two templates that we made earlier
    assert len(res["table_templates"]) >= 2


def test_delete_table_template_not_found(client: ThanoSQL, empty_table_template_name: str):
    # check that we cannot delete table template with random name
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.delete(name=fake.unique.pystr(8))
        
    # random version should also not work
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.delete(name=empty_table_template_name, version="3.0")


def test_delete_table_template_success(client: ThanoSQL, empty_table_template_name: str):
    # delete a specific version only
    res = client.table.template.delete(name=empty_table_template_name, version=latest_version)
    assert res["table_template_name"] == empty_table_template_name
    
    # now said version should not be found
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.get(name=empty_table_template_name, version=latest_version)
    
    # the other version should still exist
    # no need for comprehensive asserts as it is covered in previous tests already
    res = client.table.template.get(name=empty_table_template_name, version="1.0")
    assert res["versions"][0] == "1.0"
    
    # delete all versions
    res = client.table.template.delete(name=empty_table_template_name)
    assert res["table_template_name"] == empty_table_template_name
    
    # now any table template with this name should not exist
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.template.get(name=empty_table_template_name)
