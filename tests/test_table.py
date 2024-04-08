from __future__ import annotations

import glob
import logging
import os
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from tests.faker import fake
from thanosql._error import (
    ThanoSQLAlreadyExistsError,
    ThanoSQLNotFoundError,
    ThanoSQLValueError,
)
from thanosql.resources import (
    BaseColumn,
    BaseTable,
    Constraints,
    PrimaryKey,
    Table,
    TableObject,
)

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from thanosql._client import ThanoSQL

test_table_name = f"test_table_{fake.unique.pystr(8).lower()}"
test_table_name_old = test_table_name + "_old"
test_table_name_excel = test_table_name + "_excel"
test_table_name_df = test_table_name + "_df"


def test_get_table_not_found(client: ThanoSQL):
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.get(name=fake.unique.pystr(10))


@pytest.mark.parametrize("name", ["empty_table_name", "basic_table_name"])
def test_get_table_success(client: ThanoSQL, name: str, request: FixtureRequest):
    name = request.getfixturevalue(name)
    res = client.table.get(name=name)
    assert isinstance(res, Table)
    assert res.name == name


def test_create_table_invalid(client: ThanoSQL, empty_table_name: str):
    # creating a table that already exists should not be allowed
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.table.create(name=empty_table_name, schema="public", table=TableObject())

    # creating a table in a nonexistent schema should not be allowed
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.create(
            name=fake.unique.pystr(8), schema=fake.unique.pystr(8), table=TableObject()
        )

    # creating a table with invalid body should not be allowed
    with pytest.raises(ThanoSQLValueError):
        table_object = TableObject(
            columns=[BaseColumn(type="varchar", name="some_column")],
            constraints=Constraints(primary_key=PrimaryKey(columns=["another_column"])),
        )
        client.table.create(name=fake.unique.pystr(8), table=table_object)


def test_create_table_success(client: ThanoSQL, new_schema: str):
    # create a table in a non-"public" schema properly without fixture
    res = client.table.create(
        name=test_table_name_old, schema=new_schema, table=TableObject()
    )
    assert isinstance(res, Table)
    assert res.name == test_table_name_old

    # check that the table is indeed created
    res = client.table.get(name=test_table_name_old, schema=new_schema)
    assert res.name == test_table_name_old


def test_get_tables_default(client: ThanoSQL):
    res = client.table.list()
    assert isinstance(res, list)
    # at least the three tables that we made earlier
    assert len(res) >= 3


def test_get_tables_with_options(client: ThanoSQL, new_schema: str):
    res = client.table.list(schema=new_schema, verbose=True)
    assert isinstance(res, list)
    # we only have one table in this schema
    assert len(res) == 1
    assert isinstance(res[0], Table)


def test_update_table(client: ThanoSQL, new_schema: str):
    table_object = BaseTable(
        name=test_table_name,
        schema="public",
        columns=[BaseColumn(type="integer", name="number")],
        constraints=Constraints(
            primary_key=PrimaryKey(
                name=f"pk_{fake.unique.pystr(8)}", columns=["number"]
            )
        ),
    )
    res = client.table.update(
        name=test_table_name_old, schema=new_schema, table=table_object
    )
    assert isinstance(res, Table)
    assert res.name == test_table_name_old

    # make sure the old table is no longer retrievable
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.get(name=test_table_name_old, schema=new_schema)

    # check that the update is successful
    res = client.table.get(name=test_table_name, schema="public")
    assert res.name == test_table_name
    assert res.table_schema == "public"
    assert len(res.columns) == 1
    assert res.constraints.primary_key.columns == ["number"]

    # move the object back to new_schema for testing
    table_object = BaseTable(schema=new_schema)
    res = client.table.update(name=test_table_name, table=table_object)
    res = client.table.get(name=test_table_name, schema=new_schema)
    assert res.name == test_table_name


def test_insert_records_invalid(client: ThanoSQL, new_schema: str):
    target_table = client.table.get(name=test_table_name, schema=new_schema)

    # inserting unsuitable records should result in IntegrityError
    with pytest.raises(ThanoSQLValueError):
        target_table.insert(records=[{"number": 1, "language": "Python"}])


def test_insert_get_records_success(client: ThanoSQL, new_schema: str):
    target_table = client.table.get(name=test_table_name, schema=new_schema)
    records = [{"number": i + 1} for i in range(5)]
    res = target_table.insert(records=records)

    # check if the records are successfully inserted
    res = target_table.get_records()
    assert {"records", "total"} == set(res.keys())
    assert res["total"] == len(records)
    assert res["records"] == records


def test_upload_table_invalid(client: ThanoSQL, new_schema: str):
    # file or df must be provided
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(name=test_table_name)

    # file and df cannot be provided at the same time
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(
            name=test_table_name, file="file_csv.csv", df=pd.DataFrame()
        )

    # only CSV or Excel accepted
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(name=test_table_name, file="file_txt.txt")

    # schema should exist
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.upload(
            name=test_table_name, file="file_csv.csv", schema=fake.unique.pystr(10)
        )

    # if_exists should be valid
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(
            name=test_table_name, file="file_csv.csv", if_exists="random"
        )

    # if the table exists but if_exists is fail, error should be raised
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.table.upload(
            name=test_table_name, file="file_csv.csv", schema=new_schema
        )

    # if the table exists and if_exists is append but data is not of appropriate format, error should be raised
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(
            name=test_table_name,
            file="file_csv.csv",
            schema=new_schema,
            if_exists="append",
        )

    # if body does not match data, error should be raised
    with pytest.raises(ThanoSQLValueError):
        table_object = TableObject(
            columns=[BaseColumn(type="integer", name="number")],
            constraints=Constraints(primary_key=PrimaryKey(columns=["number"])),
        )
        client.table.upload(
            name=fake.unique.pystr(10), file="file_csv.csv", table=table_object
        )


def test_upload_table_csv(client: ThanoSQL, basic_table_name: str):
    res = client.table.upload(
        name=basic_table_name, file="file_csv.csv", if_exists="replace"
    )
    assert isinstance(res, Table)

    # check that the table and records are indeed uploaded
    target_table = client.table.get(name=basic_table_name)
    assert len(target_table.columns) == 9

    # check get records with limit in the meantime
    limit = 5
    res = target_table.get_records(limit=limit)
    assert len(res["records"]) == limit


def test_upload_table_excel(client: ThanoSQL, new_schema: str):
    # using upload with a suitable body should work
    table_object = TableObject(
        columns=[
            BaseColumn(type="varchar", name="title"),
            BaseColumn(type="double precision", name="price"),
            BaseColumn(type="varchar", name="star_rating"),
        ]
    )

    res = client.table.upload(
        name=test_table_name_excel,
        file="file_excel.xlsx",
        schema=new_schema,
        table=table_object,
    )
    assert isinstance(res, Table)

    # check that the table and records are indeed uploaded
    target_table = client.table.get(name=test_table_name_excel, schema=new_schema)
    assert len(target_table.columns) == 3

    # check get records with limit in the meantime
    limit = 5
    res = target_table.get_records(limit=limit)
    assert len(res["records"]) == limit


# we do more thorough testing for upload with df as it is an SDK-exclusive feature
def test_upload_table_df(client: ThanoSQL):
    df = pd.read_excel("file_excel.xlsx")

    # make sure a new table is created even if if_exists is 'append' if
    # the name is not already taken by another table
    res = client.table.upload(name=test_table_name_df, df=df, if_exists="append")
    assert isinstance(res, Table)

    # get the number of records
    num_records_initial = res.get_records()["total"]

    # make sure records are appended if the table exists
    res = client.table.upload(name=test_table_name_df, df=df, if_exists="append")
    num_records_appended = res.get_records()["total"]
    assert num_records_appended == 2 * num_records_initial

    # make sure creating a table of the same name fails by default
    with pytest.raises(ThanoSQLAlreadyExistsError):
        client.table.upload(name=test_table_name_df, df=df)

    # make sure the table is overwritten if if_exists is 'replace'
    res = client.table.upload(name=test_table_name_df, df=df, if_exists="replace")
    num_records_replaced = res.get_records()["total"]
    assert num_records_replaced == num_records_initial

    table_object = TableObject(
        columns=[BaseColumn(type="integer", name="number")],
        constraints=Constraints(primary_key=PrimaryKey(columns=["number"])),
    )

    # should not succeed if table body does not match cdf
    with pytest.raises(ThanoSQLValueError):
        client.table.upload(
            name=test_table_name_df, df=df, table=table_object, if_exists="replace"
        )

    # check if the created table follows the table object provided (not the df)
    res = client.table.get(test_table_name_df)
    assert len(res.columns) == len(table_object.columns)


def test_get_records_as_csv(client: ThanoSQL, new_schema: str):
    target_table = client.table.get(name=test_table_name_excel, schema=new_schema)
    target_table.get_records_as_csv()

    # check that the csv file is created
    csv_files = glob.glob("*[!file_csv].csv")
    assert len(csv_files) >= 1

    # check the contents of each csv file and remove it when done
    for csv_file in csv_files:
        with open(file=csv_file, mode="rb") as f:
            lines = f.readlines()
            assert len(lines) == 21

        try:
            os.remove(csv_file)
        except OSError:
            logging.info(f"{csv_file} is already removed")

    # we can technically check to see whether the contents of file_excel.xlsx and the recently-created csv are the same
    # using pandas or other library, but it adds more requirements and requires some time and resources
    # for now, we will just rely on whether a non-empty new csv file is created or not


def test_delete_table(client: ThanoSQL, new_schema: str):
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.delete(name=test_table_name, schema=fake.unique.pystr(10))

    # the other two tables will be automatically deleted when fixtures are destroyed
    # so we just need to "manually" delete these
    client.table.delete(name=test_table_name, schema=new_schema)
    client.table.delete(name=test_table_name_excel, schema=new_schema)
    client.table.delete(name=test_table_name_df)

    # check that the table is indeed deleted
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.get(name=test_table_name, schema=new_schema)

    # trying to delete the table again should not be allowed
    with pytest.raises(ThanoSQLNotFoundError):
        client.table.delete(name=test_table_name, schema=new_schema)
