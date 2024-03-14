from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.faker import fake
from thanosql._error import (
    ThanoSQLNotFoundError,
    ThanoSQLValueError,
)

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL

column_name = "name"
int_column_name = "price"
# ideally we should delete this dir after testing
# but as there is no API for that, it is somewhat difficult and risky
# so for now we will have a dedicated, fixed folder name for all tests
dir_name = "test"
file_name = "file_image.jpeg"
default_file_path = f"drive/image/{file_name}"


def test_upload_file(client: ThanoSQL, basic_table_name: str):
    # nonexistent dir
    with pytest.raises(ThanoSQLValueError):
        client.file.upload(path=file_name, dir=dir_name)
    
    dir = f"drive/{dir_name}"
    
    # nonexistent table
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.upload(path=file_name, db_commit=True, table=fake.unique.pystr(8), column=column_name, dir=dir)
    
    # nonexistent column
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.upload(path=file_name, db_commit=True, table=basic_table_name, column=fake.unique.pystr(8), dir=dir)
    
    # trying to insert text into integer column
    with pytest.raises(ThanoSQLValueError):
         client.file.upload(path=file_name, db_commit=True, table=basic_table_name, column=int_column_name, dir=dir)
    
    # upload without db_commit and dir
    # note that we cannot check the existence of uploaded files as it requires user_data_root
    # which can technically be saved, but it increases complexity and safety risk
    res = client.file.upload(path=file_name)
    assert res["data"]["file_path"] == default_file_path
    
    # upload with db_commit and dir
    target_table = client.table.get(name=basic_table_name)
    record_count_before = target_table.get_records()["total"]
    
    res = client.file.upload(path=file_name, db_commit=True, table=basic_table_name, column=column_name, dir=dir)
    assert res["data"]["table_name"] == basic_table_name
    assert res["data"]["column_name"] == column_name
    
    record_count_after = target_table.get_records()["total"]
    assert record_count_after == record_count_before + 1


def test_get_files(client: ThanoSQL):
    # path must start with drive/
    with pytest.raises(ThanoSQLValueError):
        client.file.list(path=dir_name)
    
    res = client.file.list(path=f"drive/{dir_name}/*")
    # at least the file we just uploaded
    assert len(res["data"]["matched_pathnames"]) >= 1


def test_delete_file(client: ThanoSQL, basic_table_name: str):
    # path must start with drive/
    with pytest.raises(ThanoSQLValueError):
        client.file.delete(path=dir_name)
        
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.delete(path=f"drive/{fake.unique.pystr(8)}")
    
    # delete file inside default drive dir
    res = client.file.delete(path=default_file_path)
    assert "message" in res
    
    # delete file inside custom dir
    path = f"drive/{dir_name}/{file_name}"
    
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.delete(path=path, db_commit=True, table=fake.unique.pystr(8), column=column_name)
    
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.delete(path=path, db_commit=True, table=basic_table_name, column=fake.unique.pystr(8))
    
    target_table = client.table.get(name=basic_table_name)
    record_count_before = target_table.get_records()["total"]
    
    res = client.file.delete(path=path, db_commit=True, table=basic_table_name, column=column_name)
    assert "message" in res
    
    record_count_after = target_table.get_records()["total"]
    assert record_count_after == record_count_before - 1
