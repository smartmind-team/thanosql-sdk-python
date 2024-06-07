from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from thanosql._error import ThanoSQLNotFoundError, ThanoSQLValueError

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL

dir_name = "test"
file_name = "file_image.jpeg"
relative_file_path = f"{dir_name}/{file_name}"
temp_file_name = "temp"


def test_create_folder(client: ThanoSQL):
    res = client.file.create(dir_name)
    assert res.content_info.name == dir_name
    # we will check whether the folder is created successfully again
    # when we upload a file to it in the next test


def test_upload_file(client: ThanoSQL):
    # nonexistent file in local
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.create(file="random_file")

    res = client.file.create(dir_name, file_name)
    assert res.content_info.path == relative_file_path

    # cannot upload to file as destination
    with pytest.raises(ThanoSQLValueError):
        client.file.create(relative_file_path, file_name)


def test_get_files(client: ThanoSQL):
    # nonexistent file/directory
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.get("random_file")

    res = client.file.get(relative_file_path)
    assert "".join([res.content_info.name, res.content_info.format]) == file_name
    assert res.content_info.path == relative_file_path
    assert res.content_info.type == "file"

    res = client.file.get()
    assert res.content_info.type == "directory"
    for content in res.content_info.content:
        if content["name"] == dir_name:
            return
    raise AssertionError("Test folder not found in root directory")


def test_download_file(client: ThanoSQL):
    # we cannot download a directory
    with pytest.raises(ThanoSQLValueError):
        client.file.get(dir_name, "download")

    # upload a dummy file that we can delete later
    with open(temp_file_name, "w") as f:
        f.write("Hello ThanoSQL!")

    res = client.file.create("/", temp_file_name)
    assert res.content_info.path == temp_file_name

    res = client.file.get(temp_file_name, "download")
    assert os.path.isfile(temp_file_name)

    # cleanup
    os.remove(temp_file_name)


def test_delete_file(client: ThanoSQL):
    client.file.delete(dir_name)
    client.file.delete(temp_file_name)

    # nonexistent file/directory -> we already deleted the folder
    # make sure deletion deletes all folder contents as well
    with pytest.raises(ThanoSQLNotFoundError):
        client.file.delete(relative_file_path)
