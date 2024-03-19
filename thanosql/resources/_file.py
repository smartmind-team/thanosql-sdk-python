from __future__ import annotations

import os
from typing import TYPE_CHECKING, TypeAlias

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


FileName: TypeAlias = str | bytes | os.PathLike


class FileService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="file")

    def list(self, path: FileName) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(file_path=path)

        return self.client._request(
            method="get", path=api_path, query_params=query_params
        )

    def upload(
        self,
        path: FileName,
        db_commit: bool | None = None,
        table: str | None = None,
        column: str | None = None,
        dir: str | None = None,
    ) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            db_commit=db_commit, table_name=table, column_name=column, dir=dir
        )

        return self.client._request(
            method="post", path=api_path, query_params=query_params, file=path
        )

    def delete(
        self,
        path: FileName,
        db_commit: bool | None = None,
        table: str | None = None,
        column: str | None = None,
    ) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            file_path=path, db_commit=db_commit, table_name=table, column_name=column
        )

        return self.client._request(
            method="delete", path=api_path, query_params=query_params
        )
