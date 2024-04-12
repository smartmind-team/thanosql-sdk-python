from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional, Union

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class FileService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="file")

    def list(self, path: Union[str, os.PathLike]) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(search_path=path)

        return self.client._request(
            method="get", path=api_path, query_params=query_params
        )

    def upload(
        self,
        path: Union[str, os.PathLike],
        db_commit: Optional[bool] = None,
        table: Optional[str] = None,
        column: Optional[str] = None,
        schema: Optional[str] = None,
        dir: Optional[str] = None,
    ) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            db_commit=db_commit,
            table_name=table,
            column_name=column,
            schema=schema,
            dir=dir,
        )

        return self.client._request(
            method="post", path=api_path, query_params=query_params, file=path
        )

    def delete(
        self,
        path: Union[str, os.PathLike],
        db_commit: Optional[bool] = None,
        table: Optional[str] = None,
        column: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            file_path=path,
            db_commit=db_commit,
            table_name=table,
            column_name=column,
            schema=schema,
        )

        return self.client._request(
            method="delete", path=api_path, query_params=query_params
        )
