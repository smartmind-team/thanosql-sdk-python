from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional, Union

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class FileService(ThanoSQLService):
    """Service layer for file methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.
    
    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="file")

    def list(self, path: Union[str, os.PathLike]) -> dict:
        """Lists all files and directories under a specified path.
        
        Parameters
        ----------
        path: str or path_like
            The path that contains the files and directories to be listed.
            Regex pattern is recommended.

        Returns
        -------
        dict
            A dictionary containing the list of files and folders under
            the input path in the format:

            ```python
            {
                "data": {
                    "matched_pathnames": [list of matched pathnames],
                }
            }
            ```

        """

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
        dir: Optional[str] = None,
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
        path: Union[str, os.PathLike],
        db_commit: Optional[bool] = None,
        table: Optional[str] = None,
        column: Optional[str] = None,
    ) -> dict:
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            file_path=path, db_commit=db_commit, table_name=table, column_name=column
        )

        return self.client._request(
            method="delete", path=api_path, query_params=query_params
        )
