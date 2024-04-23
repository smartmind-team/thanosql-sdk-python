from __future__ import annotations

import json
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
        """Lists all files and directories under the specified path.

        Parameters
        ----------
        path: str or path_like
            The path that contains the files and directories to be listed.
            It should begin with 'drive/'. Regex pattern is recommended.

        Returns
        -------
        dict
            A dictionary containing the list of files and folders under
            the input path in the format of::

                {
                    "data": {
                        "matched_pathnames": [list of matched pathnames],
                    }
                }

        Raises
        ------
        ThanoSQLValueError
            If path is not within the 'drive' directory.

        """
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(search_path=path)

        res = self.client._request(
            method="get", path=api_path, query_params=query_params
        )
        matched_pathnames_list = json.loads(res["data"]["matched_pathnames"])
        res["data"]["matched_pathnames"] = matched_pathnames_list

        return res

    def upload(
        self,
        path: Union[str, os.PathLike],
        db_commit: Optional[bool] = None,
        table: Optional[str] = None,
        column: Optional[str] = None,
        schema: Optional[str] = None,
        dir: Optional[str] = None,
        if_exists: Optional[str] = None,
    ) -> dict:
        """Uploads a file to the workspace.

        Parameters
        ----------
        path: str or path_like
            The path to the local file to be uploaded to the workspace.
        db_commit: bool, optional
            Whether to save the uploaded file path to a table or not.
        table: str, optional
            The name of the table to save the uploaded file path in.
            Only relevant if db_commit is set to True.
        column: str, optional
            The column name in the table where the uploaded file path will be
            saved in. Only relevant if db_commit is set to True.
        schema: str, optional
            The schema where the table to save the uploaded file path in resides.
            Only relevant if db_commit is set to True and defaults to "public".
        dir: str, optional
            Path to directory under drive/ to store the uploaded file in.
            If the directory does not exist, it will be created by the API.

        Returns
        -------
        dict
            Dictionary containing values of "file_path", "table_name", "column_name",
            and "schema". "file_path" will always be returned, while the rest are
            only returned if db_commit is set to True. The result is in the format of::

                {
                    "data": {
                        "file_path": file_path,
                        "table_name": table_name | null,
                        "column_name": column_name | null,
                        "schema": schema | null
                    }
                }

        Raises
        ------
        ThanoSQLValueError
            - If dir is not within the 'drive' directory.
            - If the path cannot be saved to table due to integrity or data error.

        """
        api_path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            db_commit=db_commit,
            table_name=table,
            column_name=column,
            schema=schema,
            dir=dir,
            if_exists=if_exists,
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
        """Deletes the specified file from the workspace.

        Parameters
        ----------
        path: str or path_like
            The path to the file to be removed from the workspace.
        db_commit: bool, optional
            Whether to remove the file path from a table or not.
        table: str, optional
            The name of the table where the file path to be removed is in.
            Only relevant if db_commit is set to True.
        column: str, optional
            The column name in the table where the file path to be removed
            is in. Only relevant if db_commit is set to True.
        schema: str, optional
            The schema where the table to remove the file path from resides.
            Only relevant if db_commit is set to True and defaults to "public".

        Returns
        -------
        dict
            A dictionary containing a success message in the format of::

                {
                    "message": "string"
                }

        Raises
        ------
        ThanoSQLValueError
            If path is not within the 'drive' directory.

        """
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
