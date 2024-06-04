from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, Union

from pydantic import TypeAdapter

from thanosql._service import ThanoSQLService
from thanosql.resources._model import BaseModel

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class ContentInfo(BaseModel):
    name: str
    format: Optional[str] = None
    path: str
    type: str
    size: Optional[int] = None
    writable: bool
    content: Any = None
    created_at: datetime
    updated_at: datetime


class Content(BaseModel):
    content_info: ContentInfo
    root: str


class Size(BaseModel):
    max_size: int
    used_size: int
    avail_size: int


class FileService(ThanoSQLService):
    """Service layer for file methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="fm")

    def _parse_file_content_response(self, raw_response: dict):
        file_content_adapter = TypeAdapter(Content)
        parsed_response = file_content_adapter.validate_python(raw_response)
        return parsed_response

    def get(
        self,
        path: Optional[Union[str, os.PathLike]] = None,
        option: Optional[str] = None,
    ) -> Optional[Content]:
        """Details the information of a file or directory in the specified path.

        Parameters
        ----------
        path: str or path_like
            The path to the file/directory relative to the user data root
            (default value is "/", the user data root directory itself)
        option: controls the behavior of the API
            - default (None): retrieves file/directory information
            - download: downloads a file (directory download is not possible)

        Returns
        -------
        Depending on option:
        - default (None): Returns a Content Pydantic object containing
            information on the target path
        - download: Downloads the requested file and returns nothing

        Raises
        ------
        ThanoSQLValueError
            If user attempts to download a directory (when option="download")

        """
        path_prefix = "fm"
        api_path = "/contents/"
        if path:
            api_path = api_path + str(path)
        query_params = self._create_input_dict(option=option)

        if option == "download":
            self.client._request(
                method="get",
                path=api_path,
                path_prefix=path_prefix,
                query_params=query_params,
                stream=True,
            )
        else:
            res = self.client._request(
                method="get",
                path=api_path,
                path_prefix=path_prefix,
                query_params=query_params,
            )
            return self._parse_file_content_response(res)

    def create(
        self,
        path: Optional[Union[str, os.PathLike]] = None,
        file: Optional[Union[str, os.PathLike]] = None,
    ) -> Content:
        """Uploads a file to the workspace or creates an empty folder.

        Parameters
        ----------
        path: str or path_like
            The destination path of the upload/folder creation relative to the user data root.
            Should point to a (would-be) directory (default value is "/", the user data root directory itself)
        file : str or PathLike, optional
            The file to be uploaded. If left empty, creates an empty folder in the given path.

        Returns
        -------
        A Content Pydantic object containing information on the created file/folder.

        Raises
        ------
        ThanoSQLValueError
            If path does not point to a (would-be) directory

        """
        path_prefix = "fm"
        api_path = "/contents/"
        if path:
            api_path = api_path + str(path)

        res = self.client._request(
            method="post", path=api_path, path_prefix=path_prefix, file=file
        )

        return self._parse_file_content_response(res)

    def delete(
        self,
        path: Union[str, os.PathLike],
    ) -> None:
        """Deletes the specified file or directory permanently from the workspace.

        Parameters
        ----------
        path: str or path_like
            The path to the file or directory to be removed from the workspace.
            Relative to the user data root, no default value.

        Returns
        -------
        No content

        Raises
        ------

        """
        path_prefix = "fm"
        api_path = f"/contents/{str(path)}"

        return self.client._request(
            method="delete", path=api_path, path_prefix=path_prefix
        )
