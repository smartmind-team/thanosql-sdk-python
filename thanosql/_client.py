from __future__ import annotations

import os
from typing import Optional

from thanosql._base_client import ThanoSQLBaseClient
from thanosql._error import ThanoSQLValueError
from thanosql.resources import (
    FileService,
    QueryService,
    SchemaService,
    TableService,
    ViewService,
)


class ThanoSQL(ThanoSQLBaseClient):
    """Client for accessing the ThanoSQL engine.

    Parameters
    ----------
    api_token: str, optional
        The workspace/engine API token. If not supplied, will be retrieved
        from the THANOSQL_API_TOKEN environment variable.
    engine_url: str, optional
        The workspace engine URL. If not supplied, will be retrieved from
        the THANOSQL_ENGINE_URL environment variable.
    api_version: str, default "v1"
        The version of the API used.

    Attributes
    ----------
    query: QueryService
        The service layer for query APIs.
    file: FileService
        The service layer for file APIs.
    schema: SchemaService
        The service layer for schema APIs.
    table: TableService
        The service layer for table APIs.
    view: ViewService
        The service layer for view APIs.

    Raises
    ------
    ThanoSQLValueError
        If API token and/or engine URL is neither passed in as parameters
        nor set as environment variables.

    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        engine_url: Optional[str] = None,
        api_version: str = "v1",
    ) -> None:
        if api_token is None:
            api_token = os.environ.get("THANOSQL_API_TOKEN", "")
        if not api_token:
            raise ThanoSQLValueError(
                "Please input a valid API token. You can do this either by passing it as a parameter or setting the THANOSQL_API_TOKEN environment variable."
            )

        if engine_url is None:
            engine_url = os.environ.get("THANOSQL_ENGINE_URL", "")
        if not engine_url:
            raise ThanoSQLValueError(
                "Please input a valid engine URL. You can do this either by passing it as a parameter or setting the THANOSQL_ENGINE_URL environment variable."
            )

        super().__init__(token=api_token, base_url=engine_url, version=api_version)

    @property
    def query(self) -> QueryService:
        """Access the QueryService."""
        return QueryService(self)

    @property
    def file(self) -> FileService:
        """Access the FileService."""
        return FileService(self)

    @property
    def schema(self) -> SchemaService:
        """Access the SchemaService."""
        return SchemaService(self)

    @property
    def table(self) -> TableService:
        """Access the TableService."""
        return TableService(self)

    @property
    def view(self) -> ViewService:
        """Access the ViewService."""
        return ViewService(self)
