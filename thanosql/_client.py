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
    def __init__(
        self,
        engine_url: Optional[str] = None,
        api_token: Optional[str] = None,
        api_version: str = "v1",
    ) -> None:
        if engine_url is None:
            engine_url = os.environ.get("THANOSQL_ENGINE_URL", "")
        if not engine_url:
            raise ThanoSQLValueError(
                "Please input a valid engine URL. You can do this either by passing it as a parameter or setting the THANOSQL_ENGINE_URL environment variable."
            )

        if api_token is None:
            api_token = os.environ.get("THANOSQL_API_TOKEN", "")
        if not api_token:
            raise ThanoSQLValueError(
                "Please input a valid API token. You can do this either by passing it as a parameter or setting the THANOSQL_API_TOKEN environment variable."
            )

        super().__init__(base_url=engine_url, version=api_version, token=api_token)

        self.file: FileService = FileService(self)
        self.query: QueryService = QueryService(self)
        self.schema: SchemaService = SchemaService(self)
        self.table: TableService = TableService(self)
        self.view: ViewService = ViewService(self)
