from __future__ import annotations

import os

from thanosql._base_client import ThanoSQLBaseClient
from thanosql.resources import (
    FileService,
    QueryService,
    SchemaService,
    TableService,
    ViewService,
)

THANOSQL_API_TOKEN: str = os.environ.get("THANOSQL_API_TOKEN", "")
THANOSQL_ENGINE_URL: str = os.environ.get("THANOSQL_ENGINE_URL", "")
THANOSQL_API_VERSION: str = "v1"


class ThanoSQL(ThanoSQLBaseClient):
    def __init__(
        self,
        engine_url: str = THANOSQL_ENGINE_URL,
        api_version: str = THANOSQL_API_VERSION,
        api_token: str = THANOSQL_API_TOKEN,
    ) -> None:
        super().__init__(base_url=engine_url, version=api_version, token=api_token)

        self.file: FileService = FileService(self)
        self.query: QueryService = QueryService(self)
        self.schema: SchemaService = SchemaService(self)
        self.table: TableService = TableService(self)
        self.view: ViewService = ViewService(self)
