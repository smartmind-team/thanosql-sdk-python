from __future__ import annotations

import os

from thanosql import resources
from thanosql._base_client import ThanoSQLBaseClient

API_TOKEN: str = os.environ.get("API_TOKEN", "")
ENGINE_URL: str = os.environ.get("ENGINE_URL", "")
API_VERSION: str = "v1"


class ThanoSQL(ThanoSQLBaseClient):
    def __init__(
        self,
        engine_url: str = ENGINE_URL,
        api_version: str = API_VERSION,
        api_token: str = API_TOKEN,
    ) -> None:
        super().__init__(base_url=engine_url, version=api_version, token=api_token)

        self.file: resources.FileService = resources.FileService(self)
        self.query: resources.QueryService = resources.QueryService(self)
        self.schema: resources.SchemaService = resources.SchemaService(self)
        self.table: resources.TableService = resources.TableService(self)
        self.view: resources.ViewService = resources.ViewService(self)
