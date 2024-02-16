from __future__ import annotations

import os

from thanosql import resources
from thanosql._base_client import ThanoSQLBaseClient

API_TOKEN: str = os.environ.get("API_TOKEN", "")
API_VERSION: str = os.environ.get("API_VERSION", "v1")
ENGINE_URL: str = os.environ.get("ENGINE_URL", "")


class ThanoSQL(ThanoSQLBaseClient):
    query: resources.QueryService

    def __init__(
        self,
        engine_url: str = ENGINE_URL,
        api_version: str = API_VERSION,
        api_token: str = API_TOKEN,
    ) -> None:
        super().__init__(base_url=engine_url, version=api_version, token=api_token)

        self.query = resources.QueryService(self)
