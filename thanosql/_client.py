from thanosql import API_TOKEN, API_VERSION, ENGINE_URL
from thanosql._base_client import ThanoSQLBaseClient


class ThanoSQL(ThanoSQLBaseClient):
    def __init__(
        self,
        engine_url: str = ENGINE_URL,
        api_version: str = API_VERSION,
        api_token: str = API_TOKEN,
    ) -> None:
        super().__init__(base_url=engine_url, version=api_version, token=api_token)
