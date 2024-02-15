from thanosql import API_TOKEN, ENGINE_URL
from thanosql._base_client import ThanoSQLBaseClient


class ThanoSQL(ThanoSQLBaseClient):
    def __init__(self, api_token=API_TOKEN, engine_url=ENGINE_URL):
        super().__init__()
        
        self.api_token = api_token
        self.engine_url = engine_url
