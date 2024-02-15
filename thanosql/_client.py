from thanosql import API_TOKEN, ENGINE_URL
from thanosql._base_client import ThanoSQLBaseClient


class ThanoSQL(ThanoSQLBaseClient):
    api_token: str
    engine_url: str
    
    def __init__(self, api_token: str = API_TOKEN, engine_url: str = ENGINE_URL) -> None:
        super().__init__()
        
        self.api_token = api_token
        self.engine_url = engine_url
