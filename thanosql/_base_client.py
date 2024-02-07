class ThanoSQLBaseClient(object):
    def __init__(self, api_token, engine_url):
        self.api_token = api_token
        self.engine_url = engine_url
