from thanosql._base_client import ThanoSQLBaseClient


class ThanoSQLConsole(ThanoSQLBaseClient):
    def __init__(self, email, password):
        super().__init__()

        self.email = email
        self.password = password
