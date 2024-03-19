class ThanoSQLError(Exception):
    def __init__(
        self,
        message: str | None = None,
        json_body: object | None = None,
        code: str | None = None,
    ):
        self.message: str | None = message
        self.json_body: object | None = json_body
        self.code: str | None = code

    def __str__(self):
        msg = self.message or "<empty message>"
        return msg


class ThanoSQLAlreadyExistsError(ThanoSQLError):
    pass


class ThanoSQLConnectionError(ThanoSQLError):
    pass


class ThanoSQLInternalError(ThanoSQLError):
    pass


class ThanoSQLNotFoundError(ThanoSQLError):
    pass


class ThanoSQLPermissionError(ThanoSQLError):
    pass


class ThanoSQLValueError(ThanoSQLError):
    pass
