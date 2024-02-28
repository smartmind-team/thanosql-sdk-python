class ThanoSQLError(Exception):
    message: str | None
    json_body: object | None
    code: str | None

    def __init__(
        self,
        message: str | None = None,
        json_body: object | None = None,
        code: str | None = None,
    ):
        self.message = message
        self.json_body = json_body
        self.code = code

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
