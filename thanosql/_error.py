from typing import Optional


class ThanoSQLError(Exception):
    def __init__(
        self,
        message: Optional[str] = None,
        json_body: Optional[object] = None,
        code: Optional[str] = None,
    ):
        self.message: Optional[str] = message
        self.json_body: Optional[object] = json_body
        self.code: Optional[str] = code

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
