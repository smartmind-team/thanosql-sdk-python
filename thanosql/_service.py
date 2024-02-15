from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._client import ThanoSQL


class ThanoSQLService:
    client: ThanoSQL
    tag: str

    def __init__(self, client: ThanoSQL, tag: str = "") -> None:
        self.client = client
        self.tag = tag
