from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class SchemaService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="schema")

    def list(self) -> dict:
        path = f"/{self.tag}/"

        return self.client._request(method="get", path=path)

    def create(self, name: str) -> dict:
        path = f"/{self.tag}/{name}"

        return self.client._request(method="post", path=path)
