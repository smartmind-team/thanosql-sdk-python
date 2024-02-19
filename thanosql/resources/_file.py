from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class FileService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="file")

    def get_all(self):
        pass

    def upload(self):
        pass

    def delete(self):
        pass
