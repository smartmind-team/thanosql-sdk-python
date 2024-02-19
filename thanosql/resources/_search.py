from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class SearchService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="search")

    def search_by_text(self):
        pass

    def search_by_file(self):
        pass
