from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class TableTemplateService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table_template")

    def get_all(self):
        pass

    def get(self):
        pass

    def create(self):
        pass

    def delete(self):
        pass
