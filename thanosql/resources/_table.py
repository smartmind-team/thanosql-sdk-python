from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class TableService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table")

    def get_all(self):
        pass

    def get(self):
        pass

    def update(self):
        pass

    def create(self):
        pass

    def delete(self):
        pass

    def get_records(self):
        pass

    def insert_records(self):
        pass

    def get_records_csv(self):
        pass

    def upload_from_csv(self):
        pass

    def upload_from_excel(self):
        pass
