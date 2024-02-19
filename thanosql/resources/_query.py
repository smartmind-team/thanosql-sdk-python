from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class QueryService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="query")

    def post(self):
        pass

    def get_logs(
        self,
        search: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict:
        path = f"/{self.tag}/log"
        query_params = self.create_input_dict(search=search, offset=offset, limit=limit)

        return self.client.request(method="get", path=path, query_params=query_params)

    def get_templates(self):
        pass

    def create_template(self):
        pass

    def get_template(self):
        pass

    def update_template(self):
        pass

    def delete_template(self):
        pass
