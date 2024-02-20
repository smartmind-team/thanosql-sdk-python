from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL
    from thanosql.resources._file import FileName


class SearchService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="search")

    def search_image(
        self,
        table: str,
        model: str,
        column: str,
        text: str | None = None,
        file: FileName | None = None,
        method: str = "text",
    ) -> dict:
        path = f"/{self.tag}/{method}/"
        query_params = self.create_input_dict(
            table_name=table, model_name=model, column_name=column, text=text
        )

        return self.client.request(
            method="get", path=path, query_params=query_params, file=file
        )
