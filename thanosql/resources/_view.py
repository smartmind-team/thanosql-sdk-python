from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class ViewService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="view")

    def list(
        self,
        schema: str | None = None,
        verbose: bool | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict:
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            verbose=verbose,
            offset=offset,
            limit=limit,
        )

        return self.client._request(method="get", path=path, query_params=query_params)

    def get(self, name: str, schema: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(method="get", path=path, query_params=query_params)

    def delete(self, name: str, schema: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )
