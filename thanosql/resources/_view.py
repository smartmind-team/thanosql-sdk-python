from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, TypeAdapter

from thanosql._service import ThanoSQLService
from thanosql.resources import Column

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class View(BaseModel):
    name: str
    table_schema: str | None = Field(alias="schema", default=None)
    columns: list[Column]
    definition: str


class ViewService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="view")

    def list(
        self,
        schema: str | None = None,
        verbose: bool | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> list[View] | dict:
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            verbose=verbose,
            offset=offset,
            limit=limit,
        )

        raw_response = self.client._request(method="get", path=path, query_params=query_params)
        
        if "views" in raw_response:
            views_service_adapter = TypeAdapter(list[View])
            parsed_response = views_service_adapter.validate_python(
                raw_response["views"]
            )
            return parsed_response

        return raw_response

    def get(self, name: str, schema: str | None = None) -> View | dict:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        raw_response = self.client._request(method="get", path=path, query_params=query_params)
        
        if "view" in raw_response:
            view_service_adapter = TypeAdapter(View)
            parsed_response = view_service_adapter.validate_python(
                raw_response["view"]
            )
            return parsed_response

        return raw_response

    def delete(self, name: str, schema: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )
