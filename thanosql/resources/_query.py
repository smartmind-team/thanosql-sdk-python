from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class QueryService(ThanoSQLService):
    log: QueryLogService
    template: QueryTemplateService

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="query")

        self.log = QueryLogService(self)
        self.template = QueryTemplateService(self)

    def execute(
        self,
        query_type: str = "thanosql",
        query: str | None = None,
        template_id: int | None = None,
        template_name: str | None = None,
        parameters: dict | None = None,
        schema: str | None = None,
        table_name: str | None = None,
        overwrite: bool | None = None,
        max_results: int | None = None,
    ) -> dict:
        path = f"/{self.tag}/"
        query_params = self.create_input_dict(
            schema=schema,
            table_name=table_name,
            overwrite=overwrite,
            max_results=max_results,
        )
        payload = self.create_input_dict(
            query_type=query_type,
            query_string=query,
            template_id=template_id,
            template_name=template_name,
            parameters=parameters,
        )

        return self.client.request(
            method="post", path=path, query_params=query_params, payload=payload
        )


class QueryLogService(ThanoSQLService):
    """Cannot exist without a parent QueryService"""

    query: QueryService

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="log")

        self.query = query

    def list(
        self,
        search: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self.create_input_dict(search=search, offset=offset, limit=limit)

        return self.client.request(method="get", path=path, query_params=query_params)


class QueryTemplateService(ThanoSQLService):
    """Cannot exist without a parent QueryService"""

    query: QueryService

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="template")

        self.query = query

    def list(
        self,
        search: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        order_by: str | None = None,
    ) -> dict:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self.create_input_dict(
            search=search, offset=offset, limit=limit, order_by=order_by
        )

        return self.client.request(method="get", path=path, query_params=query_params)

    def create(self, name: str, query: str, dry_run: bool | None = None) -> dict:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self.create_input_dict(dry_run=dry_run)
        payload = self.create_input_dict(name=name, query=query)

        return self.client.request(
            method="post", path=path, query_params=query_params, payload=payload
        )

    def get(self, name: str) -> dict:
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client.request(method="get", path=path)

    def update(
        self, current_name: str, new_name: str | None = None, query: str | None = None
    ) -> dict:
        path = f"/{self.query.tag}/{self.tag}/{current_name}"
        payload = self.create_input_dict(name=new_name, query=query)

        return self.client.request(method="put", path=path, payload=payload)

    def delete(self, name: str) -> dict:
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client.request(method="delete", path=path)
