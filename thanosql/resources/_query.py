from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import BaseModel, TypeAdapter

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class QueryLog(BaseModel):
    query_id: Optional[str]
    statement_type: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    query: str
    referer: str
    state: Optional[str]
    destination_table_name: Optional[str]
    destination_schema: Optional[str]
    error_result: Optional[str]
    created_at: Optional[datetime]
    records: Optional[list]


class QueryService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="query")

        self.log: QueryLogService = QueryLogService(self)
        self.template: QueryTemplateService = QueryTemplateService(self)

    def execute(
        self,
        query_type: str = "thanosql",
        query: Optional[str] = None,
        template_id: Optional[int] = None,
        template_name: Optional[str] = None,
        parameters: Optional[dict] = None,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        overwrite: Optional[bool] = None,
        max_results: Optional[int] = None,
    ) -> Union[QueryLog, dict]:
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            table_name=table_name,
            overwrite=overwrite,
            max_results=max_results,
        )
        payload = self._create_input_dict(
            query_type=query_type,
            query_string=query,
            template_id=template_id,
            template_name=template_name,
            parameters=parameters,
        )

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        if "query_id" in raw_response:
            query_log_adapter = TypeAdapter(QueryLog)
            parsed_response = query_log_adapter.validate_python(raw_response)
            return parsed_response

        return raw_response


class QueryLogService(ThanoSQLService):
    """Cannot exist without a parent QueryService"""

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="log")

        self.query: QueryService = query

    def list(
        self,
        search: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(
            search=search, offset=offset, limit=limit
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        if "query_logs" in raw_response:
            query_logs_adapter = TypeAdapter(List[QueryLog])
            parsed_response = {}
            parsed_response["query_logs"] = query_logs_adapter.validate_python(
                raw_response["query_logs"]
            )
            parsed_response["total"] = raw_response["total"]
            return parsed_response

        return raw_response


class QueryTemplate(BaseModel):
    id: Optional[int] = None
    name: str
    query: str
    parameters: Optional[List[str]] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class QueryTemplateService(ThanoSQLService):
    """Cannot exist without a parent QueryService"""

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="template")

        self.query: QueryService = query

    def list(
        self,
        search: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Union[List[QueryTemplate], dict]:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(
            search=search, offset=offset, limit=limit, order_by=order_by
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        if "query_templates" in raw_response:
            query_templates_adapter = TypeAdapter(List[QueryTemplate])
            parsed_response = query_templates_adapter.validate_python(
                raw_response["query_templates"]
            )
            return parsed_response

        return raw_response

    def create(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> Union[QueryTemplate, dict]:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(dry_run=dry_run)
        payload = self._create_input_dict(name=name, query=query)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        if "query_template" in raw_response:
            query_template_adapter = TypeAdapter(QueryTemplate)
            parsed_response = query_template_adapter.validate_python(
                raw_response["query_template"]
            )
            return parsed_response

        return raw_response

    def get(self, name: str) -> Union[QueryTemplate, dict]:
        path = f"/{self.query.tag}/{self.tag}/{name}"

        raw_response = self.client._request(method="get", path=path)

        if "query_template" in raw_response:
            query_template_adapter = TypeAdapter(QueryTemplate)
            parsed_response = query_template_adapter.validate_python(
                raw_response["query_template"]
            )
            return parsed_response

        return raw_response

    def update(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> Union[QueryTemplate, dict]:
        path = f"/{self.query.tag}/{self.tag}/{current_name}"
        payload = self._create_input_dict(name=new_name, query=query)

        raw_response = self.client._request(method="put", path=path, payload=payload)

        if "query_template" in raw_response:
            query_template_adapter = TypeAdapter(QueryTemplate)
            parsed_response = query_template_adapter.validate_python(
                raw_response["query_template"]
            )
            return parsed_response

        return raw_response

    def delete(self, name: str) -> dict:
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client._request(method="delete", path=path)
