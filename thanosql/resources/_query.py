from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import TypeAdapter

from thanosql._error import ThanoSQLValueError
from thanosql._service import ThanoSQLService
from thanosql.resources._model import BaseModel

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
    records: Optional[list] = None


class QueryType(enum.Enum):
    THANOSQL = "thanosql"
    PSQL = "psql"


class QueryService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="query")

        self.log: QueryLogService = QueryLogService(self)
        self.template: QueryTemplateService = QueryTemplateService(self)

    def execute(
        self,
        query: Optional[str] = None,
        query_type: str = "thanosql",
        template_id: Optional[int] = None,
        template_name: Optional[str] = None,
        parameters: Optional[dict] = None,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        overwrite: Optional[bool] = None,
        max_results: Optional[int] = None,
    ) -> QueryLog:
        try:
            query_type_enum = QueryType(query_type)
        except Exception as e:
            raise ThanoSQLValueError(str(e))

        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            table_name=table_name,
            overwrite=overwrite,
            max_results=max_results,
        )
        payload = self._create_input_dict(
            query_type=query_type_enum.value,
            query_string=query,
            template_id=template_id,
            template_name=template_name,
            parameters=parameters,
        )

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        query_log_adapter = TypeAdapter(QueryLog)
        parsed_response = query_log_adapter.validate_python(raw_response)
        return parsed_response


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

        query_logs_adapter = TypeAdapter(List[QueryLog])
        parsed_response = {}
        parsed_response["query_logs"] = query_logs_adapter.validate_python(
            raw_response["query_logs"]
        )
        parsed_response["total"] = raw_response["total"]

        return parsed_response


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

    def parse_query_template_response(self, raw_response: dict):
        query_template_adapter = TypeAdapter(QueryTemplate)
        parsed_response = query_template_adapter.validate_python(
            raw_response["query_template"]
        )
        return parsed_response

    def list(
        self,
        search: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[QueryTemplate]:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(
            search=search, offset=offset, limit=limit, order_by=order_by
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        query_templates_adapter = TypeAdapter(List[QueryTemplate])
        parsed_response = query_templates_adapter.validate_python(
            raw_response["query_templates"]
        )
        return parsed_response

    def create(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> QueryTemplate:
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(dry_run=dry_run)
        payload = self._create_input_dict(name=name, query=query)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        return self.parse_query_template_response(raw_response)

    def get(self, name: str) -> QueryTemplate:
        path = f"/{self.query.tag}/{self.tag}/{name}"
        raw_response = self.client._request(method="get", path=path)
        return self.parse_query_template_response(raw_response)

    def update(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> QueryTemplate:
        path = f"/{self.query.tag}/{self.tag}/{current_name}"
        payload = self._create_input_dict(name=new_name, query=query)

        raw_response = self.client._request(method="put", path=path, payload=payload)

        return self.parse_query_template_response(raw_response)

    def delete(self, name: str) -> dict:
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client._request(method="delete", path=path)
