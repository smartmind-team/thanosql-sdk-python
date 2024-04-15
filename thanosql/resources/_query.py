from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from pydantic import TypeAdapter

from thanosql._error import ThanoSQLValueError
from thanosql._service import ThanoSQLService
from thanosql.resources._model import BaseModel
from thanosql.resources._record import Records

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
    records: Optional[Records] = None


class QueryType(enum.Enum):
    THANOSQL = "thanosql"
    PSQL = "psql"


class QueryService(ThanoSQLService):
    """Service layer for query methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.
    log: QueryLogService
        The query log service layer to access methods involving query logs.
    template: QueryTemplateService
        The query template service layer to access methods involving query templates.
    """

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
        """Executes a query string.

        There are three ways of requesting a query:
        
        - Using a complete query directly in `query`, leaving
          `template_id`, `template_name`, and `parameters` empty.
        - Using a template query in `query` and completing it with `parameters`,
          leaving `template_id` and `template_name` empty.
        - Recalling a template query from the database using `template_id` or
          `template_name` (but not both) and completing it with `parameters`,
          leaving `query` empty.
        
        One, and only one, of these ways must be chosen. That is, exactly one of
        `query`, `template_id`, or `template_name` must be specified. If none or
        more than one way is selected, an error will occur.

        Parameters
        ----------
        query: str, optional
            The query string or template to be executed.
        query_type: str, default "thanosql"
            The type of the query to be executed. Can only be one of
            "thanosql" or "psql".
        template_id: int, optional
            The ID number of the query template to be used.
            Only relevant when a query template from the database is needed,
            and `query` and `template_name` are not used simultaneously.
        template_name: str, optional
            The name of the query template to be used.
            Only relevant when a query template from the database is needed,
            and `query` and `template_id` are not used simultaneously.
        parameters: dict, optional
            A dictionary of parameter names and values to fill in the template.
            Only relevant when a query template, either from `query` or the database,
            is used.
        schema: str, optional
            The schema of the table to save the query results in.
        table_name: str, optional
            The name of the table to save the query results in.
        overwrite: bool, optional
            Whether to overwrite the table if a table with the same `table_name`
            and `schema` already exists.
        max_results: int, optional
            The maximum number of records to be returned by the response QueryLog.
            If not specified, the `records` part of QueryLog will be None, even
            when the query produces some records.

        Returns
        -------
        QueryLog
            A query log object containing details about the results of the
            executed query.

        """
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
    """Service layer for query log methods.
    
    Cannot exist without a parent QueryService."""

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="log")

        self.query: QueryService = query

    def list(
        self,
        search: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """_summary_

        Parameters
        ----------
        search : Optional[str], optional
            _description_, by default None
        offset : Optional[int], optional
            _description_, by default None
        limit : Optional[int], optional
            _description_, by default None

        Returns
        -------
        dict
            _description_
        """
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

    def _parse_query_template_response(self, raw_response: dict):
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
        """_summary_

        Parameters
        ----------
        search : Optional[str], optional
            _description_, by default None
        offset : Optional[int], optional
            _description_, by default None
        limit : Optional[int], optional
            _description_, by default None
        order_by : Optional[str], optional
            _description_, by default None

        Returns
        -------
        List[QueryTemplate]
            _description_
        """
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
        """_summary_

        Parameters
        ----------
        name : Optional[str], optional
            _description_, by default None
        query : Optional[str], optional
            _description_, by default None
        dry_run : Optional[bool], optional
            _description_, by default None

        Returns
        -------
        QueryTemplate
            _description_
        """
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(dry_run=dry_run)
        payload = self._create_input_dict(name=name, query=query)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        return self._parse_query_template_response(raw_response)

    def get(self, name: str) -> QueryTemplate:
        """_summary_

        Parameters
        ----------
        name : str
            _description_

        Returns
        -------
        QueryTemplate
            _description_
        """
        path = f"/{self.query.tag}/{self.tag}/{name}"
        raw_response = self.client._request(method="get", path=path)
        return self._parse_query_template_response(raw_response)

    def update(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> QueryTemplate:
        """_summary_

        Parameters
        ----------
        current_name : str
            _description_
        new_name : Optional[str], optional
            _description_, by default None
        query : Optional[str], optional
            _description_, by default None

        Returns
        -------
        QueryTemplate
            _description_
        """
        path = f"/{self.query.tag}/{self.tag}/{current_name}"
        payload = self._create_input_dict(name=new_name, query=query)

        raw_response = self.client._request(method="put", path=path, payload=payload)

        return self._parse_query_template_response(raw_response)

    def delete(self, name: str) -> dict:
        """_summary_

        Parameters
        ----------
        name : str
            _description_

        Returns
        -------
        dict
            _description_
        """
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client._request(method="delete", path=path)
