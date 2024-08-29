from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import TypeAdapter

from thanosql._error import ThanoSQLValueError
from thanosql._service import ThanoSQLService
from thanosql._util import fill_query_placeholder
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


class QueryService(ThanoSQLService):
    """Service layer for query methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.
    log: QueryLogService
        The query log service layer to access methods involving query logs.
    template: QueryTemplateService
        The query template service layer to access methods involving
        query templates.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="query")

        self.log: QueryLogService = QueryLogService(self)
        self.template: QueryTemplateService = QueryTemplateService(self)

    def execute(
        self,
        query: Optional[str] = None,
        template_id: Optional[int] = None,
        template_name: Optional[str] = None,
        parameters: Optional[dict] = None,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        overwrite: Optional[bool] = None,
        max_results: int = 100,
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
            The schema of the table to save the query results in. If not specified
            and table_name is also empty, it defaults to "qm". If table_name is
            specified, it defaults to "public".
        table_name: str, optional
            The name of the table to save the query results in. If not specified
            while the query produces a result, an automatic table name will be given.
        overwrite: bool, optional
            Whether to overwrite the table if a table with the same `table_name`
            and `schema` already exists. If not specified, the value is False.
        max_results: int, optional
            The maximum number of records to be returned by the response QueryLog.
            If not specified, it defaults to 100.

        Returns
        -------
        QueryLog
            A query log object containing details about the results of the
            executed query.

        Raises
        ------
        ThanoSQLValueError
            - If invalid input combination is provided; that is:

                - query, template_id, and template_name are all empty, or
                - more than one of query, template_id, and template_name are non-empty
            - If max_results is not between 0 and 100 (inclusive).
            - If query and parameters are used but the template has invalid format.
            - If rendering query template by substituting in parameters fails, either \
                by direct query template or templates from the database.

        """
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            table_name=table_name,
            overwrite=overwrite,
            max_results=max_results,
        )
        payload = self._create_input_dict(
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

    def execute_values(
        self,
        query: str,
        values: Union[List[tuple], List[dict]],
        template: Optional[str] = None,
        page_size: int = 100,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        overwrite: Optional[bool] = None,
        max_results: int = 100,
    ) -> QueryLog:
        """Executes a query string with a value placeholder.

        Unlike execute, query templates cannot be used, as this might be confusing
        when combined with value placeholders. Following psycopg, only a single main
        placeholder is allowed. If multiple placeholders are required, users should
        use templates. Unlike psycopg, we will use jinja formatting for consistency
        with query template purposes.

        Parameters
        ----------
        query: str
            The query string with a "{{ val }}" placeholder to be executed.
        values: list of tuples
            List of values to be inserted to the placeholder(s).
        template: str, optional
            How values should be formatted inside {{ val }}. If it is left
            empty, values will just be listed inside brackets separated by commas.
            In this case, values should be a list of tuples. For example, we have
            the following values:
            ```
            [(1, "gangnam", "seoul"), (2, "haeundae", "busan")]
            ```
            and we leave template empty. The original query looks like
            ```
            VALUES
                {{ val }}
            ```
            The rendered query will look like
            ```
            VALUES
                (1, 'gangnam', 'seoul'),
                (2, 'haeundae', 'busan')
            ```
            Meanwhile, if we have the following named values:
            ```
            [
                {"id": 1, "name": "gangnam", "city": "seoul"},
                {"id": 2, "name": "haeundae", "city": "busan"},
            ]
            ```
            and we have the following template:
            ```
            ({{ id }}, CONCAT(INITCAP({{ name }}), '-gu'), INITCAP({{ city }}))
            ```
            the rendered query will be:
            ```
            VALUES
                (1, CONCAT(INITCAP('gangnam', '-gu')), INITCAP('seoul')),
                (2, CONCAT(INITCAP('haeundae', '-gu')), INITCAP('busan'))
            ```
        page_size: int, default 100
            The maximum number of rows (set of values) to be sent in one statement.
            If not specified, it defaults to 100.
        schema: str, optional
            The schema of the table to save the query results in. If not specified
            and table_name is also empty, it defaults to "qm". If table_name is
            specified, it defaults to "public".
        table_name: str, optional
            The name of the table to save the query results in. If not specified
            while the query produces a result, an automatic table name will be given.
        overwrite: bool, optional
            Whether to overwrite the table if a table with the same `table_name`
            and `schema` already exists. If not specified, the value is False.
        max_results: int, default 100
            The maximum number of records to be returned by the response QueryLog.
            If not specified, it defaults to 100.

        Returns
        -------
        QueryLog
            A query log object containing details about the results of the
            executed query.

        Raises
        ------
        ThanoSQLValueError
            - If values is empty.
            - If page_size is not between 1 and 100 (inclusive).
            - If max_results is not between 0 and 100 (inclusive).
            - If the formats of template and values don't match.

        """
        if len(values) == 0:
            raise ThanoSQLValueError("Values cannot be empty")

        if page_size < 1 or page_size > 100:
            raise ThanoSQLValueError(
                "Page size can only range from 1 to 100 (inclusive)"
            )

        completed_query = fill_query_placeholder(query, values, template, page_size)

        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            table_name=table_name,
            overwrite=overwrite,
            max_results=max_results,
        )
        payload = self._create_input_dict(query_string=completed_query)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        query_log_adapter = TypeAdapter(QueryLog)
        parsed_response = query_log_adapter.validate_python(raw_response)
        return parsed_response


class QueryLogService(ThanoSQLService):
    """Service layer for query log methods.

    Cannot exist without a parent QueryService.

    Attributes
    ----------
    query: QueryService
        The parent QueryService to connect to the ThanoSQL client.

    """

    def __init__(self, query: QueryService) -> None:
        super().__init__(client=query.client, tag="log")

        self.query: QueryService = query

    def list(
        self,
        search: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """Lists the details of stored query logs.

        Parameters
        ----------
        search : str, optional
            Search keywords that the query strings in the results must contain.
            If not set, all query logs are returned by default.
        offset : int, optional
            When set to n, skips the first n results and excludes them from
            the output list. Otherwise, starts the list from the first result
            stored. Must be greater than 0.
        limit : int, optional
            When set to n, limits the number of results listed to n. Otherwise,
            lists up to 100 results per call. Must range between 0 to 100.

        Returns
        -------
        dict
            A dictionary of results in the format of::

                {
                    "query_logs": ["QueryLog"],
                    "total": 0
                }

        Raises
        ------
        ThanoSQLValueError
            If offset is less than 0 or if limit is not between 0 to 100 (inclusive).

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
    """Service layer for query template methods.

    Cannot exist without a parent QueryService.

    Attributes
    ----------
    query: QueryService
        The parent QueryService to connect to the ThanoSQL client.

    """

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
        """Lists query templates stored in the workspace.

        Parameters
        ----------
        search : str, optional
            Search keywords that the query strings in the results must contain.
            If not set, all query templates are returned by default.
        offset : int, optional
            When set to n, skips the first n results and excludes them from
            the output list. Otherwise, starts the list from the first result
            stored. Must be greater than 0.
        limit : int, optional
            When set to n, limits the number of results listed to n. Otherwise,
            lists up to 100 results per call. Must range between 0 to 100.
        order_by : str, optional
            How to order the results. There are only three possible values:

            - recent: based on the date of creation, from most recent to oldest
            - name_asc: based on the name of the template, from A to Z
            - name_desc: based on the name of the template, from Z to A

        Returns
        -------
        List[QueryTemplate]
            A list of QueryTemplate objects.

        Raises
        ------
        ThanoSQLValueError
            - If offset is less than 0 or if limit is not between 0 to 100 (inclusive).
            - If order_by is not one of "recent", "name_asc", or "name_desc".

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
        """Creates a new query template.

        Parameters
        ----------
        name : str, optional
            The name of the query template to be created. If left empty, an
            automatic name will be given.
        query : str, optional
            The string contents of the template.
        dry_run : bool, optional
            Whether to "dry run" the template creation or save it to database.
            When set to True, only shows whether the query template is valid
            or not without actually storing it. By default, created query
            templates will be saved to the workspace database.

        Returns
        -------
        QueryTemplate
            QueryTemplate object of the created query template.

        Raises
        ------
        ThanoSQLValueError
            - If the template name contains invalid characters or is too long.
            - If the query template contains invalid formatting.

        """
        path = f"/{self.query.tag}/{self.tag}"
        query_params = self._create_input_dict(dry_run=dry_run)
        payload = self._create_input_dict(name=name, query=query)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        return self._parse_query_template_response(raw_response)

    def get(self, name: str) -> QueryTemplate:
        """Shows the details of the specified query template.

        Parameters
        ----------
        name : str
            The name of the query template to be retrieved.

        Returns
        -------
        QueryTemplate
            A QueryTemplate object.

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
        """Updates the specified query template.

        Parameters
        ----------
        current_name : str
            The current name of the query template to be updated.
        new_name : str, optional
            The new name of query template after update. If not set,
            the name will not be changed.
        query : str, optional
            The query contents of the query template after update. If
            not set, the query string will not be changed.

        Returns
        -------
        QueryTemplate
            QueryTemplate object of the new query template after update.

        Raises
        ------
        ThanoSQLValueError
            - If the new template name is set but is empty or null, contains invalid \
                characters, or is too long.
            - If the new query template is set but is null or contains invalid formatting.

        """
        path = f"/{self.query.tag}/{self.tag}/{current_name}"
        payload = self._create_input_dict(name=new_name, query=query)

        raw_response = self.client._request(method="put", path=path, payload=payload)

        return self._parse_query_template_response(raw_response)

    def delete(self, name: str) -> dict:
        """Deletes the specified query template.

        Parameters
        ----------
        name : str
            The name of the query template to be deleted.

        Returns
        -------
        dict
            A dictionary containing a success message in the format of::

                {
                    "message": "string"
                }

        """
        path = f"/{self.query.tag}/{self.tag}/{name}"

        return self.client._request(method="delete", path=path)
