from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from pydantic import Field, TypeAdapter

from thanosql._service import ThanoSQLService
from thanosql.resources import Column
from thanosql.resources._model import BaseModel

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class View(BaseModel):
    name: str
    table_schema: Optional[str] = Field(alias="schema", default=None)
    columns: List[Column] = []
    definition: str = ""


class ViewService(ThanoSQLService):
    """Service layer for view methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="view")

    def list(
        self,
        schema: Optional[str] = None,
        verbose: Optional[bool] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[View]:
        """Lists views stored in the workspace.

        Parameters
        ----------
        schema: str, optional
            When specified, lists views from a specific schema only.
            Otherwise, lists views from all available schemas.
        verbose: bool, optional
            When not set or set to False (default behavior), only lists
            view names and the schema they belong to. When set to True,
            includes column descriptions and view definition in the output
            on top of the basic (name and schema) information.
        offset: int, optional
            When set to n, skips the first n views and excludes them from
            the output list. Otherwise, starts the list from the first view
            stored. Must be greater than 0.
        limit: int, optional
            When set to n, limits the number of views listed to n. Otherwise,
            lists up to 100 views per call. Must range between 0 to 100.

        Returns
        -------
        List[View]
            A list of View objects.

        Raises
        ------
        ThanoSQLPermissionError
            If an invalid API token is provided.
        ThanoSQLValueError
            If offset is less than 0 or if limit is not between 0 to 100 (inclusive).
        ThanoSQLNotFoundError
            If schema is not found.
        ThanoSQLInternalError
            If an error happens while fetching views from the database.

        """
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema,
            verbose=verbose,
            offset=offset,
            limit=limit,
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        views_adapter = TypeAdapter(List[View])
        parsed_response = views_adapter.validate_python(raw_response["views"])
        return parsed_response

    def get(self, name: str, schema: Optional[str] = None) -> View:
        """Shows the details of a view stored in the workspace.

        Parameters
        ----------
        name: str
            The name of the view to be retrieved.
        schema: str, optional
            The schema the target view is stored in. When not specified,
            defaults to "public".

        Returns
        -------
        View
            A View object.

        Raises
        ------
        ThanoSQLPermissionError
            If an invalid API token is provided.
        ThanoSQLNotFoundError
            If schema or view is not found.
        ThanoSQLInternalError
            If an error happens while fetching the view from the database.

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        view_adapter = TypeAdapter(View)
        parsed_response = view_adapter.validate_python(raw_response["view"])
        return parsed_response

    def delete(self, name: str, schema: Optional[str] = None) -> dict:
        """Deletes a view from the workspace.

        Parameters
        ----------
        name: str
            The name of the view to be deleted.
        schema: str, optional
            The schema the target view is stored in. When not specified,
            defaults to "public".

        Returns
        -------
        dict
            A dictionary containing a success message and the name of the
            deleted view in the format of::

                {
                    "message": "string",
                    "view_name": "string"
                }

        Raises
        ------
        ThanoSQLPermissionError
            If an invalid API token is provided.
        ThanoSQLNotFoundError
            If schema or view is not found.
        ThanoSQLInternalError
            If an error happens while deleting the view from the database.

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )
