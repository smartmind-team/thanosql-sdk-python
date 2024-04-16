from __future__ import annotations

from typing import TYPE_CHECKING

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class SchemaService(ThanoSQLService):
    """Service layer for schema methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="schema")

    def list(self) -> dict:
        """Lists schemas stored in the workspace.

        Does not have any parameters.

        Returns
        -------
        dict
            A dictionary containing the names of stored schemas in the format of::

                {
                    "schemas": [
                        {
                            "name": "string"
                        }
                    ]
                }

        """
        path = f"/{self.tag}/"

        return self.client._request(method="get", path=path)

    def create(self, name: str) -> dict:
        """Creates a new schema in the workspace.

        Parameters
        ----------
        name: str
            The name of the schema to be created.

        Returns
        -------
        dict
            A dictionary containing the name of the created schema and
            a success message in the format of::

                {
                    "schema": "string",
                    "message": "string"
                }

        """
        path = f"/{self.tag}/{name}"

        return self.client._request(method="post", path=path)
