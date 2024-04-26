from __future__ import annotations

import enum
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

import pandas as pd
from numpy import nan
from pydantic import Field, TypeAdapter

from thanosql._error import ThanoSQLValueError
from thanosql._service import ThanoSQLService
from thanosql.resources._model import BaseModel
from thanosql.resources._record import Records

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class BaseColumn(BaseModel):
    default: Optional[str] = None
    is_nullable: Optional[bool] = True
    type: str
    name: str


class Column(BaseModel):
    id: Optional[int] = None
    default: Optional[str] = None
    is_nullable: Optional[bool] = True
    type: str
    name: str


class Unique(BaseModel):
    name: Optional[str] = None
    columns: Optional[List[str]] = []


class PrimaryKey(BaseModel):
    name: Optional[str] = None
    columns: Optional[List[str]] = []


class ForeignKey(BaseModel):
    name: Optional[str] = None
    reference_schema: str = "public"
    reference_column: str
    reference_table: str
    column: str


class Constraints(BaseModel):
    unique: Optional[List[Unique]] = None
    primary_key: Optional[PrimaryKey] = None
    foreign_keys: Optional[List[ForeignKey]] = None


class BaseTable(BaseModel):
    name: Optional[str] = None
    table_schema: Optional[str] = Field(alias="schema", default=None)
    columns: Optional[List[BaseColumn]] = None
    constraints: Optional[Constraints] = None


class TableObject(BaseModel):
    columns: Optional[List[BaseColumn]] = None
    constraints: Optional[Constraints] = None


class IfExists(enum.Enum):
    FAIL = "fail"
    APPEND = "append"
    REPLACE = "replace"


class TableService(ThanoSQLService):
    """Service layer for table methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.
    template: TableTemplateService
        The table template service layer to access methods involving
        table templates.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table")

        self.template: TableTemplateService = TableTemplateService(client)

    def _parse_table_response(self, raw_response: dict) -> Table:
        table_adapter = TypeAdapter(Table)
        parsed_response = table_adapter.validate_python(raw_response["table"])
        parsed_response.service = self
        return parsed_response

    def list(
        self,
        schema: Optional[str] = None,
        verbose: Optional[bool] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Table]:
        """Lists tables stored in the workspace.

        Parameters
        ----------
        schema : str, optional
            The schema where the listed tables should reside in. If not set,
            all tables from all schemas will be included.
        verbose : bool, optional
            Whether to include the table columns and constraints in the results.
            By default, or if set to False, only retrieves the names and schemas
            of stored tables.
        offset : int, optional
            When set to n, skips the first n results and excludes them from
            the output list. Otherwise, starts the list from the first result
            stored. Must be greater than 0.
        limit : int, optional
            When set to n, limits the number of results listed to n. Otherwise,
            lists up to 100 results per call. Must range between 0 to 100.

        Returns
        -------
        List[Table]
            A list of Table objects.

        Raises
        ------
        ThanoSQLValueError
            If offset is less than 0 or if limit is not between 0 to 100 (inclusive).

        """
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            schema=schema, verbose=verbose, offset=offset, limit=limit
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        tables_adapter = TypeAdapter(List[Table])
        parsed_response = tables_adapter.validate_python(raw_response["tables"])
        for table in parsed_response:
            table.service = self
        return parsed_response

    def get(self, name: str, schema: Optional[str] = None) -> Table:
        """Shows the details of the specified table.

        Parameters
        ----------
        name : str
            The name of the table to be retrieved.
        schema : str, optional
            The schema where the table to be retrieved is in. If not
            specified, this method will look for the table in "public".

        Returns
        -------
        Table
            A Table object.

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        return self._parse_table_response(raw_response)

    def update(
        self, name: str, schema: Optional[str] = None, table: Optional[BaseTable] = None
    ) -> Table:
        """Updates the specified table.

        Parameters
        ----------
        name : str
            The name of the table to be updated.
        schema : str, optional
            The schema where the table to be updated is in. If not
            specified, this method will look for the table in "public".
        table : BaseTable, optional
            BaseTable object containing changed details of the table to be
            updated. Any attribute of BaseTable can be modified, and if left
            unset, the current value will be maintained after update.
            The attributes are as follows:
            - name: new name to rename the table to
            - schema: new schema to move the table to
            - columns: new columns of the updated table. All new columns
                must be in this object, including columns that already exist
                in the original table. If this attribute is set but some
                original columns are not included, they will be removed from
                the table.
            - constraints: new constraints of the updated table. All new
                constraints must be in this object, including constraints
                that already exist in the original table. If this attribute is
                set but some original constraints are not included, they will
                be removed from the table.

        Returns
        -------
        Table
            Table object of the new table after update.

        Raises
        ------
        ThanoSQLValueError
            If the table object contains invalid formatting.

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)
        payload = self._create_input_dict(table=table)

        raw_response = self.client._request(
            method="put", path=path, query_params=query_params, payload=payload
        )

        return self._parse_table_response(raw_response)

    def create(
        self,
        name: str,
        table: TableObject,
        schema: Optional[str] = None,
    ) -> Table:
        """Creates a new table.

        Parameters
        ----------
        name : str
            The name of the table to be created.
        table : TableObject
            TableObject containing the columns and constraints of the table
            to be created. In order to create an empty table, pass in an empty
            object (TableObject()).
        schema : str, optional
            The schema to save the created table in. If not specified, the table
            will be saved to "public".

        Returns
        -------
        Table
            Table object of the created table.

        Raises
        ------
        ThanoSQLValueError
            If the table object contains invalid formatting.

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)
        payload = self._create_input_dict(table=table)

        raw_response = self.client._request(
            method="post", path=path, query_params=query_params, payload=payload
        )

        return self._parse_table_response(raw_response)

    def upload(
        self,
        name: str,
        file: Optional[Union[str, os.PathLike]] = None,
        df: Optional[pd.DataFrame] = None,
        schema: Optional[str] = None,
        table: Optional[TableObject] = None,
        if_exists: str = "fail",
    ) -> Table:
        """Uploads the contents of a CSV or Excel-like file or Pandas DataFrame
        into the specified table.

        Either a CSV or Excel-like (.xls, .xlsx, .xlsm, .xlsb, .odf, .ods, .odt)
        file or DataFrame must be specified. However, both should not be used
        at the same time.

        Parameters
        ----------
        name : str
            The name of the table created from the file or DataFrame.
        file : str or PathLike, optional
            CSV or Excel-like file containing tabulated data to be uploaded
            to the specified table.
        df : DataFrame, optional
            Pandas DataFrame containing data to be uploaded to the specified
            table.
        schema : str, optional
            The schema to save the created table in. If not specified, the table
            will be saved to "public".
        table : TableObject, optional
            TableObject containing the columns and constraints of the table
            to be created. If specified, the created table will follow the object
            format and no type inference is conducted. Otherwise, type
            inference will be performed and the table will be created to match
            the columns from source.
        if_exists : str, default "fail"
            What to do if table of the same name already exists. There are only
            three available values:
            - fail: fails (throws an error) if the same table exists
            - append: appends records into an existing table (columns must match
                in order to not make an error)
            - replace: deletes existing table and creates a new one with the
                given name

        Returns
        -------
        Table
            Table object of the uploaded table.

        Raises
        ------
        ThanoSQLValueError
            - If if_exists is not one of "fail", "append", or "replace".
            - If neither file nor df is used, or if both are used at the same time.
            - If file is used but it is neither CSV nor Excel-like.
            - If the file or df contains badly-formatted contents.
            - If a table body is specified but does not match the contents of the \
                file or df.
            - If if_exists is set to "append" but the new contents does not match \
                the format of the existing table.

        """
        try:
            if_exists_enum = IfExists(if_exists)
        except Exception as e:
            raise ThanoSQLValueError(str(e))

        if file and df is not None:
            raise ThanoSQLValueError(
                "Cannot use both file and DataFrame for upload at the same time."
            )

        if file:
            path = f"/{self.tag}/{name}/upload/"

            file_extension = Path(file).suffix.lower()
            if file_extension == ".csv":
                path = path + "csv"
            elif file_extension in [
                ".xls",
                ".xlsx",
                ".xlsm",
                ".xlsb",
                ".odf",
                ".ods",
                ".odt",
            ]:
                path = path + "excel"
            else:
                raise ThanoSQLValueError(
                    "Invalid format: only CSV and Excel files possible."
                )

            query_params = self._create_input_dict(
                schema=schema, if_exists=if_exists_enum.value
            )
            payload = self._create_input_dict(table=table)

            raw_response = self.client._request(
                method="post",
                path=path,
                query_params=query_params,
                payload=payload,
                file=file,
            )

            return self._parse_table_response(raw_response)

        elif df is not None:
            path = f"/{self.tag}/{name}/upload/json"

            df = df.replace({nan: None})
            df_json = df.to_dict(orient="records")
            query_params = self._create_input_dict(
                schema=schema, if_exists=if_exists_enum.value
            )
            payload = self._create_input_dict(table=table, data=df_json)

            raw_response = self.client._request(
                method="post",
                path=path,
                query_params=query_params,
                payload=payload,
                file=file,
            )

            return self._parse_table_response(raw_response)

        else:
            raise ThanoSQLValueError("No file or DataFrame provided for upload")

    def delete(self, name: str, schema: Optional[str] = None) -> dict:
        """Deletes the specified table.

        Parameters
        ----------
        name : str
            The name of the table to be deleted.
        schema : str, optional
            The schema where the table to be deleted is in. If not specified,
            this method will look for the table in "public".

        Returns
        -------
        dict
            A dictionary containing a success message, table name, and schema
            in the format of::

                {
                    "message": "string",
                    "table_name": "string",
                    "schema": "string"
                }

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )


class Table(BaseTable):
    """Extends the BaseTable class, which has name, schema,
    columns, and constraints as attributes, with a table service
    layer to allow connection to the ThanoSQL engine.

    """

    service: Optional[TableService] = None
    """The table service layer to access the ThanoSQL client."""

    def get_records(
        self,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Records:
        """Lists the records of the table.

        Parameters
        ----------
        offset : int, optional
            When set to n, skips the first n results and excludes them from
            the output list. Otherwise, starts the list from the first result
            stored. Must be greater than 0.
        limit : int, optional
            When set to n, limits the number of results listed to n. Otherwise,
            lists up to 100 results per call. Must range between 0 to 100.

        Returns
        -------
        Records
            A Records object.

        Raises
        ------
        ThanoSQLValueError
            If offset is less than 0 or if limit is not between 0 to 100 (inclusive).

        """
        path = f"/{self.service.tag}/{self.name}/records"

        query_params = self.service._create_input_dict(
            schema=self.table_schema,
            offset=offset,
            limit=limit,
        )

        res = self.service.client._request(
            method="get",
            path=path,
            query_params=query_params,
        )
        return Records(data=res["records"], total=res["total"])

    def get_records_as_csv(
        self,
        timezone_offset: Optional[int] = None,
    ) -> None:
        """Downloads the records of the table as a CSV file.

        Parameters
        ----------
        timezone_offset : int, optional
            Timezone offset from Coordinated Universal Time (UTC).
            If not set, this value is 9, following the timezone in Seoul.
            This value is used to determine the time used in the file name.

        """
        path = f"/{self.service.tag}/{self.name}/records/csv"

        query_params = self.service._create_input_dict(
            schema=self.table_schema,
            timezone_offset=timezone_offset,
        )

        self.service.client._request(
            method="get", path=path, query_params=query_params, stream=True
        )

    def insert(
        self,
        records: List[dict],
    ) -> Table:
        """Inserts records to the specified table.

        Parameters
        ----------
        records : list of dict
            The records to be inserted in the format of a list of
            column-value pairs.

        Returns
        -------
        Table
            A Table object.

        Raises
        ------
        ThanoSQLValueError
            If the records are in an invalid format or contain invalid contents.

        """
        path = f"/{self.service.tag}/{self.name}/records"
        query_params = self.service._create_input_dict(schema=self.table_schema)

        raw_response = self.service.client._request(
            method="post", path=path, query_params=query_params, payload=records
        )

        return self.service._parse_table_response(raw_response)


class TableTemplate(BaseModel):
    name: str
    table_template: TableObject
    version: Optional[str]
    compatibility: Optional[str]
    created_at: Optional[datetime]


class TableTemplateService(ThanoSQLService):
    """Service layer for table template methods.

    Attributes
    ----------
    client: ThanoSQL
        The ThanoSQL client used to make requests to the engine.

    """

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table_template")

    def list(
        self,
        search: Optional[str] = None,
        order_by: Optional[str] = None,
        latest: Optional[bool] = None,
    ) -> List[TableTemplate]:
        """Lists table templates in the workspace.

        Parameters
        ----------
        search : str, optional
            Search keywords that the table template names in the results must
            contain. If not set, all table templates are returned by default.
        order_by : str, optional
            How to order the results. There are only three possible values:
            - recent: based on the date of creation, from most recent to oldest
            - name_asc: based on the name of the template, from A to Z
            - name_desc: based on the name of the template, from Z to A
        latest : bool, optional
            Whether to return only the latest version of each table template.
            By default, or if set to False, all versions of table templates are
            included in the results.

        Returns
        -------
        List[TableTemplate]
            A list of TableTemplate objects.

        Raises
        ------
        ThanoSQLValueError
            If order_by is not one of "recent", "name_asc", or "name_desc".

        """
        path = f"/{self.tag}/"
        query_params = self._create_input_dict(
            search=search,
            order_by=order_by,
            latest=latest,
        )

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        table_templates_adapter = TypeAdapter(List[TableTemplate])
        parsed_response = table_templates_adapter.validate_python(
            raw_response["table_templates"]
        )
        return parsed_response

    def get(self, name: str, version: Optional[str] = None) -> dict:
        """Shows the details of the specified table template.

        Parameters
        ----------
        name : str
            The name of the table template to be retrieved.
        version : str, optional
            The version of the table template to be retrieved. The value
            can either be a specific version such as "1.0", or "latest".
            If "latest" is specified, only the latest version of the table
            template will be shown. If version is not set, all versions
            will be shown.

        Returns
        -------
        dict
            A dictionary of matching table template(s) in the format of::

                {
                    "table_templates": ["TableTemplate"],
                    "versions": ["string"]
                }

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(version=version)

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        table_templates_adapter = TypeAdapter(List[TableTemplate])
        parsed_response = {}
        parsed_response["table_templates"] = table_templates_adapter.validate_python(
            raw_response["table_templates"]
        )
        parsed_response["versions"] = raw_response["versions"]

        return parsed_response

    def create(
        self,
        name: str,
        table_template: TableObject,
        version: Optional[str] = None,
        compatibility: Optional[str] = None,
    ) -> TableTemplate:
        """Creates a new table template.

        Parameters
        ----------
        name : str
            The name of the table template to be created.
        table_template : TableObject
            TableObject containing the columns and constraints of the table
            template to be created. In order to create an empty table template,
            pass in an empty object (TableObject()).
        version : str, optional
            The version of the table template to be created. It must be in the
            format of "[1-9].[0-9]". If not set, it will default to "1.0".
        compatibility : str, optional
            The compatibility setting of the table template to be created. If not
            set, it will default to "ignore" (no compatibility checks).

        Returns
        -------
        TableTemplate
            TableTemplate object of the created table template.

        Raises
        ------
        ThanoSQLValueError
            - If the template name contains invalid characters or is too long.
            - If version is specified but is not in the right format.
            - If the table template contains formatting errors.

        """
        path = f"/{self.tag}/{name}"
        payload = self._create_input_dict(
            table_template=vars(table_template),
            version=version,
            compatibility=compatibility,
        )

        raw_response = self.client._request(method="post", path=path, payload=payload)

        table_template_adapter = TypeAdapter(TableTemplate)
        parsed_response = table_template_adapter.validate_python(
            raw_response["table_template"]
        )
        return parsed_response

    def delete(self, name: str, version: Optional[str] = None) -> dict:
        """Deletes the specified table template.

        Parameters
        ----------
        name : str
            The name of the table template to be removed.
        version : str, optional
            The version of the table template to be removed. If not specified, all
            versions of the table template will be removed.

        Returns
        -------
        dict
            A dictionary containing a success message and the name of the table
            template in the format of::

                {
                    "message": "string",
                    "table_template_name": "string"
                }

        """
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(version=version)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )
