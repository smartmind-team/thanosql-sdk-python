from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import Field, TypeAdapter

from thanosql._error import ThanoSQLValueError
from thanosql._service import ThanoSQLService
from thanosql.resources._model import BaseModel

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


class TableService(ThanoSQLService):
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

    def get(self, name: str, schema: Optional[str] = None) -> Union[Table, dict]:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        raw_response = self.client._request(
            method="get", path=path, query_params=query_params
        )

        return self._parse_table_response(raw_response)

    def update(
        self, name: str, schema: Optional[str] = None, table: Optional[BaseTable] = None
    ) -> Table:
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
        schema: Optional[str] = None,
        table: Optional[TableObject] = None,
    ) -> Table:
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
        file: Union[str, os.PathLike],
        schema: Optional[str] = None,
        table: Optional[TableObject] = None,
        if_exists: Optional[str] = None,
    ) -> Table:
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

        query_params = self._create_input_dict(schema=schema, if_exists=if_exists)
        payload = self._create_input_dict(table=table)

        raw_response = self.client._request(
            method="post",
            path=path,
            query_params=query_params,
            payload=payload,
            file=file,
        )

        return self._parse_table_response(raw_response)

    def delete(self, name: str, schema: Optional[str] = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(schema=schema)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )


class TableTemplate(BaseModel):
    name: str
    table_template: TableObject
    version: Optional[str]
    compatibility: Optional[str]
    created_at: Optional[datetime]


class TableTemplateService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table_template")

    def list(
        self,
        search: Optional[str] = None,
        order_by: Optional[str] = None,
        latest: Optional[bool] = None,
    ) -> List[TableTemplate]:
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
        path = f"/{self.tag}/{name}"
        query_params = self._create_input_dict(version=version)

        return self.client._request(
            method="delete", path=path, query_params=query_params
        )


class Table(BaseTable):
    service: Optional[TableService] = None

    def get_records(
        self,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        path = f"/{self.service.tag}/{self.name}/records"

        query_params = self.service._create_input_dict(
            schema=self.table_schema,
            offset=offset,
            limit=limit,
        )

        return self.service.client._request(
            method="get",
            path=path,
            query_params=query_params,
        )

    def get_records_as_csv(
        self,
        timezone_offset: Optional[int] = None,
    ) -> None:
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
    ) -> Union[Table, dict]:
        path = f"/{self.service.tag}/{self.name}/records"
        query_params = self.service._create_input_dict(schema=self.table_schema)

        raw_response = self.service.client._request(
            method="post", path=path, query_params=query_params, payload=records
        )

        return self.service._parse_table_response(raw_response)
