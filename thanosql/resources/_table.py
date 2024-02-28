from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, TypeAdapter

from thanosql._service import ThanoSQLService

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL
    from thanosql.resources._file import FileName


class TableService(ThanoSQLService):
    template: TableTemplateService

    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table")

        self.template = TableTemplateService(client)

    def list(
        self,
        schema: str | None = None,
        verbose: bool | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict:
        path = f"/{self.tag}/"
        query_params = self.create_input_dict(
            schema=schema, verbose=verbose, offset=offset, limit=limit
        )

        return self.client.request(method="get", path=path, query_params=query_params)

    def get(self, name: str, schema: str | None = None) -> TableServiceObject | dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(schema=schema)

        raw_response = self.client.request(
            method="get", path=path, query_params=query_params
        )

        if "table" in raw_response:
            table_service_adapter = TypeAdapter(TableServiceObject)
            parsed_response = table_service_adapter.validate_python(
                raw_response["table"]
            )
            parsed_response.service = self
            return parsed_response

        return raw_response

    def update(
        self, name: str, schema: str | None = None, table: Table | None = None
    ) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(schema=schema)
        payload = self.create_input_dict(table=table)

        return self.client.request(
            method="put", path=path, query_params=query_params, payload=payload
        )

    def create(
        self, name: str, schema: str | None = None, table: TableObject | None = None
    ) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(schema=schema)
        payload = self.create_input_dict(table=table)

        return self.client.request(
            method="post", path=path, query_params=query_params, payload=payload
        )

    def upload(
        self,
        name: str,
        file: FileName,
        schema: str | None = None,
        table: TableObject | None = None,
        if_exists: str | None = None,
    ) -> dict:
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

        query_params = self.create_input_dict(schema=schema, if_exists=if_exists)
        payload = self.create_input_dict(table=table)

        return self.client.request(
            method="post",
            path=path,
            query_params=query_params,
            payload=payload,
            file=file,
        )

    def delete(self, name: str, schema: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(schema=schema)

        return self.client.request(
            method="delete", path=path, query_params=query_params
        )


class TableTemplateService(ThanoSQLService):
    def __init__(self, client: ThanoSQL) -> None:
        super().__init__(client=client, tag="table_template")

    def list(
        self,
        search: str | None = None,
        order_by: str | None = None,
        latest: bool | None = None,
    ) -> dict:
        path = f"/{self.tag}/"
        query_params = self.create_input_dict(
            search=search,
            order_by=order_by,
            latest=latest,
        )

        return self.client.request(method="get", path=path, query_params=query_params)

    def get(self, name: str, version: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(version=version)

        return self.client.request(method="get", path=path, query_params=query_params)

    def create(
        self,
        name: str,
        table_template: TableObject,
        version: str | None = None,
        compatibility: str | None = None,
    ) -> dict:
        path = f"/{self.tag}/{name}"
        payload = self.create_input_dict(
            table_template=vars(table_template),
            version=version,
            compatibility=compatibility,
        )

        return self.client.request(method="post", path=path, payload=payload)

    def delete(self, name: str, version: str | None = None) -> dict:
        path = f"/{self.tag}/{name}"
        query_params = self.create_input_dict(version=version)

        return self.client.request(
            method="delete", path=path, query_params=query_params
        )


class BaseColumn(BaseModel):
    default: str | None = None
    is_nullable: bool | None = True
    type: str
    name: str


class Column(BaseModel):
    id: int | None = None
    default: str | None = None
    is_nullable: bool | None = True
    type: str
    name: str


class Unique(BaseModel):
    name: str | None = None
    columns: list[str] | None = []


class PrimaryKey(BaseModel):
    name: str | None = None
    columns: list[str] | None = []


class ForeignKey(BaseModel):
    name: str | None = None
    reference_schema: str = "public"
    reference_column: str
    reference_table: str
    column: str


class Constraints(BaseModel):
    unique: list[Unique] | None = None
    primary_key: PrimaryKey | None = None
    foreign_keys: list[ForeignKey] | None = None


class BaseTable(BaseModel):
    name: str | None
    table_schema: str | None = Field(alias="schema")


class Table(BaseTable):
    columns: list[BaseColumn] | None
    constraints: Constraints | None


class TableObject(BaseModel):
    columns: list[BaseColumn] | None = None
    constraints: Constraints | None = None


class TableServiceObject(Table):
    service: TableService | None = None

    class Config:
        arbitrary_types_allowed = True

    def get_records(
        self,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict:
        path = f"/{self.service.tag}/{self.name}/records"

        query_params = self.service.create_input_dict(
            schema=self.table_schema,
            offset=offset,
            limit=limit,
        )

        return self.service.client.request(
            method="get",
            path=path,
            query_params=query_params,
        )

    def get_records_as_csv(
        self,
        timezone_offset: int | None = None,
    ) -> dict:
        path = f"/{self.service.tag}/{self.name}/records/csv"

        query_params = self.service.create_input_dict(
            schema=self.table_schema,
            timezone_offset=timezone_offset,
        )

        return self.service.client.request(
            method="get", path=path, query_params=query_params, stream=True
        )

    def insert(
        self,
        records: list[dict],
    ) -> dict:
        path = f"/{self.service.tag}/{self.name}/records"
        query_params = self.service.create_input_dict(schema=self.table_schema)

        return self.service.client.request(
            method="post", path=path, query_params=query_params, payload=records
        )
