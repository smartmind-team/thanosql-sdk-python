from __future__ import annotations

from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from thanosql.resources._file import FileName


class ThanoSQLBaseClient:
    base_url: str
    url: str
    version: str
    token: str

    def __init__(self, base_url: str, version: str, token: str) -> None:
        self.base_url = base_url.strip("/")
        self.version = version
        self.token = token

        self.url = f"{self.base_url}/api/{version}"

    def create_auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}

    def create_full_url(
        self,
        path: str = "",
        path_params: dict | None = None,
        query_params: dict | None = None,
    ) -> str:
        url = self.url + path

        if path_params:
            for param, value in path_params.items():
                url = url.replace(param, value)

        if query_params:
            query_params_list = []
            for param, value in query_params.items():
                query_params_list.append(f"{param}={value}")
            query_params_string = "&".join(query_params_list)
            url = f"{url}?{query_params_string}"

        return url

    def request(
        self,
        method: str,
        path: str,
        path_params: dict | None = None,
        query_params: dict | None = None,
        payload: dict | None = None,
        file: FileName | None = None,
    ) -> Any:
        full_url = self.create_full_url(
            path=path, path_params=path_params, query_params=query_params
        )

        header = self.create_auth_header()

        payload_json = {}

        if payload:
            payload_json["json"] = payload

        if file:
            payload_json["file"] = (file, open(file, "rb"))

        request_func = getattr(requests, method.lower())

        return request_func(url=full_url, headers=header, **payload_json)
