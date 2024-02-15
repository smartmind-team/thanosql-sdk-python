from typing import Any

import requests


class ThanoSQLBaseClient:
    base_url: str
    url: str
    version: str
    token: str

    def __init__(self, base_url: str, version: str, token: str) -> None:
        self.base_url = base_url
        self.version = version
        self.token = token

        self.url = f"{base_url}/api/{version}"

    def create_auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}

    def create_full_url(
        self,
        path: str = "",
        path_params: dict | None = None,
        query_params: dict | None = None,
    ) -> str:
        url = self.url + path

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
    ) -> Any:
        full_url = self.create_full_url(
            path=path, path_params=path_params, query_params=query_params
        )

        payload_json = {}
        if payload:
            payload_json = {"json": payload}

        request_func = getattr(requests, method.lower())

        return request_func(url=full_url, **payload_json)
