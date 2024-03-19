from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import requests
from tqdm import tqdm

import thanosql._error as thanosql_error

if TYPE_CHECKING:
    from thanosql.resources._file import FileName


class ThanoSQLBaseClient:
    def __init__(self, base_url: str, version: str, token: str) -> None:
        self.base_url: str = base_url.strip("/")
        self.version: str = version
        self.token: str = token

        self.url: str = f"{self.base_url}/api/{version}"

    def _create_auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}

    def _create_full_url(
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

    def _request(
        self,
        method: str,
        path: str,
        path_params: dict | None = None,
        query_params: dict | None = None,
        payload: dict | None = None,
        file: FileName | None = None,
        stream: bool = False,
    ) -> Any:
        full_url = self._create_full_url(
            path=path, path_params=path_params, query_params=query_params
        )

        headers = self._create_auth_header()
        headers["accept"] = "application/json"

        payload_json = {}

        try:
            if file:
                payload_json["files"] = {"file": (file, open(file, "rb"))}
                if payload:
                    payload_json["files"]["body"] = (
                        None,
                        json.dumps(payload),
                        "application/json",
                    )

            elif payload is not None:
                payload_json["json"] = payload

            request_func = getattr(requests, method.lower())
            response = request_func(
                url=full_url, headers=headers, stream=stream, **payload_json
            )

            response_json = {}
            if "application/json" in response.headers.get("Content-Type", ""):
                response_json = response.json()

            if not response.ok or "error" in response_json:
                code = response.status_code
                message = "An error had occurred. Please contact ThanoSQL team."

                if "error" in response_json:
                    code = response_json["error"].get("code", code)
                    message = response_json["error"].get("message", message)

                match code:
                    case 400 | 405 | 422:
                        raise thanosql_error.ThanoSQLValueError(message=message)
                    case 401 | 403:
                        raise thanosql_error.ThanoSQLPermissionError(message=message)
                    case 404:
                        raise thanosql_error.ThanoSQLNotFoundError(message=message)
                    case 409:
                        raise thanosql_error.ThanoSQLAlreadyExistsError(message=message)
                    case 500:
                        raise thanosql_error.ThanoSQLInternalError(message=message)

            if stream:
                filename = response.headers.get(
                    "Content-Disposition", "filename=output.bin"
                ).split("filename=")[1]
                with open(filename, "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                return {"message": f"Successfully downloaded {filename}."}

            if response_json:
                return response_json

            return response
        except thanosql_error.ThanoSQLError as ex:
            raise ex
        except FileNotFoundError as ex:
            raise thanosql_error.ThanoSQLNotFoundError(message=str(ex))
        except PermissionError as ex:
            raise thanosql_error.ThanoSQLPermissionError(message=str(ex))
        except requests.exceptions.JSONDecodeError as ex:
            raise thanosql_error.ThanoSQLValueError(message=str(ex))
        except TypeError as ex:
            raise thanosql_error.ThanoSQLValueError(message=str(ex))
        except Exception as e:
            raise thanosql_error.ThanoSQLInternalError(message=str(e))
