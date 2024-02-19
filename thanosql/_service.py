from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class ThanoSQLService:
    client: ThanoSQL
    tag: str

    def __init__(self, client: ThanoSQL, tag: str = "") -> None:
        self.client = client
        self.tag = tag

    def create_input_dict(self, **kwargs) -> dict:
        input_dict = {}
        for key, value in kwargs.items():
            if value is not None:
                input_dict[key] = value

        return input_dict
