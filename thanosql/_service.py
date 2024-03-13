from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from thanosql._client import ThanoSQL


class ThanoSQLService:
    client: ThanoSQL
    tag: str

    def __init__(self, client: ThanoSQL, tag: str = "") -> None:
        self.client = client
        self.tag = tag

    def convert_obj_to_dict(self, obj: object) -> dict:
        try:
            model_dump = obj.model_dump_json(by_alias=True)
        except:
            model_dump = json.dumps(obj, default=lambda o: o.__dict__)
        return json.loads(model_dump)

    def create_input_dict(self, **kwargs) -> dict:
        input_dict = {}
        for key, value in kwargs.items():
            if value is not None:
                if hasattr(value, "__dict__"):
                    value = self.convert_obj_to_dict(value)
                input_dict[key] = value

        return input_dict
