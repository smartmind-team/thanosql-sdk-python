from typing import Any, List

import pandas as pd
from pydantic import BaseModel, model_validator


class Records(BaseModel):
    records: List[dict] = []
    total: int = 0

    @model_validator(mode="before")
    @classmethod
    def convert_records_list_only(cls, data: Any) -> Any:
        if isinstance(data, list):
            return {"records": data, "total": len(data)}
        return data

    def __repr__(self) -> str:
        return self.model_dump_json(indent=4)

    def to_df(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame.from_records(self.records, **kwargs)
