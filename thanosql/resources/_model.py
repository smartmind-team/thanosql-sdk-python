import pydantic

from thanosql._service import ThanoSQLService


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True, json_encoders={ThanoSQLService: lambda v: str(v)})
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.model_dump_json(indent=4)})"
