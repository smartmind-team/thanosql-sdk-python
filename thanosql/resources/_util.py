from __future__ import annotations

from dateutil.parser import isoparse
from pandas._libs.lib import infer_dtype
from pandas.core.series import Series

from thanosql._error import ThanoSQLValueError
    

# get_sqlachmey_type has been created based on the _sqlalchemy_type function from pandas.io.sql.SQLTable()
def get_sqlalchemy_type(column_values: Series):
    column_value = column_values.iloc[0]
    column_type = infer_dtype(column_values, skipna=True)
    if column_type == "string":
        # if a column_series is a series of ISO-8601 dateimte string, parse it to a datetime format
        if is_date(column_values):
            column_value = isoparse(column_value)
            column_type = infer_dtype([column_value], skipna=True)

    column_value_dtype = type(column_value).__name__

    if column_type == "datetime64" or column_type == "datetime":
        # GH 9086: TIMESTAMP is the suggested type if the column contains
        # timezone information
        try:
            if column_value.dt.tz is not None:
                return "TIMESTAMP"
        except AttributeError:
            # The column is actually a DatetimeIndex
            # GH 26761 or an Index with date-like data e.g. 9999-01-01
            if getattr(column_value, "tz", None) is not None:
                return "TIMESTAMP"
        return "DATE"
    elif column_type == "timedelta64" or column_type == "timedelta":
        raise ThanoSQLValueError("the 'timedelta' type is not supported")
    elif column_type == "floating":
        if column_value_dtype.lower() == "float32":
            # postgresql real type
            return "FLOAT(23)"
        else:
            # postgresql double precision dtype
            return "FLOAT(53)"
    elif column_type == "integer":
        # GH35076 Map pandas integer to optimal SQLAlchemy integer type
        if column_value_dtype.lower() in ("int8", "uint8", "int16"):
            return "SMALLINT"
        elif column_value_dtype.lower() in ("uint16", "int32"):
            return "INT"
        elif column_value_dtype.lower() == "uint64":
            raise ThanoSQLValueError("Unsigned 64 bit integer datatype is not supported")
        else:
            return "BIGINT"
    elif column_type == "mixed":
        if column_value_dtype == "dict":
            return "JSON"
        elif column_value_dtype == "memoryview":
            return "BYTEA"
    elif column_type == "boolean":
        return "BOOLEAN"
    elif column_type == "date":
        return "DATE"
    elif column_type == "time":
        return "TIME"
    elif column_type == "bytes":
        return "BYTEA"
    elif column_type == "complex":
        raise ThanoSQLValueError("Complex datatypes not supported")
    return "TEXT"


# only applicable for an ISO-8601 datetime string series
def is_date(series: Series):
    try:
        if series.str.len().min() >= 8 and not series.str.isdigit().all():
            series.apply(isoparse)
            return True
    except:
        return False
