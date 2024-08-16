import json


def _stringify_list(val: object) -> str:
    # If we use str(lst) to show the string representation of a list directly,
    # there can be unwanted extra quotes. e.g. ['a', 'b'] -> ["'a'", "'b'"]
    # so we have to join list elements manually; this also covers nested lists
    if isinstance(val, list):
        return f"[{', '.join([_stringify_list(item) for item in val])}]"
    return str(val)


def _to_postgresql_value_helper(val: object) -> object:
    # Helper function to convert Python objects into their PSQL representation
    # First, recursively apply the function to each element of lists
    if isinstance(val, list):
        return [_to_postgresql_value_helper(item) for item in val]

    # Convert Python None values to PSQL NULL (without single quotes)
    if val is None:
        return "NULL"

    # If the object is a dictionary, convert it to string since we are going
    # to use its string representation in our PSQL query
    if isinstance(val, dict):
        val = json.dumps(val)

    # If the object is a string (including the stringified dictionary above),
    # replace quoted single quotes (') from \' to '' (C-style to PSQL-style)
    # Double quotes (") need not be replaced as they are treated as a normal
    # character in PSQL strings, and are also C-style escaped in JSONs
    # Then, enclose string/varchar values in single quotes (this includes
    # JSON/dictionary objects)
    if isinstance(val, str):
        quoted_val = val.replace("\\'", "'").replace("'", "''")
        return f"'{quoted_val}'"

    # For other types of objects, return it as it is
    return val


def to_postgresql_value(val: object) -> object:
    val = _to_postgresql_value_helper(val)
    # There are two possible array representations in PSQL,
    # '{{...}, {...}}' and ARRAY[[...], [...]]
    # For empty arrays/lists, we will use the '{}' notation
    # For non-empty arrays/lists, we will use the ARRAY[[...]] notation
    if isinstance(val, list):
        if len(val) == 0:
            return "'{}'"
        return f"ARRAY{_stringify_list(val)}"
    # All other values do not need special treatment
    return val
