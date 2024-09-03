import json
from typing import Iterable, List, Optional, Tuple, Union

from jinja2 import BaseLoader, Environment, StrictUndefined

from thanosql._error import ThanoSQLValueError


def _stringify_list(val: object) -> str:
    # If we use str(lst) to show the string representation of a list directly,
    # there can be unwanted extra quotes. e.g. ['a', 'b'] -> ["'a'", "'b'"]
    # so we have to join list elements manually; this also covers nested lists
    if isinstance(val, list):
        return f"[{', '.join([_stringify_list(item) for item in val])}]"
    return str(val)


def _to_postgresql_value_helper(val: object) -> Union[str, list]:
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

    # For other types of objects, return their string representation
    return str(val)


def _to_postgresql_value(val: object) -> str:
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


def _split_query(query: str) -> Tuple[str, str]:
    # Make sure there is only one {{ val }} placeholder
    split_val = query.split("{{ val }}")
    if len(split_val) != 2:
        raise ThanoSQLValueError("One and only one {{ val }} placeholder is required")

    return split_val[0], split_val[1]


def _paginate(seq: Iterable, page_size: int):
    """Consume an iterable and return it in chunks.

    Every chunk is at most `page_size`. Never return an empty chunk.
    From https://github.com/psycopg/psycopg2/blob/master/lib/extras.py#L1175
    """
    page = []
    it = iter(seq)
    while True:
        try:
            for _ in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


def _render(query: str, params: dict) -> str:
    template = Environment(loader=BaseLoader, undefined=StrictUndefined).from_string(
        query
    )
    return template.render(**params)


def fill_query_placeholder(
    query: str,
    values: Union[List[tuple], List[dict]],
    template: Optional[str] = None,
    page_size: int = 100,
) -> str:
    # The query statement consists of three parts:
    # {{ 1. before val }}{{ 2. val placeholder }}{{ 3. after val }}
    # Split the query with regards to {{ val }} into pre and post parts
    pre, post = _split_query(query)

    full_query_list = []

    # Fill in the values to the query according to the defined page size
    # We will repeat the statement (with values) according to the number of pages
    # Each page contains at most page_size number of value sets
    for page in _paginate(values, page_size):
        val_query_list = []

        for args in page:
            if not template:
                if not isinstance(args, tuple):
                    raise ThanoSQLValueError(
                        "If template is not provided, values must be given as a list of tuples"
                    )

                # If there is no template, simply join values inside brackets separated by commas
                # We cannot directly use str(args) as this may result in unwanted quotations
                args_transformed = tuple(map(_to_postgresql_value, args))
                args_str = ", ".join(args_transformed)
                args_query = f"({args_str})"

            else:
                if not isinstance(args, dict):
                    raise ThanoSQLValueError(
                        "If template is provided, values must be given as a list of dictionaries"
                    )

                # If there is a template, we substitute the arguments into the template
                args_transformed = {k: _to_postgresql_value(v) for k, v in args.items()}
                args_query = _render(template, args_transformed)

            val_query_list.append(args_query)

        # Assemble: pre - values - post in query statement
        val_query = pre + ", ".join(val_query_list) + post
        full_query_list.append(val_query)

    return ";\n".join(full_query_list)
