import json
import os

from IPython.core.magic import Magics, line_cell_magic, magics_class, needs_local_scope

from .exception import (
    ThanoSQLConnectionError,
    ThanoSQLInternalError,
    ThanoSQLSyntaxError,
)
from .parse import *

engine_cluster_ip = os.getenv("THANOSQL_ENGINE_SERVICE_HOST")
DEFAULT_API_URL = f"ws://{engine_cluster_ip}/ws/v1/query"


def request_thanosql_engine(ws, api_url, api_token, query_context):
    from .util import format_result

    try:
        ws.connect(f"{api_url}?api_token={api_token}")
    except:
        raise ThanoSQLConnectionError("Could not connect to the Websocket")

    ws.send(json.dumps(query_context))

    try:
        while True:
            output = ws.recv()
            output_dict = json.loads(output)

            if output_dict["output_type"] == "MESSAGE":
                print(output_dict["output_message"])
            elif output_dict["output_type"] == "RESULT":
                return format_result(output_dict["output_message"])
            elif output_dict["output_type"] == "ERROR":
                raise ThanoSQLInternalError(output_dict["output_message"])

    except KeyboardInterrupt:
        ws.close()
        raise ThanoSQLConnectionError("KeyboardInterrupt")


@magics_class
class ThanosMagic(Magics):
    import websocket

    ws = websocket.WebSocket()

    @needs_local_scope
    @line_cell_magic
    def thanosql(self, line: str = None, cell: str = None, local_ns={}):
        if line:
            if is_url(line):
                # Set API URL
                api_url = line.strip()
                os.environ["API_URL"] = api_url
                print(f"API URL is changed to {api_url}")
                return

            elif is_api_token(line):
                # Set API Token
                api_token = line.strip().split("API_TOKEN=")[-1]
                os.environ["API_TOKEN"] = api_token
                print(f"API Token is set as '{api_token}'")
                return

            # 'line' will be treated the same as 'cell'
            else:
                cell = line

        if not cell:
            return

        api_url = os.getenv("API_URL", DEFAULT_API_URL)
        api_token = os.getenv("API_TOKEN", None)

        if not api_token:
            raise ThanoSQLConnectionError(
                "An API Token is required. Set the API Token by running the following: %thanosql API_TOKEN=<API_TOKEN>"
            )

        query_string = cell

        if is_multiple_queries(query_string):
            raise ThanoSQLSyntaxError("Multiple Queries are not supported.")

        query_context = {"query_string": query_string}

        return request_thanosql_engine(self.ws, api_url, api_token, query_context)

    @needs_local_scope
    @line_cell_magic
    def psql(self, line: str = None, cell: str = None, local_ns={}):
        if line:
            cell = line

        if not cell:
            return

        api_url = os.getenv("API_URL", DEFAULT_API_URL)
        api_token = os.getenv("API_TOKEN", None)

        query_string = cell

        query_context = {"query_string": query_string}

        return request_thanosql_engine(self.ws, api_url, api_token, query_context)
