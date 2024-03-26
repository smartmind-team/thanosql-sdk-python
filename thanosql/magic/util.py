import pandas as pd
import re
from IPython.display import Audio, Image, Video, display
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ResourceClosedError

from .exception import (
    ThanoSQLConnectionError,
    ThanoSQLInternalError,
    ThanoSQLSyntaxError,
)


def format_result(output_dict: dict):

    data = output_dict["data"]
    workspace_db_info = data.get("workspace_db_info")
    response_type = data.get("response_type")

    query_string = data.get("query_string")
    extra_query_string = data.get("extra_query_string")

    user = workspace_db_info.get("user")
    password = workspace_db_info.get("password")
    database = workspace_db_info.get("database")
    host = workspace_db_info.get("host")

    connection_string = f"postgresql://{user}:{password}@/{database}?host={host}"

    try:
        engine = create_engine(connection_string)
    except:
        raise ThanoSQLConnectionError("Error connecting to workspace database")

    with engine.connect() as conn:
        result = None

        if response_type == "NORMAL":
            if get_query_type(query_string=query_string) == "SELECT":
                result = stream_sql_results(conn=conn, query_string=query_string)
            else:
                try:
                    result = pd.read_sql_query(text(query_string), conn)
                except ResourceClosedError:
                    """
                    ResourceClosedError will capture queries
                    like INSERT and DROP that don’t return a value.
                    This is not the best solution as we are presumptuously assuming
                    that the connection with the database will always be secure and succeed.
                    If a failure happens in the database,
                    ResourceClosedError will be raised
                    and “Success” will be printed out, which is a problem.
                    Therefore, this is subject to change in the future.
                    """
                    print("Success")
                


        elif response_type == "SELECT":
            result = stream_sql_results(conn=conn, query_string=query_string)

        elif response_type == "SELECT_DROP":
            result = stream_sql_results(conn=conn, query_string=query_string)
            conn.execution_options(stream_results=False)
            conn.execute(text(extra_query_string))

        elif response_type is None:
            print("Success")

    # close sqlalchemy(DB) engine
    engine.dispose()

    print_type = data.get("print")
    if print_type:
        print_option = data.get("print_option", {})
        return print_result(result, print_type, print_option)

    return result


def print_result(query_df, print_type: str, print_option):
    if print_type == "print_image":
        return print_image(query_df, print_option)
    elif print_type == "print_audio":
        return print_audio(query_df, print_option)
    elif print_type == "print_video":
        return print_video(query_df, print_option)
    else:
        raise ThanoSQLInternalError("Error: Wrong print_type.")


def print_image(df, print_option):
    column_name = print_option.get("image_col", "image_path")
    image_file_list = list(df[column_name])

    base_dir = print_option.get("base_dir", "")
    limit = print_option.get("limit")
    for image_path in image_file_list[:limit]:
        image_full_path = f"{base_dir}/{image_path}"
        print(image_full_path)
        display(Image(image_full_path, width=240, height=240))
    return


def print_audio(df, print_option):
    column_name = print_option.get("audio_col", "audio_path")
    audio_file_list = list(df[column_name])

    base_dir = print_option.get("base_dir", "")
    limit = print_option.get("limit")
    for audio_path in audio_file_list[:limit]:
        audio_full_path = f"{base_dir}/{audio_path}"
        print(audio_full_path)
        display(Audio(audio_full_path))
    return


def print_video(df, print_option):
    column_name = print_option.get("video_col", "video_path")
    video_file_list = list(df[column_name])

    base_dir = print_option.get("base_dir", "")
    limit = print_option.get("limit")
    for video_path in video_file_list[:limit]:
        video_full_path = f"{base_dir}/{video_path}"
        print(video_full_path)
        display(Video(video_full_path, embed=True))
    return

def stream_sql_results(conn: Connection, query_string: str) -> pd.DataFrame:
    dfs = []
    for chunk_df in pd.read_sql_query(
        text(query_string), 
        conn.execution_options(stream_results=True),
        chunksize=10000):
        dfs.append(chunk_df)
    result = pd.concat(dfs)
    return result

def get_query_type(query_string: str) -> str:
    import pglast

    try:
        query_type = "_".join(
                    map(
                        str,
                        re.findall(
                            "[A-Z][^A-Z]*",
                            pglast.parser.parse_sql(query_string)[
                                0
                            ].stmt.__class__.__name__.replace("Stmt", ""),
                        ),
                    )
                ).upper()
        
    except pglast.parser.ParseError as e:
        raise ThanoSQLSyntaxError(str(e))

    return query_type