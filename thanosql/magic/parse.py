import re

def is_url(s):
    p = re.compile(r"^\s*\w*://\w*")
    if p.match(s):
        return True
    else:
        return False


def is_api_token(s):
    p = re.compile(r"^\s*API_TOKEN=\w*")
    if p.match(s):
        return True
    else:
        return False


def is_multiple_queries(query_string):
    # regex below finds substring that starts with '[ or "[ and ends with ]' or ]" 
    # It removes all the substrings containing semicolon which does not need to be checked.
    processed_query_string = re.sub('''('|")\[[^']*\]('|")''', "", query_string)
    return ";" in processed_query_string
