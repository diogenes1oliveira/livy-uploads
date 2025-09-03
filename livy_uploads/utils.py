from typing import Any, Type, TypeVar
import requests


from livy_uploads.executor.cluster import assert_type

T = TypeVar('T')


def try_decode(response: requests.Response) -> Any:
    '''
    Tries to decode the response as JSON or text.
    '''
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        try:
            return response.text
        except UnicodeDecodeError:
            return response.content.decode('utf8', errors='replace')


