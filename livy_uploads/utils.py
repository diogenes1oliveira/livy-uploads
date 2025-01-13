from typing import Any, TypeVar
import requests


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

