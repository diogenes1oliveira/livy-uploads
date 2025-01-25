from logging import getLogger
from typing import Dict, Optional, TypeVar

import requests
import requests.exceptions

from livy_uploads.retry_policy import RetryPolicy, DontRetryPolicy, WithExceptionsPolicy
from livy_uploads.exceptions import LivyRequestError, LivyRetriableError
from livy_uploads.utils import try_decode


LOGGER = getLogger(__name__)
T = TypeVar('T')


class LivyEndpoint:
    '''
    A class to upload generic data to a remote Spark session using the Livy API.
    '''
    def __init__(
        self,
        url: str,
        default_headers: Optional[Dict[str, str]] = None,
        verify: bool = True,
        auth=None,
        requests_session: Optional[requests.Session] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ):
        '''
        Parameters:
        - url: the base URL of the Livy server
        - default_headers: a dictionary of headers to include in every request
        - verify: whether to verify the SSL certificate of the server
        - auth: an optional authentication object to pass to requests
        - requests_session: an optional requests.Session object to use for making requests
        - retry_policy: an optional retry policy to use for requests
        '''
        self.url = url.rstrip('/')
    
        if default_headers is None:
            default_headers = {'content-type': 'application/json'}
        self.default_headers = {k.lower(): v for k, v in default_headers.items()}

        self.verify = verify
        self.auth = auth
        self.requests_session = requests_session or requests.Session()
        self.retry_policy = retry_policy or DontRetryPolicy()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.url!r})'

    def build_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        '''
        Merges the list of default headers with the provided headers, and normalizes the keys to lowercase
        '''
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        return {**self.default_headers, **headers}

    def request(
        self,
        method: str,
        path: str,
        headers=None,
        retry_policy: Optional[RetryPolicy] = None,
        **kwargs,
    ) -> requests.Response:
        '''
        Sends a request to the Livy endpoint.

        Parameters:
        - method: the HTTP method to use
        - path: the path to append to the base URL
        - headers: a dictionary of headers to include in the request. If None, the default headers will be used
        - retry_policy: an optional retry policy to use for this request, defaults to the one configured in the endpoint
        - kwargs: extra arguments to pass to `requests.Session.request`
        '''
        retry_policy = WithExceptionsPolicy(retry_policy or self.retry_policy, LivyRetriableError)
        if headers is None:
            headers = self.default_headers

        return self.retry_policy.run(
            func=_request_do,
            session=self.requests_session,
            method=method,
            url=self.url + path,
            headers=headers,
            auth=self.auth,
            verify=self.verify,
            **kwargs,
        )


def _request_do(
    session: requests.Session,
    *args,
    **kwargs,
) -> requests.Response:
    try:
        response = session.request(*args, **kwargs)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        raise LivyRetriableError from e

    if response.status_code < 300:
        return response

    if response.status_code == 429 or response.status_code >= 500:
        raise LivyRetriableError

    raise LivyRequestError(response, body=try_decode(response))
