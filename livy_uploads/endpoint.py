import threading
from collections.abc import Mapping
from logging import getLogger
from typing import Any, Optional, TypeVar

import requests
import requests.exceptions

from livy_uploads.auth import Authenticator
from livy_uploads.exceptions import LivyRequestError, LivyRetriableError
from livy_uploads.utils.retry_policy import MaxTries, NoRetry, RetryPolicy
from livy_uploads.utils.typeutils import assert_type, try_decode

LOGGER = getLogger(__name__)
T = TypeVar("T")


class LivyEndpoint:
    """
    A class to upload generic data to a remote Spark session using the Livy API.
    """

    def __init__(
        self,
        url: str,
        default_headers: Optional[dict[str, str]] = None,
        verify: Optional[bool] = True,
        authenticator: Optional[Authenticator] = None,
        requests_session: Optional[requests.Session] = None,
        retry_policy: Optional[RetryPolicy] = None,
        proxy: Optional[str] = None,
    ):
        """
        Parameters:
        - url: the base URL of the Livy server
        - default_headers: a dictionary of headers to include in every request
        - verify: whether to verify the SSL certificate of the server
        - authenticator: an optional authentication object factory to pass to requests
        - requests_session: an optional requests.Session object to use for making requests
        - retry_policy: an optional retry policy to use for requests
        """
        self.url = url.rstrip("/")

        if default_headers is None:
            default_headers = {"content-type": "application/json"}
        self.default_headers = {k.lower(): v for k, v in default_headers.items()}

        self.verify = True if verify is None else (verify or False)
        self._auth = None
        self.authenticator = authenticator
        self._auth_lock = threading.RLock()
        self.requests_session = requests_session or requests.Session()
        self.retry_policy = retry_policy or NoRetry()

        self.requests_session.trust_env = False
        self.proxy = proxy or None
        if self.proxy:
            self.requests_session.proxies.update(
                {
                    "http": self.proxy,
                    "https": self.proxy,
                }
            )
        else:
            self.requests_session.proxies = {}

    @property
    def auth(self) -> Any:
        if self.authenticator is None:
            return None

        if self._auth:
            return self._auth  # type: ignore

        with self._auth_lock:
            if self._auth is None:
                self._auth = self.authenticator()  # type: ignore
            return self._auth

    @classmethod
    def from_config(cls, config: Optional[Mapping[str, Any]]) -> "LivyEndpoint":
        if not config:
            raise ValueError("config is required")

        session = requests.Session()
        session.trust_env = False
        proxy = assert_type(config.get("proxy"), Optional[str])  # type: ignore
        if proxy:
            session.proxies.update(
                {
                    "http": proxy,
                    "https": proxy,
                }
            )

        retry_config = config.get("retry_policy")
        if retry_config:
            retry_policy: RetryPolicy = MaxTries(
                count=assert_type(retry_config["max_tries"], int),
                pause=assert_type(retry_config["pause"], float),
            )
        else:
            retry_policy = NoRetry()

        return cls(
            url=assert_type(config["url"], str),
            default_headers=assert_type(config.get("default_headers"), Optional[dict]),  # type: ignore
            verify=assert_type(config.get("verify"), Optional[bool]),  # type: ignore
            authenticator=Authenticator.from_config(config.get("auth")),
            retry_policy=retry_policy,
            proxy=proxy,
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.url!r})"

    def build_headers(self, headers: Optional[dict[str, str]] = None) -> dict[str, str]:
        """
        Merges the list of default headers with the provided headers, and normalizes the keys to lowercase
        """
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        return {**self.default_headers, **headers}

    def request(
        self,
        method: str,
        path: str,
        headers: Any = None,
        retry_policy: Optional[RetryPolicy] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Sends a request to the Livy endpoint.

        Parameters:
        - method: the HTTP method to use
        - path: the path to append to the base URL
        - headers: a dictionary of headers to include in the request. If None, the default headers will be used
        - retry_policy: an optional retry policy to use for this request, defaults to the one configured in the endpoint
        - kwargs: extra arguments to pass to `requests.Session.request`
        """
        policy = retry_policy or self.retry_policy
        if headers is None:
            headers = self.default_headers

        return policy.run(
            func=_request_do,
            kwargs={
                "session": self.requests_session,
                "method": method,
                "url": self.url + path,
                "headers": headers,
                "auth": self.auth,
                "verify": self.verify,
                **kwargs,
            },
            exceptions=(LivyRetriableError,),
        )


def _request_do(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs: Any,
) -> requests.Response:
    try:
        response = session.request(method, url, **kwargs)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        raise LivyRetriableError("request failed" + str(e)) from e

    if response.status_code < 300:
        return response

    if response.status_code == 429 or response.status_code >= 500:
        raise LivyRetriableError("request failed with status code %d (text: %s)", response.status_code, response.text)

    raise LivyRequestError(response, body=try_decode(response))
