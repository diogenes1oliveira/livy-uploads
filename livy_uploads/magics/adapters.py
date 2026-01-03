import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from livy_uploads.client.handle import SessionHandle
from livy_uploads.client.manager import SessionManager
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.models.http import HttpConfig
from livy_uploads.models.session import LivyClientConfig, SessionInfo
from livy_uploads.project import Project
from livy_uploads.utils.retry_policy import CustomIntervalRetry, NoRetry, RetryPolicy

# mypy: disable-error-code="import-untyped"

if TYPE_CHECKING:
    from sparkmagic.livyclientlib.sparkcontroller import SparkController
else:
    SparkController = Any

LOGGER = logging.getLogger(__name__)


def get_spark_controller() -> SparkController:
    from IPython.core.getipython import get_ipython
    from sparkmagic.livyclientlib.sparkcontroller import SparkController

    cell_magics = get_ipython().magics_manager.magics["cell"]

    try:
        magic_name = "send_to_spark"
        spark_magic = cell_magics["send_to_spark"]
    except KeyError:
        try:
            magic_name = "spark"
            spark_magic = cell_magics["spark"]
        except KeyError:
            raise RuntimeError("no spark magic found named '%send_to_spark' or '%spark'")

    try:
        obj = spark_magic.__self__.spark_controller
    except (AttributeError, TypeError):
        raise RuntimeError(f"no KernelMagics object found in magic %{magic_name!r}")

    if type(obj).__name__ != "SparkController":
        # auto reload might break this
        raise RuntimeError(
            f"bad value type for spark_controller: expected {SparkController!r}, got {type(obj)!r} instead"
        )

    return obj


def get_configured_session_info(name: Optional[str] = None) -> SessionInfo:
    spark_controller = get_spark_controller()
    livy_session = spark_controller.get_session_by_name_or_default(name)
    return SessionInfo.parse(
        {
            "id": livy_session.id,
            "appId": livy_session.get_app_id(),
        }
    )


def get_livy_endpoint(name: Optional[str] = None, project: Optional[Project] = None) -> LivyEndpoint:

    import requests
    import sparkmagic.utils.configuration as conf
    from sparkmagic.livyclientlib.endpoint import Endpoint
    from sparkmagic.livyclientlib.exceptions import SessionManagementException
    from sparkmagic.livyclientlib.livyreliablehttpclient import LivyReliableHttpClient

    project = project or Project.get()
    http_config = project.config.get(".http_config", HttpConfig, default=HttpConfig())

    url: str
    retry_policy = CustomIntervalRetry(
        retry_seconds_to_sleep_list=conf.retry_seconds_to_sleep_list(),
        configurable_retry_policy_max_retries=conf.configurable_retry_policy_max_retries(),
    )
    requests_session: Optional[requests.Session] = None
    default_headers = dict(conf.custom_headers())

    if conf.ignore_ssl_errors():
        verify: Optional[Union[bool, Path]] = False
    elif isinstance(http_config.verify, Path):
        verify = http_config.verify
    else:
        verify = True

    spark_controller = get_spark_controller()
    try:
        livy_session = spark_controller.get_session_by_name_or_default(name)
    except SessionManagementException:
        LOGGER.debug("failed to get configured session, trying to get through the configuration", exc_info=True)

        credentials = conf.base64_kernel_python_credentials()
        endpoint = Endpoint(url=credentials["url"], auth=credentials["auth"])
        livy_reliable_http_client = LivyReliableHttpClient.from_endpoint(endpoint)

        url = credentials["url"]
        requests_session = livy_reliable_http_client._http_client._session
    else:
        LOGGER.debug("using configured session")
        livy_client = livy_session.http_client._http_client

        url = livy_client._endpoint.url
        requests_session = livy_client._session

    return LivyEndpoint(
        url=url,
        retry_policy=retry_policy,
        default_headers=default_headers,
        requests_session=requests_session,
        verify=verify,
    )


def get_session_manager(name: Optional[str] = None, project: Optional[Project] = None) -> SessionManager:
    project = project or Project.get()
    endpoint = get_livy_endpoint(name=name, project=project)

    return SessionManager(
        endpoint=endpoint,
        livy_client_config=project.config.get(".livy_client", LivyClientConfig, nullable=True),
        delete_policy=project.config.get(".delete_policy", RetryPolicy, nullable=True),
    )


def get_configured_session_handle(
    name: Optional[str] = None,
    project: Optional[Project] = None,
) -> SessionHandle:
    return SessionHandle(
        info=get_configured_session_info(name),
        manager=get_session_manager(name=name, project=project),
    )
