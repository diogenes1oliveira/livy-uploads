import argparse
from typing import Optional

from livy_uploads.auth import Authenticator
from livy_uploads.client.manager import SessionManager
from livy_uploads.commands.base import SessionCommand
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.models.http import HttpConfig
from livy_uploads.models.session import LivyClientConfig, SessionInfo
from livy_uploads.project import Project
from livy_uploads.utils.retry_policy import NoRetry, RetryPolicy


def get_livy_endpoint(project: Optional[Project] = None) -> LivyEndpoint:
    project = project or Project.get()

    livy_client_config = project.config.get(".livy_client", LivyClientConfig, default=LivyClientConfig())
    http_config = project.config.get(".http_config", HttpConfig, default=HttpConfig())

    url = livy_client_config.url
    retry_policy = project.config.get(".retry_policy", RetryPolicy, default=NoRetry())
    authenticator = project.config.get(".livy_client.auth", Authenticator, nullable=True)
    default_headers = livy_client_config.default_headers
    verify = http_config.verify
    proxy = http_config.proxy

    return LivyEndpoint(
        url=url,
        default_headers=default_headers,
        verify=verify,
        authenticator=authenticator,
        retry_policy=retry_policy,
        proxy=proxy,
    )


def get_session_manager(project: Optional[Project] = None) -> SessionManager:
    project = project or Project.get()
    endpoint = get_livy_endpoint(project=project)

    livy_client_config = project.config.get(".livy_client", LivyClientConfig, default=LivyClientConfig())
    create_config = project.config.get(".session_configs", SessionInfo)
    readiness_policy = project.config.get(".readiness_policy", RetryPolicy, nullable=True)
    delete_policy = project.config.get(".delete_policy", RetryPolicy, nullable=True)

    return SessionManager(
        endpoint=endpoint,
        livy_client_config=livy_client_config,
        create_config=create_config,
        readiness_policy=readiness_policy,
        delete_policy=delete_policy,
    )


def build_parser(command: type[SessionCommand]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=command.__command__,
        description=command.get_short_description(),
    )

    # Get decorators from __init__ method and apply them to the parser
    # Following the pattern from IPython.core.magic_arguments.construct_parser
    init_method = command.__init__
    if hasattr(init_method, "decorators"):
        # Reverse the list of decorators to apply them in the order they appear in source
        group = None
        for deco in init_method.decorators[::-1]:
            result = deco.add_to_parser(parser, group)
            if result is not None:
                group = result

    return parser
