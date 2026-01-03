__all__ = ("SessionInfoCommand",)

import dataclasses
import logging
from typing import ClassVar

from IPython.core.magic import needs_local_scope
from IPython.core.magic_arguments import argument

from livy_uploads.client.handle import SessionHandle
from livy_uploads.commands.base import SessionCommand
from livy_uploads.models.session import SessionInfo

LOGGER = logging.getLogger(__name__)

# mypy: disable-error-code="misc"


@dataclasses.dataclass
class SessionInfoCommand(SessionCommand[SessionInfo]):
    """
    Gets extended information about the session.
    """

    __command__: ClassVar[str] = "session_info"
    refresh: bool = False

    @argument("--no-refresh", action="store_true", help="Do not refresh the session information")
    def __init__(self, no_refresh: bool = False) -> None:
        self.refresh = not no_refresh

    @needs_local_scope
    def run(self, handle: SessionHandle) -> SessionInfo:
        if self.refresh:
            session_info = handle.manager.refresh(handle.info)
        else:
            session_info = handle.info

        LOGGER.info("session info: %s", session_info.as_attrs())

        if handle.globals is not None:
            handle.globals["session_info"] = session_info

        return session_info
