from IPython.core.magic_arguments import argument, magic_arguments

from livy_uploads.client.handles import SessionHandle
from livy_uploads.commands.base import COMMON_ARGS, SessionCommand
from livy_uploads.models.session import SessionInfo


class SessionInfoCommand(SessionCommand[SessionInfo]):
    """
    Command to get the session information.
    """

    __command__ = "session_info"

    @classmethod
    def args(cls) -> tuple[argument, ...]:
        return COMMON_ARGS

    def run(self, handle: SessionHandle) -> SessionInfo:
        return handle.manager.refresh(handle.info)
