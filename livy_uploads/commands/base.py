from abc import ABC, abstractmethod
from typing import ClassVar, Generic, Iterable, TypeVar

from IPython.core.magic_arguments import argument, magic_arguments

from livy_uploads.client.handles import SessionHandle

T = TypeVar("T")


COMMON_ARGS = (
    argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    ),
)
"Common basic arguments for all commands."


class SessionCommand(ABC, Generic[T]):
    """
    Base class for commands that operate on a Livy session.

    Implementations must define the `__command__` class variable to the name of the command/magic.
    """

    __command__: ClassVar[str]

    @abstractmethod
    def run(self, handle: SessionHandle) -> T:
        """
        Command execution logic.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def args(cls) -> Iterable[argument]:
        """
        Returns the arguments for the command.
        """
        raise NotImplementedError
