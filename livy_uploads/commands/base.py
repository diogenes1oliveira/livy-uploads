from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Generic, Optional, TypeVar

import docstring_parser
from IPython.core.magic_arguments import argument

from livy_uploads.client.handle import SessionHandle

T = TypeVar("T")
F = TypeVar("F", bound=Callable)

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


_HANDLE_BODY_ATTR = "_livy_command_handles_body"


def handles_body(f: F) -> F:
    setattr(f, _HANDLE_BODY_ATTR, True)
    return f


class SessionCommand(ABC, Generic[T]):
    """
    Base class for commands that operate on a Livy session.
    """

    __command__: ClassVar[str]
    "Identifier name of this command/magic."

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize the command.

        This method receives the parsed options from the command line or magic arguments as keyword arguments.

        You should add the `@argument` decorators to this method.
        """
        pass

    @abstractmethod
    def run(self, handle: SessionHandle) -> T:
        """
        Command execution logic.

        You should add extra decorators like `@needs_local_scope` to this method.
        """
        raise NotImplementedError

    @classmethod
    def get_short_description(cls) -> Optional[str]:
        if not (doc := getattr(cls, "__doc__", None)):
            return None
        parsed = docstring_parser.parse(doc)
        return parsed.short_description

    @classmethod
    def as_json(cls) -> dict[str, Any]:
        return {
            "name": cls.__command__,
            "description": cls.get_short_description(),
            "impl": f"{cls.__module__}:{cls.__name__}",
        }

    @classmethod
    def handles_body(cls) -> bool:
        return True if getattr(cls.run, _HANDLE_BODY_ATTR, None) else False
