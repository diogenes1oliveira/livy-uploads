import functools
import logging
from typing import Any, Callable, Optional

from IPython.core.error import UsageError
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from livy_uploads.commands.base import SessionCommand
from livy_uploads.commands.helpers import get_commands
from livy_uploads.magics.adapters import get_configured_session_handle
from livy_uploads.project import Project

MagicFunc = Callable[[str, str, Any], Any]


# Argument parsing attributes (from IPython.core.magic_arguments)
ARG_ATTRS = (
    "has_arguments",  # Set to True by @magic_arguments and @argument decorators
    "decorators",  # List of decorator instances (ArgDecorator subclasses)
    "argcmd_name",  # Optional custom magic name set by @magic_arguments(name=...)
    "argcmd_kwds",  # Keywords for parser constructor, set by @kwds decorator
    # IGNORED: "parser",  # The argument parser (MagicArgumentParser), set by @magic_arguments
)

# Meta/behavior attributes (from IPython.core.magic)
META_ATTRS = (
    *(set(functools.WRAPPER_ASSIGNMENTS) - {"__name__", "__qualname__", "__doc__"}),
    "needs_local_scope",  # Set by @needs_local_scope decorator
    "_ipython_magic_no_var_expand",  # Set by @no_var_expand decorator
    "_ipython_magic_output_can_be_silenced",  # Set by @output_can_be_silenced decorator
)
with_session_arg = argument(
    "-s",
    "--session-name",
    dest="_magics_register_session_name",
    type=str,
    default=None,
    help="Name of the Livy client to use. If not provided, uses the default one",
)
LOGGER = logging.getLogger(__name__)


def build_line_magic_func(
    cls: type[SessionCommand],
    project: Optional[Project] = None,
) -> MagicFunc:

    needs_local_scope = getattr(cls.run, "needs_local_scope", False)

    @functools.wraps(cls.run, assigned=META_ATTRS)
    @functools.wraps(cls.__init__, assigned=ARG_ATTRS)
    def magic_func(line: str, cell: str = "", local_ns: Optional[Any] = None) -> Any:
        if needs_local_scope and local_ns is None:
            raise UsageError("local_ns is required")
        if cell and cell.strip() and not cls.handles_body():
            raise UsageError("%%{} magic must be used without a cell body".format(cls.__command__))

        ns = parse_argstring(magic_func, line)
        kwargs = dict(ns._get_kwargs())
        session_name = kwargs.pop("_magics_register_session_name", None) or None
        cmd = cls(**dict(ns._get_kwargs()))

        handle = get_configured_session_handle(name=session_name, project=project)
        handle.body = cell or ""
        if local_ns is not None:
            handle.globals = local_ns

        LOGGER.info("invoking %s on %s", cmd, handle)
        return cmd.run(handle)

    magic_func.__name__ = f"magic_func_{cls.__command__}"
    magic_func.__qualname__ = magic_func.__name__
    magic_func.__doc__ = cls.__doc__

    # finalize the parser
    magic_func = magic_arguments(name=cls.__command__)(magic_func)

    return magic_func


def build_all_line_magic_funcs(project: Optional[Project] = None) -> dict[str, MagicFunc]:
    return {cls.__command__: build_line_magic_func(cls, project=project) for cls in get_commands(project=project)}
