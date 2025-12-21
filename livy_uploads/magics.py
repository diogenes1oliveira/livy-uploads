# mypy: disable-error-code="misc,import-untyped"

import logging
import os
import traceback
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, List, Optional, TypeVar, cast

from hdijupyterutils.ipythondisplay import IpythonDisplay
from IPython.core.error import UsageError
from IPython.core.getipython import get_ipython
from IPython.core.magic import Magics, cell_magic, line_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from sparkmagic.livyclientlib.exceptions import (
    BadUserDataException,
    LivyClientLibException,
    SparkStatementException,
    handle_expected_exceptions,
    wrap_unexpected_exceptions,
)
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.utils.sparklogger import SparkLog

from livy_uploads.logs import configure_logger
from livy_uploads.commands import LivyRunCode, LivyRunShell, LivyUploadDir, LivyUploadFile
from livy_uploads.paths import find_first_in_paths, resolve_pathspec, load_envfile, NBLIB_PATH_ENVVAR
from livy_uploads.session import LivyCommand, LivySession

F = TypeVar("F", bound=Callable)
LOGGER = logging.getLogger(__name__)


def wrap_standard_exceptions(f: F) -> F:
    @wraps(f)
    def inner(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except LivyClientLibException as e:
            raise
        except Exception as e:
            traceback.print_exc()
            raise SparkStatementException("error") from e

    return cast(F, inner)


@magics_class
class LivyUploaderMagics(Magics):
    def __init__(self, shell: Any) -> None:
        super().__init__(shell)
        self.ipython_display = IpythonDisplay()
        self.logger = SparkLog("LivyUploaderMagics")

    @magic_arguments()
    @argument(
        "-n",
        "--varname",
        type=str,
        default=None,
        help="Name of the variable to send.",
    )
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @wrap_standard_exceptions
    def send_obj_to_spark(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format("send_obj_to_spark", cell.strip())
            )
        args = parse_argstring(LivyUploaderMagics.send_obj_to_spark, line)
        if not args.varname:
            raise BadUserDataException("Variable name must be provided with -n/--varname option")
        session = get_session(getattr(args, "session_name", None))
        cmd = LivyRunCode(
            vars=dict(
                varname=args.varname,
                value=(local_ns or {})[args.varname],
            ),
            code="""
                globals()[varname] = value
            """,
        )
        session.apply(cmd)

    @magic_arguments()
    @argument(
        "-n",
        "--varname",
        type=str,
        default=None,
        help="Name of the variable to fetch.",
    )
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def get_obj_from_spark(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format("send_obj_to_spark", cell.strip())
            )
        args = parse_argstring(LivyUploaderMagics.get_obj_from_spark, line)
        if not args.varname:
            raise BadUserDataException("Variable name must be provided with -n/--varname option")
        session = get_session(getattr(args, "session_name", None))
        cmd = LivyRunCode(
            vars=dict(varname=args.varname),
            code="""
                _ = globals()[varname]
            """,
        )
        _, result = session.apply(cmd)
        local_ns[args.varname] = result  # type: ignore

    @magic_arguments()
    @argument(
        "-p",
        "--path",
        type=str,
        default=None,
        help="Local path to upload.",
    )
    @argument(
        "-d",
        "--dest",
        type=str,
        default=None,
        help="Remote path to upload into.",
    )
    @argument(
        "-c",
        "--chunk-size",
        type=int,
        default=50_000,
        help="Max size in each upload chunk",
    )
    @argument(
        "-m",
        "--mode",
        type=int,
        default=0,
        help="Permissions to set on the uploaded file or directory. Defaults to 0o700 for directories and 0o600 for files.",
    )
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @wrap_standard_exceptions
    def send_path_to_spark(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format("send_obj_to_spark", cell.strip())
            )
        args = parse_argstring(LivyUploaderMagics.send_path_to_spark, line)
        if not args.path:
            raise BadUserDataException("Source must be provided with -s/--source option")
        source = Path(args.path)
        if not source.exists():
            raise BadUserDataException(f"Source path {source} does not exist")

        session = get_session(getattr(args, "session_name", None))
        if source.is_dir():
            cmd: LivyCommand = LivyUploadDir(
                source_path=args.path,
                dest_path=args.dest,
                chunk_size=args.chunk_size,
                mode=args.mode or 0o700,
            )
        else:
            cmd = LivyUploadFile(
                source_path=args.path,
                dest_path=args.dest,
                chunk_size=args.chunk_size,
                mode=args.mode or 0o600,
            )

        final_path = session.apply(cmd)
        self.ipython_display.write(f"Uploaded {source} to {final_path}")

    @magic_arguments()
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    )
    @argument(
        "-t",
        "--timeout",
        type=float,
        default=10.0,
        help="Max execution time of the command",
    )
    @needs_local_scope
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def shell_command(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        "Executes an adhoc shell command in the remote Spark master"
        if local_ns is None:
            raise UsageError("local_ns is required")

        args = parse_argstring(LivyUploaderMagics.shell_command, line)
        command = cell.strip()

        if not command:
            raise BadUserDataException("Non-empty command must be provided in the cell")

        session = get_session(getattr(args, "session_name", None))
        cmd = LivyRunShell(
            command=command,
            run_timeout=args.timeout,
        )
        pid, output, returncode = session.apply(cmd)
        for l in output.splitlines():
            print(l)
        self.ipython_display.write(f"$ command exited with code {returncode} (pid={pid})")
        local_ns = local_ns or {}
        local_ns["shell_output"] = output  # type: ignore
        local_ns["shell_returncode"] = returncode  # type: ignore

    _logs_follower: Iterator[List[str]] = None  # type: ignore

    @magic_arguments()
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy client to use. If not provided, uses the default one",
    )
    @argument(
        "-p",
        "--page-size",
        type=int,
        default=100,
        help="Max lines to fetch at once",
    )
    @argument(
        "-r",
        "--reset",
        action="store_true",
        default=False,
        help="Reset the logs follower",
    )
    @needs_local_scope
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def logs_follow(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        "Follow the logs from the Livy API"
        args = parse_argstring(LivyUploaderMagics.logs_follow, line)
        command = cell.strip()

        if command:
            raise BadUserDataException("No command must be provided in the cell")

        if self.__class__._logs_follower is None or args.reset:
            session = get_session(getattr(args, "session_name", None))
            self.__class__._logs_follower = session.follow(
                page_size=args.page_size,
            )

        try:
            lines = next(self.__class__._logs_follower)
        except StopIteration:
            lines = []

        local_ns["logs_lines"] = lines  # type: ignore

        if not lines:
            self.ipython_display.write(f"No new logs")
            return

        for line in lines:
            print(line)

    @needs_local_scope
    @line_magic
    def nblib(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        """
        Finds and loads an .ipynb library file into the current notebook.

        The library pathspec search paths can be set via either:
            - the local __nblib_path__ variable in the entrypoint notebook;
            - the environment variable $NBLIB_PATH.

        If the path is relative, we will also try to load it from the current directory.
        """
        if local_ns is None:
            raise UsageError("local_ns is required")

        ipython = get_ipython()
        if ipython is None:
            raise UsageError("no IPython shell found")

        if cell and cell.strip():
            raise UsageError("%nblib magic must be used without a cell body")

        if not (line := line.strip()):
            raise UsageError("%nblib magic must be used with the path argument")

        path = PurePosixPath(line)

        try:
            nblib_pathspec = local_ns["__nblib_path__"]
        except KeyError:
            # save the resolved path in the namespace and in os.environ,
            # so it's not resolved in downstream notebooks
            local_ns["__nblib_path__"] = resolve_pathspec(save=True)
        else:
            # save only if not already set in the environment, making sure it's not
            # overriden in downstream notebooks
            if os.getenv(NBLIB_PATH_ENVVAR) is None:
                local_ns["__nblib_path__"] = resolve_pathspec(nblib_pathspec, save=True)

        resolved_path = find_first_in_paths([path])

        LOGGER.warning("resolved %%run %s to %s", path, resolved_path)
        ipython.run_line_magic("run", str(resolved_path))

    @needs_local_scope
    @line_magic
    def dotenv(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        """
        Finds and loads a dotenv file into the current environment.

        The env filename can be overriden via `__dotenv_names__` local variable in the notebook, defaulting to
        `("env", ".env")`.
        """
        if local_ns is None:
            raise UsageError("local_ns is required")

        if cell and cell.strip():
            raise UsageError("%%dotenv magic must be used without a cell body")

        if line := line.strip():
            filenames = line.split()
            required = True
        else:
            filenames = [".env", "env"]
            required = False

        # # Auto-configure logging if root logger hasn't been set up yet
        # root_logger = logging.getLogger()
        # if not root_logger.handlers or root_logger.level == logging.WARNING:
        #     configure_logger()

        load_envfile(*filenames, required=required)

    @needs_local_scope
    @line_magic
    def configure_logging(self, line: str, cell: str = "", local_ns: Optional[Any] = None) -> None:
        """
        Configures logging according to the $LOG_LEVEL environment variable.
        """
        if local_ns is None:
            raise UsageError("local_ns is required")

        if not (ipython := get_ipython()):
            raise UsageError("no IPython shell found")

        if cell and cell.strip():
            raise UsageError("%%configure_logging magic must be used without a cell body")

        if line := line.strip():
            raise UsageError("%%configure_logging magic must be used without arguments")

        configure_logger()


def load_ipython_extension(ipython: Any) -> None:
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(LivyUploaderMagics)


def get_session(session_name: Optional[str] = None) -> "LivySession":
    """
    Creates a session endpoint instance from the current IPython shell
    """
    cell_magics = get_ipython().magics_manager.magics["cell"]  # type: ignore

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
        spark_controller: SparkController = spark_magic.__self__.spark_controller
    except (AttributeError, TypeError):
        raise RuntimeError(f"no spark controller found in magic %{magic_name!r}")

    livy_session = spark_controller.get_session_by_name_or_default(session_name)
    livy_client = livy_session.http_client._http_client

    return LivySession(
        url=livy_client._endpoint.url,
        session_id=livy_session.id,
        default_headers=livy_client._headers,
        verify=livy_client.verify_ssl,
        requests_session=livy_client._session,
    )
