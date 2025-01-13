from functools import wraps
from pathlib import Path
import traceback
from typing import Iterator, List, Optional

from IPython.core.getipython import get_ipython
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from IPython.core.magic import magics_class, Magics
from IPython.core.magic import needs_local_scope, line_magic, cell_magic
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.core.magic_arguments import parse_argstring
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.livyclientlib.exceptions import (
    handle_expected_exceptions,
    wrap_unexpected_exceptions,
    BadUserDataException,
    SparkStatementException,
    LivyClientLibException,
)
from hdijupyterutils.ipythondisplay import IpythonDisplay

from livy_uploads.session import LivySessionEndpoint
from livy_uploads.commands import (
    LivyRunCode,
    LivyRunShell,
    LivyUploadDir,
    LivyUploadFile,
)


def wrap_standard_exceptions(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except LivyClientLibException as e:
            raise
        except Exception as e:
            traceback.print_exc()
            raise SparkStatementException('error') from e
    return inner


@magics_class
class LivyUploaderMagics(Magics):
    def __init__(self, shell):
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
        help="Name of the Livy session to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @wrap_standard_exceptions
    def send_obj_to_spark(self, line, cell="", local_ns=None):
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format(
                    'send_obj_to_spark', cell.strip()
                )
            )
        args = parse_argstring(LivyUploaderMagics.send_obj_to_spark, line)
        if not args.varname:
            raise BadUserDataException(
                "Variable name must be provided with -n/--varname option"
            )
        session = get_session(getattr(args, 'session_name', None))
        cmd = LivyRunCode(
            vars=dict(
                varname=args.varname,
                value=(local_ns or {})[args.varname],
            ),
            code='''
                globals()[varname] = value
            '''
        )
        session.run(cmd)

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
        help="Name of the Livy session to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def get_obj_from_spark(self, line, cell="", local_ns=None):
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format(
                    'send_obj_to_spark', cell.strip()
                )
            )
        args = parse_argstring(LivyUploaderMagics.get_obj_from_spark, line)
        if not args.varname:
            raise BadUserDataException(
                "Variable name must be provided with -n/--varname option"
            )
        session = get_session(getattr(args, 'session_name', None))
        cmd = LivyRunCode(
            vars=dict(varname=args.varname),
            code='''
                return globals()[varname]
            '''
        )
        _, result = session.run(cmd)
        local_ns[args.varname] = result

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
        help="Name of the Livy session to use. If not provided, uses the default one",
    )
    @line_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @wrap_standard_exceptions
    def send_path_to_spark(self, line, cell="", local_ns=None):
        if cell.strip():
            raise BadUserDataException(
                "Cell body for %%{} magic must be empty; got '{}' instead".format(
                    'send_obj_to_spark', cell.strip()
                )
            )
        args = parse_argstring(LivyUploaderMagics.send_path_to_spark, line)
        if not args.path:
            raise BadUserDataException(
                "Source must be provided with -s/--source option"
            )
        source = Path(args.path)
        if not source.exists():
            raise BadUserDataException(
                f"Source path {source} does not exist"
            )

        session = get_session(getattr(args, 'session_name', None))
        if source.is_dir():
            cmd = LivyUploadDir(
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

        _, final_path = session.run(cmd)
        self.ipython_display.write(f"Uploaded {source} to {final_path}")

    @magic_arguments()
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy session to use. If not provided, uses the default one",
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
    def shell_command(self, line, cell="", local_ns=None):
        args = parse_argstring(LivyUploaderMagics.shell_command, line)
        command = cell.strip()

        if not command:
            raise BadUserDataException(
                "Non-empty command must be provided in the cell"
            )

        session = get_session(getattr(args, 'session_name', None))
        cmd = LivyRunShell(
            command=command,
            run_timeout=args.timeout,
        )
        output, returncode = session.run(cmd)
        for l in output.splitlines():
            print(l)
        self.ipython_display.write(f"$ command exited with code {returncode}")
        local_ns = local_ns or {}
        local_ns['shell_output'] = output
        local_ns['shell_returncode'] = returncode

    _logs_follower: Iterator[List[str]] = None

    @magic_arguments()
    @argument(
        "-s",
        "--session-name",
        type=str,
        default=None,
        help="Name of the Livy session to use. If not provided, uses the default one",
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
    def logs_follow(self, line, cell="", local_ns=None):
        args = parse_argstring(LivyUploaderMagics.logs_follow, line)
        command = cell.strip()

        if command:
            raise BadUserDataException(
                "No command must be provided in the cell"
            )

        if self.__class__._logs_follower is None or args.reset:
            session = get_session(getattr(args, 'session_name', None))
            self.__class__._logs_follower = session.follow(
                page_size=args.page_size,
            )

        try:
            lines = next(self.__class__._logs_follower)
        except StopIteration:
            lines = []

        local_ns['logs_lines'] = lines

        if not lines:
            self.ipython_display.write(f"No new logs")
            return

        for line in lines:
            print(line)


def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(LivyUploaderMagics)


def get_session(session_name: Optional[str] = None) -> 'LivySessionEndpoint':
    '''
    Creates a session endpoint instance from the current IPython shell
    '''
    cell_magics = get_ipython().magics_manager.magics['cell']

    try:
        magic_name = 'send_to_spark'
        spark_magic = cell_magics['send_to_spark']
    except KeyError:
        try:
            magic_name = 'spark'
            spark_magic = cell_magics['spark']
        except KeyError:
            raise RuntimeError("no spark magic found named '%send_to_spark' or '%spark'")

    try:
        spark_controller: SparkController = spark_magic.__self__.spark_controller
    except (AttributeError, TypeError):
        raise RuntimeError(f'no spark controller found in magic %{magic_name!r}')

    livy_session = spark_controller.get_session_by_name_or_default(session_name)
    livy_client = livy_session.http_client._http_client

    return LivySessionEndpoint(
        url=livy_client._endpoint.url,
        session_id=livy_session.id,
        default_headers=livy_client._headers,
        verify=livy_client.verify_ssl,
        auth=livy_client._auth,
        requests_session=livy_client._session,
    )
