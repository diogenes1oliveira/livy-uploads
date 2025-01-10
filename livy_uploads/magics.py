from functools import wraps
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
)
from hdijupyterutils.ipythondisplay import IpythonDisplay

from livy_uploads.uploader import LivyUploader
from livy_uploads.commander import LivyCommander


def wrap_standard_exceptions(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            raise SparkStatementException('bad input') from e
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
        uploader = LivyUploader.from_ipython(getattr(args, 'session_name', None))
        obj = (local_ns or {})[args.varname]
        uploader.send_pickled(obj, args.varname)

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
        uploader = LivyUploader.from_ipython(getattr(args, 'session_name', None))
        obj = uploader.get_pickled(args.varname)
        (local_ns or {})[args.varname] = obj

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
        default=-1,
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
        uploader = LivyUploader.from_ipython(getattr(args, 'session_name', None))
        uploader.upload_path(
            source_path=args.path,
            dest_path=args.dest,
            chunk_size=args.chunk_size,
            mode=args.mode,
        )

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
        "--pause",
        type=float,
        default=2.0,
        help="Time between poll checks",
    )
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def remote_command(self, line, cell="", local_ns=None):
        args = parse_argstring(LivyUploaderMagics.remote_command, line)
        session_name = getattr(args, 'session_name', None)
        cmd = cell.strip()

        if not cmd:
            raise BadUserDataException(
                "Non-empty command must be provided in the cell"
            )

        commander = LivyCommander.from_ipython(session_name)
        commander.run_command_fg(['bash', '-c', cmd], pause=args.pause)

def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(LivyUploaderMagics)
