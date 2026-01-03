from typing import Optional

from IPython.core.interactiveshell import InteractiveShell

from livy_uploads.magics.builders import build_all_line_magic_funcs
from livy_uploads.project import Project


def register_magic_commands(ipython: InteractiveShell, project: Optional[Project] = None) -> None:
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    line_magic_funcs = build_all_line_magic_funcs(project=project)
    for name, func in line_magic_funcs.items():
        ipython.register_magic_function(func, magic_kind="line_cell", magic_name=name)  # type: ignore


def load_ipython_extension(ipython: InteractiveShell) -> None:
    register_magic_commands(ipython)
