"""
Constants for the plugins module.

Don't use `from ... import *` format here, otherwise the constants can't be reloaded.
"""

import os

APPNAME_ENV = "APPNAME"
"Environment variable name to override the app name"

APPNAME: str
"The app name to use as prefix. Defaults to `sparkrl`"

PLUGINS_ENV: str
"The environment variable name to store the plugin URIs. Defaults to `SPARKRL_PLUGINS`"

GROUP_PREFIX: str
"The prefix to use for the plugin group names. Defaults to `sparkrl.plugins.`"

LOADERS_GROUP: str
"The entrypoint group name for the loaders. Defaults to `sparkrl.plugins.loaders`"


def as_json() -> dict[str, str]:
    """
    Returns the constants as a JSON-serializable dictionary.
    """
    return {
        "APPNAME": APPNAME,
        "PLUGINS_ENV": PLUGINS_ENV,
        "GROUP_PREFIX": GROUP_PREFIX,
        "LOADERS_GROUP": LOADERS_GROUP,
    }


def reload() -> None:
    """
    Reload the constants.
    """
    global APPNAME, GROUP_PREFIX, PLUGINS_ENV, LOADERS_GROUP

    # TODO: once I rename the whole project
    # APPNAME = os.environ.get(APPNAME_ENV) or __name__.partition(".")[0]
    APPNAME = os.environ.get(APPNAME_ENV) or "sparkrl"
    GROUP_PREFIX = f"{APPNAME}.plugins."
    PLUGINS_ENV = f"{APPNAME.upper()}_PLUGINS"
    LOADERS_GROUP = GROUP_PREFIX + "loaders"


reload()
