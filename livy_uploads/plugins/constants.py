"""
Constants for the plugins module.

Don't use `from ... import *` format here, otherwise the constants can't be reloaded.
"""

import os

APPNAME_ENV = "PROJECT_APPNAME"
"Environment variable name to override the app name"

PLUGINS_ENV: str
"The environment variable name to store the plugin URIs. Defaults to `SPARKRL_PLUGINS`"

PROFILES_ENV: str
"The environment variable name to store the profile URIs. Defaults to `SPARKRL_PROFILES`"

PROJECT_APPNAME: str
"The app name to use as prefix. Defaults to `sparkrl`"

GROUP_PREFIX: str
"The prefix to use for the plugin group names. Defaults to `sparkrl.plugins.`"

LOADERS_GROUP: str
"The entrypoint group name for the loaders. Defaults to `sparkrl.plugins.loaders`"


def as_json() -> dict[str, str]:
    """
    Returns the constants as a JSON-serializable dictionary.
    """
    return {
        "PROJECT_APPNAME": PROJECT_APPNAME,
        "PLUGINS_ENV": PLUGINS_ENV,
        "PROFILES_ENV": PROFILES_ENV,
        "GROUP_PREFIX": GROUP_PREFIX,
        "LOADERS_GROUP": LOADERS_GROUP,
    }


def reload() -> None:
    """
    Reload the constants.
    """
    global PROJECT_APPNAME, GROUP_PREFIX, PLUGINS_ENV, PROFILES_ENV, LOADERS_GROUP

    # TODO: once I rename the whole project
    # APPNAME = os.environ.get(APPNAME_ENV) or __name__.partition(".")[0]
    PROJECT_APPNAME = os.environ.get(APPNAME_ENV) or "sparkrl"
    GROUP_PREFIX = f"{PROJECT_APPNAME}.plugins."
    PLUGINS_ENV = f"{PROJECT_APPNAME.upper()}_PLUGINS"
    PROFILES_ENV = f"{PROJECT_APPNAME.upper()}_PROFILES"
    LOADERS_GROUP = GROUP_PREFIX + "loaders"


reload()
