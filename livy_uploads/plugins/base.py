import os
from collections.abc import Mapping
from typing import Protocol, runtime_checkable


@runtime_checkable
class SetupPlugin(Protocol):
    def setup(self, basedir: os.PathLike, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
        """
        Executes the plugin's setup logic.

        Args:
            basedir: The base directory of the project.
            env_filename: The name of the environment file.
            env: The current environment variables.

        Returns:
            A mapping of environment variables to override.
        """
        raise NotImplementedError
