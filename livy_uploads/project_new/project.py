__all__ = ("Project",)

import logging
from typing import Any, ClassVar, Optional

from typing_extensions import Self

from livy_uploads.configs.logs import LoggingConfigurator
from livy_uploads.plugins import ImplementationLoader
from livy_uploads.project_new.loader import GlobalEnvLoader

LOGGER = logging.getLogger(__name__)


class Project:
    """
    The singleton class representing the current project.
    """

    _instance: ClassVar[Optional["Project"]] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Project":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._initialized = False
        self._configured = False
        self._loader = GlobalEnvLoader()
        self._logging_configurator = LoggingConfigurator()

    @classmethod
    def get(cls) -> "Project":
        """
        Gets the configured project instance.

        Raises:
            AssertionError: if the project is not configured yet.
        """
        instance = cls()
        instance._assert_configured()
        return instance

    @property
    def loader(self) -> "GlobalEnvLoader":
        """
        The global plugin loader.

        Raises:
            AssertionError: if the project is not initialized yet.
        """
        self._assert_initialized()
        return self._loader

    def setup(self) -> Self:
        """
        Sets up the project instance.

        Should be invoked at least once in the main entrypoint after initialization.
        """
        if self._configured:
            return self

        if not self._initialized:
            self.initialize()

        self._loader.resolve()
        self._configured = True
        return self

    @property
    def impls(self) -> tuple[ImplementationLoader, ...]:
        self._assert_configured()

        loaders = list[ImplementationLoader]()
        for loader in self._loader.loaders:
            if isinstance(loader, ImplementationLoader):
                loaders.append(loader)

        if not loaders:
            raise ValueError("No implementation loader found")

        return tuple(loaders)
        # return ImplementationLoader(groups=())

    def initialize(self) -> None:
        """
        Executes a very basic and cheap initialization of the project: just the logging and plugin loader.

        You should prefer invoking `Project.setup()` directly instead.
        """
        self._logging_configurator.setup()
        self._loader.setup()
        self._initialized = True

    def _assert_initialized(self) -> None:
        assert self._initialized, "Project not initialized yet. Call `Project.initialize()` at your entrypoint."

    def _assert_configured(self) -> None:
        assert self._configured, "Project not configured yet. Call `Project.setup()` after loading."
