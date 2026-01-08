__all__ = ("Project",)

import contextvars
import functools
import logging
from pathlib import Path
from typing import Any, ClassVar, Optional

from typing_extensions import Self

from livy_uploads.configs.base import Configurable
from livy_uploads.configs.configfiles import ConfigFileLoader
from livy_uploads.configs.envs import EnvFileLoader
from livy_uploads.configs.logs import LoggingConfigurator
from livy_uploads.converters.base import Converter, ConverterCustomizer
from livy_uploads.converters.cattrs import CattrsConverter
from livy_uploads.plugins import ImplementationLoader
from livy_uploads.plugins.impls import get_implementation, get_implementations
from livy_uploads.project.basedir import find_basedir
from livy_uploads.project.loader import GlobalEnvLoader

LOGGER = logging.getLogger(__name__)

_configuring = contextvars.ContextVar("configuring", default=False)


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
        self._converter: Converter = CattrsConverter()

    @functools.cached_property
    def basedir(self) -> Path:
        return find_basedir()

    @functools.cached_property
    def cachedir(self) -> Path:
        return self.basedir / "var" / "cache"

    @functools.cached_property
    def configurables(self) -> list[Configurable]:
        """
        Returns the list of enabled configurables in the config.
        """
        names = self.configs.get(["configurables"], t=list[str], nullable=True) or []
        LOGGER.debug("instantiating configurables: %s", names)
        return [get_implementation(Configurable, typename=name)() for name in names]

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

        _configuring.set(True)
        try:
            self.envs.setup()
            self.logs.setup()  # again now after loading the .env

            self._configure_converter()

            self.configs.setup()

            self._setup_configurables()
            self._configured = True
        finally:
            _configuring.set(False)

        return self

    @functools.cached_property
    def impls(self) -> ImplementationLoader:
        self._assert_configured()

        loaders = list[ImplementationLoader]()
        for loader in self._loader.loaders:
            if isinstance(loader, ImplementationLoader):
                loaders.append(loader)

        if not loaders:
            raise ValueError("No implementation loader found")

        return ImplementationLoader.merge(loaders)

    @functools.cached_property
    def envs(self) -> EnvFileLoader:
        self._assert_initialized()
        (loader,) = EnvFileLoader().resolve(basedir=self.basedir)
        return loader

    @functools.cached_property
    def logs(self) -> LoggingConfigurator:
        return LoggingConfigurator()

    @functools.cached_property
    def configs(self) -> ConfigFileLoader:
        self._assert_initialized()
        (loader,) = ConfigFileLoader().resolve(basedir=self.basedir)
        return loader

    @property
    def converter(self) -> Converter:
        self._assert_configured()
        return self._converter

    def initialize(self) -> None:
        """
        Executes a very basic and cheap initialization of the project: just the logging and plugin loader.

        You should prefer invoking `Project.setup()` directly instead.
        """
        self.logs.setup()
        self._loader.setup()
        (self._loader,) = self._loader.resolve(basedir=self.basedir)
        self._initialized = True

    def _assert_initialized(self) -> None:
        assert self._initialized, "Project not initialized yet. Call `Project.initialize()` at your entrypoint."

    def _assert_configured(self) -> None:
        assert (
            self._configured or _configuring.get()
        ), "Project not configured yet. Call `Project.setup()` after loading."

    def _configure_converter(self) -> None:
        self._converter.setup()

        for name, customizer_cls in get_implementations(ConverterCustomizer, pattern="*").items():
            LOGGER.debug("applying customizer %r (class %r)", name, customizer_cls)
            customizer = customizer_cls()
            self._converter = customizer.customize_converter(self._converter)

        self.configs.set_converter(self._converter)

    def _setup_configurables(self) -> None:
        for configurable in self.configurables:
            LOGGER.debug("setting up configurable %r (class %r)", configurable.impl_typename(), configurable.__class__)
            configurable.setup()
