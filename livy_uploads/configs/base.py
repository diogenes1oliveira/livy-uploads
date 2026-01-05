__all__ = ("Configurable",)

from abc import abstractmethod
from typing import ClassVar, Protocol, runtime_checkable

from livy_uploads.plugins import Implementation


@runtime_checkable
class Configurable(Implementation, Protocol):

    __plugin_group__: ClassVar[str] = "sparkrl.plugins.configurables"

    @abstractmethod
    def setup(self) -> None:
        raise NotImplementedError
