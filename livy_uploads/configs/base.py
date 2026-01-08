__all__ = ("Configurable",)

from typing import Any, ClassVar, Protocol, runtime_checkable

from livy_uploads.plugins import Implementation


@runtime_checkable
class Configurable(Implementation, Protocol):

    __plugin_group__: ClassVar[str] = "sparkrl.plugins.configurables"

    def setup(self) -> Any:
        pass
