from abc import abstractmethod
from typing import ClassVar, Protocol

from livy_uploads.plugins.impls import Implementation


class Patch(Implementation, Protocol):
    """
    Ad-hoc patches.
    """

    __plugin_group__: ClassVar[str] = "sparkrl.plugins.patches"
    "Entrypoint group name for loading Patch implementations."

    @abstractmethod
    def apply_patch(self) -> None:
        raise NotImplementedError
