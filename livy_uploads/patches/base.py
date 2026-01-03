from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from livy_uploads.project import Project
else:
    Project = Any


class Patch(ABC):
    @abstractmethod
    def apply(self, project: Project) -> None:
        raise NotImplementedError
