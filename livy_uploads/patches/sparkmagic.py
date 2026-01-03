import importlib
import sys

from livy_uploads.patches.base import Patch
from livy_uploads.project import Project


class SparkMagicReloadPatch(Patch):
    """
    Reloads the SparkMagic extension.

    This will recursively re-import all modules with the `sparkmagic` prefix.
    """

    def apply(self, project: Project) -> None:
        # Find all loaded modules that start with 'sparkmagic'
        sparkmagic_modules = [name for name in sys.modules.keys() if name.startswith("sparkmagic")]

        # Sort modules by depth (parent before children) to ensure proper reload order
        sparkmagic_modules.sort(key=lambda name: name.count("."))

        # Reload each module
        for module_name in sparkmagic_modules:
            module = sys.modules[module_name]
            if module is not None:
                importlib.reload(module)
