__all__ = (
    "constants",
    "LOADER",
)

from . import constants
from .loader import GlobalLoader

LOADER = GlobalLoader()
