__all__ = ("HttpConfig",)

import dataclasses
from pathlib import Path
from typing import Any, Optional, Union

from typing_extensions import Self

from livy_uploads.utils.typeutils import as_type


@dataclasses.dataclass(frozen=True)
class HttpConfig:
    proxy: Optional[str] = None
    verify: Union[bool, Path] = True
    page_size: int = 20
    max_results: int = 1001
    log_batch_size: int = 100
    poll_pause: float = 2.0
