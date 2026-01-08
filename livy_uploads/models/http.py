__all__ = ("HttpConfig",)

import dataclasses
from pathlib import Path
from typing import Any, Optional, Union

import requests
from typing_extensions import Self

from livy_uploads.utils.typeutils import as_type


@dataclasses.dataclass(frozen=True)
class HttpConfig:
    proxy: Optional[str] = None
    ssl_verify: Optional[bool] = None
    ca_bundle: Optional[Path] = None
    page_size: int = 20
    max_results: int = 1001
    log_batch_size: int = 100
    poll_pause: float = 2.0

    def __post_init__(self) -> None:
        if self.ssl_verify is False and self.ca_bundle is not None:
            raise ValueError("cannot specify both ssl_verify=False and ca_bundle")

    @property
    def verify(self) -> Union[bool, str]:
        if self.ssl_verify is False:
            return False

        if self.ca_bundle is not None:
            return str(self.ca_bundle)
        else:
            return True
