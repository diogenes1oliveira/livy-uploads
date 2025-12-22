__all__ = ("HttpConfig",)

import collections.abc
import dataclasses
from typing import Any, Optional

from typing_extensions import Self

from livy_uploads.utils.typeutils import as_type


@dataclasses.dataclass(frozen=True)
class HttpConfig:
    proxy: Optional[str] = None
    verify: Optional[bool] = None
    page_size: int = 20
    max_results: int = 1001
    log_batch_size: int = 100
    poll_pause: float = 2.0

    @classmethod
    def parse(cls, body: Any) -> Self:
        if not isinstance(body, collections.abc.Mapping):
            raise ValueError(f"body is not a mapping: {type(body)=!r}")

        return cls(
            proxy=as_type(body.get("proxy"), str, nullable=True),
            verify=as_type(body.get("verify"), bool, nullable=True),
            page_size=as_type(body.get("page_size"), int, nullable=True) or 20,
            max_results=as_type(body.get("max_results"), int, nullable=True) or 1001,
            log_batch_size=as_type(body.get("log_batch_size"), int, nullable=True) or 100,
            poll_pause=as_type(body.get("poll_pause"), float, nullable=True) or 2.0,
        )

    def as_json(self) -> dict[str, Any]:
        return {
            "proxy": self.proxy,
            "verify": self.verify,
            "page_size": self.page_size,
            "max_results": self.max_results,
            "log_batch_size": self.log_batch_size,
            "poll_pause": self.poll_pause,
        }
