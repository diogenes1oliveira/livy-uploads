__all__ = ("HttpConfig",)

import collections.abc
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

    @classmethod
    def parse(cls, body: Any) -> Self:
        if not isinstance(body, collections.abc.Mapping):
            raise ValueError(f"body is not a mapping: {type(body)=!r}")

        try:
            verify: Optional[Union[bool, Path]] = as_type(body.get("verify"), bool, nullable=True)
        except (ValueError, TypeError):
            verify_s = as_type(body.get("verify"), str, nullable=True) or ""
            verify_s = verify_s.strip()
            if verify_s:
                from livy_uploads.project import Project

                verify = Project.get().config.resolve_file(verify_s, filename="ca-certificates.crt", binary=False)
        else:
            verify = None

        return cls(
            proxy=as_type(body.get("proxy"), str, nullable=True),
            verify=verify if verify is not None else True,
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
