__all__ = (
    "KerberosConfig",
    "KerberosMutualAuth",
)

import dataclasses
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional


class KerberosMutualAuth(Enum):
    REQUIRED = 1
    OPTIONAL = 2
    DISABLED = 3


@dataclasses.dataclass(frozen=True)
class KerberosConfig:
    principal: str
    mutual_authentication: KerberosMutualAuth
    keytab: Annotated[Optional[Path], "sparkrl.resolve=path-or-data"] = None
    password: Optional[str] = None
    krb5_config: Annotated[Optional[Path], "sparkrl.resolve=path-or-text"] = None
    krb5_cache: Annotated[Optional[Path], "sparkrl.resolve=path-or-text"] = None
    target_name: Optional[str] = None
    delegate: Optional[bool] = None
    opportunistic_auth: Optional[bool] = None
