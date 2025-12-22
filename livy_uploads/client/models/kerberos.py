__all__ = (
    "KerberosConfig",
    "KerberosMutualAuth",
)

import collections.abc
import dataclasses
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self

from livy_uploads.utils.typeutils import as_type


class KerberosMutualAuth(Enum):
    REQUIRED = 1
    OPTIONAL = 2
    DISABLED = 3

    @classmethod
    def parse(cls, value: Any) -> "KerberosMutualAuth":
        if value is None:
            return cls.OPTIONAL

        if isinstance(value, int):
            if not value:
                return cls.OPTIONAL
            return cls(value)

        if isinstance(value, str):
            value = value.strip().upper()
            if not value:
                return cls.OPTIONAL

            try:
                return getattr(cls, value)  # type: ignore
            except AttributeError:
                raise ValueError(f"invalid mutual authentication value: {value!r}") from None

        raise ValueError(f"invalid mutual authentication value: {value!r}")

    def as_json(self) -> str:
        return self.name.upper()


@dataclasses.dataclass(frozen=True)
class KerberosConfig:
    principal: str
    mutual_authentication: KerberosMutualAuth
    keytab: Optional[str] = None
    password: Optional[str] = None
    krb5_config: Optional[str] = None
    krb5_cache: Optional[str] = None
    target_name: Optional[str] = None
    delegate: Optional[bool] = None
    opportunistic_auth: Optional[bool] = None

    @classmethod
    def parse(cls, body: Any, basedir: Optional[Path] = None) -> Optional[Self]:
        basedir = basedir or Path.cwd()

        if body is None:
            return None

        if not isinstance(body, collections.abc.Mapping):
            raise ValueError(f"body is not a mapping: {type(body)=!r}")

        if not (principal := body.get("principal")):
            return None

        return cls(
            principal=as_type(principal, str),
            keytab=cls._relativize(body.get("keytab"), basedir),
            password=as_type(body.get("password"), str, nullable=True) or None,
            krb5_config=cls._relativize(body.get("krb5_config"), basedir),
            krb5_cache=cls._relativize(body.get("krb5_cache"), basedir),
            mutual_authentication=KerberosMutualAuth.parse(body.get("mutual_authentication")),
            target_name=as_type(body.get("target_name"), str, nullable=True) or None,
            delegate=as_type(body.get("delegate"), bool, nullable=True) or None,
            opportunistic_auth=as_type(body.get("opportunistic_auth"), bool, nullable=True) or None,
        )

    def as_json(self) -> dict[str, Any]:
        return {
            "principal": self.principal,
            "keytab": self.keytab,
            "password": self.password,
            "krb5_config": self.krb5_config,
            "krb5_cache": self.krb5_cache,
            "mutual_authentication": self.mutual_authentication.as_json(),
            "target_name": self.target_name,
            "delegate": self.delegate,
            "opportunistic_auth": self.opportunistic_auth,
        }

    @classmethod
    def _relativize(cls, value: Any, basedir: Path) -> Optional[str]:
        s = as_type(value, str, nullable=True) or None
        if not s:
            return None
        if Path(s).is_absolute():
            return s
        else:
            return str((basedir / s).absolute())
