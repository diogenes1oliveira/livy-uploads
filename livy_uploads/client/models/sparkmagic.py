__all__ = (
    "SPARKMAGIC_CONFIG_ENVVAR",
    "SparkMagicConfig",
)

import dataclasses
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self

SPARKMAGIC_CONFIG_ENVVAR = "SPARKMAGIC_CONF_DIR"

from livy_uploads.client.models.http import HttpConfig
from livy_uploads.client.models.kerberos import KerberosConfig
from livy_uploads.client.models.session import SESSION_CREATE_FIELDS, SessionInfo
from livy_uploads.utils.datautils import deep_merge, keep_only
from livy_uploads.utils.retry_policy import CustomIntervalRetry, MaxTime, RetryPolicy
from livy_uploads.utils.typeutils import as_type

CREDENTIAL_FIELDS = ("username", "password", "url", "auth")


@dataclasses.dataclass(frozen=True)
class SparkMagicConfig:
    session_configs: dict[str, Any] = dataclasses.field(default_factory=dict)
    session_configs_defaults: dict[str, Any] = dataclasses.field(default_factory=dict)
    kernel_python_credentials: dict[str, Any] = dataclasses.field(default_factory=dict)
    custom_headers: dict[str, Any] = dataclasses.field(default_factory=dict)
    retry_seconds_to_sleep_list: list[float] = dataclasses.field(default_factory=lambda: [0.2, 0.5, 1, 3, 5])
    configurable_retry_policy_max_retries: int = dataclasses.field(default=8)
    kerberos_config: Optional[KerberosConfig] = None
    http_config: HttpConfig = dataclasses.field(default_factory=HttpConfig)
    readiness_policy: RetryPolicy = MaxTime(time=60, pause=2.0)
    finish_policy: RetryPolicy = MaxTime(time=15.0, pause=1.5)

    @property
    def livy_url(self) -> str:
        return as_type(self.kernel_python_credentials.get("url"), str)

    @property
    def retry_policy(self) -> RetryPolicy:
        return CustomIntervalRetry(
            retry_seconds_to_sleep_list=tuple(self.retry_seconds_to_sleep_list),
            configurable_retry_policy_max_retries=self.configurable_retry_policy_max_retries,
        )

    @classmethod
    def parse(cls, body: Any, basedir: Optional[Path] = None) -> Self:
        kwargs = as_type(body, dict)
        basedir = basedir or Path.cwd()

        session_configs = as_type(kwargs.get("session_configs"), dict, nullable=True) or {}
        session_configs = SessionInfo.parse({**session_configs, "id": 0}).as_json()
        session_configs = keep_only(session_configs, SESSION_CREATE_FIELDS)

        session_configs_defaults = as_type(kwargs.get("session_configs_defaults"), dict, nullable=True) or {}
        session_configs_defaults = SessionInfo.parse({**session_configs_defaults, "id": 0}).as_json()
        session_configs_defaults = keep_only(session_configs_defaults, SESSION_CREATE_FIELDS)

        kernel_python_credentials = as_type(kwargs.get("kernel_python_credentials"), dict, nullable=True) or {}
        kernel_python_credentials = keep_only(kernel_python_credentials, CREDENTIAL_FIELDS)
        as_type(kernel_python_credentials.get("url"), str)  # just validate it's there

        retry_seconds_to_sleep_list = as_type(kwargs.get("retry_seconds_to_sleep_list"), list, nullable=True)
        configurable_retry_policy_max_retries = as_type(
            kwargs.get("configurable_retry_policy_max_retries"), int, nullable=True
        )

        kerberos_config = KerberosConfig.parse(kwargs.get("kerberos_config"), basedir=basedir)
        http_config = HttpConfig.parse(kwargs.get("http_config") or {})

        kwargs = dict(
            session_configs=session_configs,
            session_configs_defaults=session_configs_defaults,
            kernel_python_credentials=kernel_python_credentials,
            custom_headers=as_type(kwargs.get("custom_headers"), dict, nullable=True) or {},
            kerberos_config=kerberos_config,
            http_config=http_config,
            retry_seconds_to_sleep_list=retry_seconds_to_sleep_list,
            configurable_retry_policy_max_retries=configurable_retry_policy_max_retries,
            readiness_policy=RetryPolicy.parse(kwargs.get("readiness_policy")),
            finish_policy=RetryPolicy.parse(kwargs.get("finish_policy")),
        )

        return cls(**{k: v for k, v in kwargs.items() if v is not None})

    def as_json(self) -> dict[str, Any]:
        return {
            "session_configs": self.session_configs,
            "session_configs_defaults": self.session_configs_defaults,
            "kernel_python_credentials": self.kernel_python_credentials,
            "custom_headers": self.custom_headers,
            "http_config": self.http_config.as_json(),
            "kerberos_config": self.kerberos_config.as_json() if self.kerberos_config else None,
            "retry_seconds_to_sleep_list": self.retry_seconds_to_sleep_list,
            "configurable_retry_policy_max_retries": self.configurable_retry_policy_max_retries,
            "readiness_policy": self.readiness_policy.as_json() if self.readiness_policy else None,
            "finish_policy": self.finish_policy.as_json() if self.finish_policy else None,
        }

    def as_session_info(self) -> SessionInfo:
        configs = deep_merge(self.session_configs_defaults, self.session_configs)
        return SessionInfo.parse({**configs, "id": 0})

    def as_session_post_json(self) -> dict[str, Any]:
        """
        Returns a JSON object that can be used to POST to /sessions.
        """
        configs = deep_merge(self.session_configs_defaults, self.session_configs)
        configs.pop("owner", None)
        return {k: v for k, v in configs.items() if v is not None}
