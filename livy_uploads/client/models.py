__all__ = (
    "SessionInfo",
    "SessionStrategy",
    "CommandResult",
)

import collections.abc
import dataclasses
from enum import Enum
from typing import Any, Optional, TypeVar

from livy_uploads.utils.datautils import DeltaItem, delta_patch, delta_rolling_list
from livy_uploads.utils.typeutils import as_type

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class SessionInfo:
    id: int
    state: Optional["SessionState"] = None
    kind: Optional["SessionKind"] = None
    name: Optional[str] = None
    owner: Optional[str] = None
    proxy_user: Optional[str] = None
    app_info: Optional[dict[str, Any]] = None
    app_id: Optional[str] = None
    queue: Optional[str] = None
    log: Optional[list[str]] = None

    def delta(self, override: "SessionInfo") -> tuple[dict[str, DeltaItem], list[str]]:
        items = delta_patch(
            target=dataclasses.asdict(self),
            override=dataclasses.asdict(override),
            list_diff=delta_rolling_list,
        )
        try:
            log: list[str] = items.pop(".log").override
        except KeyError:
            log = []

        values = sorted(items.values())
        return {item.key: item for item in values}, log

    @property
    def driver_log_url(self) -> Optional[str]:
        if self.app_info is None:
            return None

        return as_type(self.app_info.get("driverLogUrl"), str, nullable=True)

    @property
    def spark_ui_url(self) -> Optional[str]:
        if self.app_info is None:
            return None

        return as_type(self.app_info.get("sparkUiUrl"), str, nullable=True)

    @property
    def alive(self) -> bool:
        return self.state not in SESSION_STATE_FINISHED

    @property
    def ready(self) -> bool:
        return self.alive and self.state not in SESSION_STATE_NOT_READY

    def no_logs(self) -> "SessionInfo":
        return dataclasses.replace(self, log=None)

    @classmethod
    def parse(self, body: Any) -> "SessionInfo":
        if not isinstance(body, collections.abc.Mapping):
            raise ValueError(f"body is not a mapping: {type(body)=!r}")

        state = as_type(body.get("state"), str, nullable=True)
        kind = as_type(body.get("kind"), str, nullable=True)

        try:
            return SessionInfo(
                id=body["id"],
                state=SessionState(state) if state else None,
                kind=SessionKind(kind) if kind else None,
                name=as_type(body.get("name"), str, nullable=True) or None,
                owner=as_type(body.get("owner"), str, nullable=True) or None,
                proxy_user=as_type(body.get("proxyUser"), str, nullable=True) or None,
                app_info=as_type(body.get("appInfo"), dict, nullable=True) or {},
                app_id=as_type(body.get("appId"), str, nullable=True) or None,
                queue=as_type(body.get("queue"), str, nullable=True) or None,
                log=as_type(body.get("log"), list, nullable=True) or None,
            )
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"failed to parse session info: " + str(e)) from e

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def as_json(self) -> dict[str, Any]:
        return {
            **self.as_dict(),
            "state": self.state.value if self.state else None,
            "kind": self.kind.value if self.kind else None,
        }


# copied from https://github.com/acroz/pylivy/blob/01bd6bf974323dbe366a7045f5b7cea0aac759dc/livy/models.py


class SessionKind(str, Enum):
    SPARK = "spark"
    PYSPARK = "pyspark"
    PYSPARK3 = "pyspark3"
    SPARKR = "sparkr"
    SQL = "sql"
    SHARED = "shared"


# Possible session states are defined here:
# https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/
# org/apache/livy/sessions/SessionState.scala
class SessionState(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    RECOVERING = "recovering"
    IDLE = "idle"
    RUNNING = "running"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


SESSION_STATE_NOT_READY = {SessionState.NOT_STARTED, SessionState.STARTING}
SESSION_STATE_FINISHED = {
    SessionState.ERROR,
    SessionState.DEAD,
    SessionState.KILLED,
    SessionState.SUCCESS,
}
