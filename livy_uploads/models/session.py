__all__ = (
    "SessionQuery",
    "SessionInfo",
    "SessionLog",
    "SessionDiff",
    "SessionEvents",
    "SESSION_CREATE_FIELDS",
)

import collections.abc
import dataclasses
import json
from datetime import datetime
from enum import Enum
from fnmatch import fnmatch
from typing import Any, Callable, Iterable, NamedTuple, Optional, TypeVar

from typing_extensions import Self

from livy_uploads.utils.datautils import DeltaItem, delta_patch
from livy_uploads.utils.typeutils import as_type

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


@dataclasses.dataclass(frozen=True)
class LivyClientConfig:
    url: str = "http://localhost:8998"
    page_size: int = 20
    max_results: int = 1001
    log_batch_size: int = 100
    poll_pause: float = 2.0
    default_headers: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class SessionQuery:
    name: Optional[str] = None
    state: Optional["SessionState"] = None
    id: Optional[int] = None
    kind: Optional["SessionKind"] = None
    appId: Optional[str] = None
    queue: Optional[str] = None
    owner: Optional[str] = None

    def patch(self, override: Self) -> Self:
        overrides = {k: v for k, v in dataclasses.asdict(override).items() if v is not None}
        return dataclasses.replace(self, **overrides)

    def matches(self, info: "SessionInfo") -> bool:
        return all(self._match_field(field, info) for field in dataclasses.fields(self))

    def _match_field(self, field: dataclasses.Field, info: "SessionInfo") -> bool:
        query = getattr(self, field.name)
        if query is None:
            return True

        value = getattr(info, field.name)
        if field.name == "name":
            if not value:
                return query == "*"  # type: ignore
            if not isinstance(value, str):
                raise ValueError(f"name must be a string: {type(value)=!r}")
            return fnmatch(value, query)
        else:
            return query == value  # type: ignore

    def as_dict(self) -> dict[str, Any]:
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}

    def has_identifier(self) -> bool:
        return self.id is not None or self.name is not None or self.appId is not None


@dataclasses.dataclass(frozen=True)
class SessionInfo:
    """Livy session info.

    Based on the Livy REST API Session object:
    https://livy.apache.org/docs/latest/rest-api.html#session
    """

    id: int
    """The session id (mandatory)"""

    appId: Optional[str] = None
    """The application id of this session"""

    state: Optional["SessionState"] = None
    """The session state"""

    kind: Optional["SessionKind"] = None
    """Session kind (spark, pyspark, sparkr, or sql)"""

    owner: Optional[str] = None
    """The owner of this session"""

    proxyUser: Optional[str] = None
    """User to impersonate when running"""

    doAs: Optional[str] = None
    """?doAs= parameter passed to the session POST request"""

    jars: Optional[list[str]] = None
    """jars to be used in this session"""

    pyFiles: Optional[list[str]] = None
    """Python files to be used in this session"""

    files: Optional[list[str]] = None
    """files to be used in this session"""

    driverMemory: Optional[str] = None
    """Amount of memory to use for the driver process"""

    driverCores: Optional[int] = None
    """Number of cores to use for the driver process"""

    executorMemory: Optional[str] = None
    """Amount of memory to use per executor process"""

    executorCores: Optional[int] = None
    """Number of cores to use for each executor"""

    numExecutors: Optional[int] = None
    """Number of executors to launch for this session"""

    archives: Optional[list[str]] = None
    """Archives to be used in this session"""

    queue: Optional[str] = None
    """The name of the YARN queue to which submitted"""

    name: Optional[str] = None
    """The name of this session"""

    conf: Optional[dict[str, str]] = None
    """Spark configuration properties"""

    heartbeatTimeoutInSecond: Optional[int] = None
    """Timeout in second to which session be orphaned"""

    ttl: Optional[str] = None
    """The timeout for this inactive session, example: 10m (10 minutes)"""

    appInfo: Optional[dict[str, Any]] = None
    """The detailed application info"""

    log: Optional[list[str]] = None
    """The log lines"""

    created_at: Optional[datetime] = None
    """The timestamp when the session was created"""

    def as_query(self, fields: Optional[Iterable[str]] = None) -> SessionQuery:
        if fields is None:
            self_fields = {f.name for f in dataclasses.fields(self)}
            fields = [f.name for f in dataclasses.fields(SessionQuery) if f.name in self_fields]

        kwargs = {name: getattr(self, name) for name in fields}
        return SessionQuery(**kwargs)

    @classmethod
    def parse(cls, body: Any) -> Self:
        """Parse a SessionInfo from a JSON response body."""
        if not isinstance(body, collections.abc.Mapping):
            raise ValueError(f"body is not a mapping: {type(body)=!r}")

        try:
            return cls(
                id=as_type(body.get("id"), int, nullable=True) or 0,
                appId=as_type(body.get("appId"), str, nullable=True),
                state=SessionState.parse_optional(body.get("state")),
                kind=SessionKind.parse_optional(body.get("kind")),
                owner=as_type(body.get("owner"), str, nullable=True),
                proxyUser=as_type(body.get("proxyUser"), str, nullable=True),
                jars=as_type(body.get("jars"), list, nullable=True),
                pyFiles=as_type(body.get("pyFiles"), list, nullable=True),
                files=as_type(body.get("files"), list, nullable=True),
                driverMemory=as_type(body.get("driverMemory"), str, nullable=True),
                driverCores=as_type(body.get("driverCores"), int, nullable=True),
                executorMemory=as_type(body.get("executorMemory"), str, nullable=True),
                executorCores=as_type(body.get("executorCores"), int, nullable=True),
                numExecutors=as_type(body.get("numExecutors"), int, nullable=True),
                archives=as_type(body.get("archives"), list, nullable=True),
                queue=as_type(body.get("queue"), str, nullable=True),
                name=as_type(body.get("name"), str, nullable=True),
                conf=as_type(body.get("conf"), dict, nullable=True),
                heartbeatTimeoutInSecond=as_type(body.get("heartbeatTimeoutInSecond"), int, nullable=True),
                ttl=as_type(body.get("ttl"), str, nullable=True),
                appInfo=as_type(body.get("appInfo"), dict, nullable=True),
                log=as_type(body.get("log"), list, nullable=True),
                doAs=as_type(body.get("doAs"), str, nullable=True),
            )
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError("failed to parse session config: " + str(e)) from e

    def diff(self, current: Self) -> "SessionDiff":
        if self.id != current.id:
            raise ValueError(f"session ids do not match: {self.id} != {current.id}")

        items = delta_patch(
            target=dataclasses.asdict(self),
            override=dataclasses.asdict(current),
        )

        values = sorted(items.values())
        attributes = {item.key: item for item in values}
        return SessionDiff(changes=attributes, current=current)

    @property
    def driverLogUrl(self) -> Optional[str]:
        if self.appInfo is None:
            return None

        return as_type(self.appInfo.get("driverLogUrl"), str, nullable=True)

    @property
    def sparkUiUrl(self) -> Optional[str]:
        if self.appInfo is None:
            return None

        return as_type(self.appInfo.get("sparkUiUrl"), str, nullable=True)

    @property
    def alive(self) -> bool:
        return self.state not in SESSION_STATE_FINISHED

    @property
    def ready(self) -> bool:
        return self.alive and self.state not in SESSION_STATE_NOT_READY

    def trim(self, keep_logs: bool = False) -> Self:
        if not keep_logs:
            return dataclasses.replace(self, log=None)
        else:
            return self

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def as_attrs(self) -> str:
        return " ".join(f"{k}={v}" for k, v in self.as_json(compact=True).items())

    def as_json(self, compact: bool = False, include_nulls: bool = True) -> dict[str, Any]:
        """Convert SessionConfig to a JSON-serializable dictionary."""

        if compact:
            result = {
                "id": self.id,
                "name": self.name,
                "appId": self.appId,
                "state": self.state.as_json() if self.state else None,
            }
        else:
            result = {}

            for field in dataclasses.fields(self):
                value = getattr(self, field.name)
                if hasattr(value, "as_json"):
                    value = value.as_json()
                elif isinstance(value, datetime):
                    value = value.isoformat(timespec="seconds")
                result[field.name] = value

        if not include_nulls:
            result = {k: v for k, v in result.items() if v is not None}

        return result

    def to_json(self, compact: bool = False) -> str:
        return json.dumps(self.as_json(compact=compact))


@dataclasses.dataclass(frozen=True)
class SessionLog:
    id: int
    text: str
    offset: int = 0
    time: Optional[datetime] = None
    name: Optional[str] = None

    def as_json(self) -> dict[str, Any]:
        values = {
            "id": self.id,
            "text": self.text,
            "time": self.time.isoformat() if self.time else None,
            "name": self.name,
            "offset": self.offset,
        }
        return {k: v for k, v in values.items() if v is not None}


class SessionDiff(NamedTuple):
    current: "SessionInfo"
    changes: dict[str, DeltaItem] = dataclasses.field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        return {
            "current": {
                "id": self.current.id,
                "name": self.current.name,
                "appId": self.current.appId,
                "state": self.current.state.as_json() if self.current.state else None,
            },
            "changes": {k: {"previous": v.current, "current": v.override} for k, v in self.changes.items()},
        }


class SessionKind(str, Enum):
    """
    Possible session kinds are defined here:
    https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/org/apache/livy/sessions/Kind.scala
    """

    # copied from https://github.com/acroz/pylivy/blob/01bd6bf974323dbe366a7045f5b7cea0aac759dc/livy/models.py
    SPARK = "spark"
    PYSPARK = "pyspark"
    PYSPARK3 = "pyspark3"
    SPARKR = "sparkr"
    SQL = "sql"
    SHARED = "shared"

    def as_json(self) -> str:
        return self.value

    @classmethod
    def parse(cls, value: Any) -> Self:
        parsed = cls.parse_optional(value)
        if parsed is None:
            raise ValueError(f"no session kind found in {value=!r}")
        return parsed

    @classmethod
    def parse_optional(cls, value: Any) -> Optional[Self]:
        if value is None:
            return None

        if not isinstance(value, str):
            raise ValueError(f"value is not a string: {type(value)=!r}")

        value = value.strip().lower()
        if not value:
            return None

        return cls(value)


class SessionState(str, Enum):
    """
    Possible session states are defined here:
    https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/org/apache/livy/sessions/SessionState.scala
    """

    # copied from https://github.com/acroz/pylivy/blob/01bd6bf974323dbe366a7045f5b7cea0aac759dc/livy/models.py
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
    # added for filtering
    GONE = "gone"

    def as_json(self) -> str:
        return self.value

    @classmethod
    def parse(cls, value: Any) -> Self:
        parsed = cls.parse_optional(value)
        if parsed is None:
            raise ValueError(f"no session state found in {value=!r}")
        return parsed

    @classmethod
    def parse_optional(cls, value: Any) -> Optional[Self]:
        if value is None:
            return None

        if not isinstance(value, str):
            raise ValueError(f"value is not a string: {type(value)=!r}")

        value = value.strip().lower()
        if not value:
            return None

        return cls(value)


SESSION_STATE_NOT_READY = {SessionState.NOT_STARTED, SessionState.STARTING}
SESSION_STATE_FINISHED = {
    SessionState.ERROR,
    SessionState.DEAD,
    SessionState.KILLED,
    SessionState.SUCCESS,
    SessionState.GONE,
}

# Fields that can be passed when creating a session via POST /sessions
# Based on: https://livy.apache.org/docs/latest/rest-api.html#post-sessions
SESSION_CREATE_FIELDS: tuple[str, ...] = (
    "kind",
    "proxyUser",
    "jars",
    "pyFiles",
    "files",
    "driverMemory",
    "driverCores",
    "executorMemory",
    "executorCores",
    "numExecutors",
    "archives",
    "queue",
    "name",
    "conf",
    "heartbeatTimeoutInSecond",
    "ttl",
    "doAs",
)


@dataclasses.dataclass(frozen=True)
class SessionEvents:
    created: list[SessionInfo] = dataclasses.field(default_factory=list)
    changed: list[SessionDiff] = dataclasses.field(default_factory=list)
    gone: list[SessionInfo] = dataclasses.field(default_factory=list)
    logs: list[SessionLog] = dataclasses.field(default_factory=list)
    time: datetime = dataclasses.field(default_factory=lambda: datetime.now().astimezone())

    def as_json(self) -> dict[str, Any]:
        return {
            "created": [info.as_json(compact=True) for info in self.created],
            "changed": [diff.as_json() for diff in self.changed],
            "gone": [info.as_json(compact=True) for info in self.gone],
            "logs": [log.as_json() for log in self.logs],
            "time": self.time.isoformat(timespec="seconds"),
        }
