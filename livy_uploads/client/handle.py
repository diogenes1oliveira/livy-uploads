import dataclasses
from typing import Any, MutableMapping, Optional

from livy_uploads.client.manager import SessionManager
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.exceptions import NoSuchSessionError
from livy_uploads.models.session import SessionInfo, SessionQuery


class SessionHandle:
    def __init__(
        self,
        info: SessionInfo,
        manager: SessionManager,
        globals: Optional[MutableMapping[str, Any]] = None,
        body: Optional[str] = None,
    ) -> None:
        self.info = info
        self.manager = manager
        self.globals = globals
        self.body = body or ""

    def as_json(self) -> dict[str, Any]:
        return {
            **self.info.as_json(compact=True, include_nulls=False),
            "url": self.endpoint.url,
        }

    def __str__(self) -> str:
        fields = ", ".join(f"{k}={v}" for k, v in self.as_json().items())
        return f"SessionHandle<{fields}>"

    @property
    def endpoint(self) -> LivyEndpoint:
        return self.manager.endpoint

    @classmethod
    def get(cls, manager: SessionManager, query: SessionQuery) -> "SessionHandle":
        if not query.has_identifier():
            if not (name := manager.default_session_name):
                raise ValueError("no identifier provided in query or in configuration")
            query = dataclasses.replace(query, name=name)

        info = manager.find_one(query)
        if info is None:
            raise NoSuchSessionError(query)

        return cls(info=info, manager=manager)
