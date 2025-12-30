__all__ = ("SessionManager",)

import dataclasses
import itertools
import json
import logging
import sys
import threading
from concurrent.futures import CancelledError
from contextlib import ExitStack
from datetime import datetime
from typing import (
    Any,
    BinaryIO,
    Callable,
    Iterator,
    List,
    Literal,
    Optional,
    Protocol,
    TextIO,
    Type,
    TypeVar,
    Union,
    overload,
)
from urllib.parse import quote

from pytest import Session

from livy_uploads.models.session import (
    SESSION_STATE_FINISHED,
    SessionDiff,
    SessionEvents,
    SessionInfo,
    SessionLog,
    SessionQuery,
    SessionState,
)
from livy_uploads.models.sparkmagic import SparkMagicConfig
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.exceptions import (
    LivyRequestError,
    LivySessionDeadError,
    OperationCanceledError,
    SessionGoneError,
    UnexpectedStateError,
)
from livy_uploads.session import LivySession
from livy_uploads.utils.datautils import delta_rolling_list
from livy_uploads.utils.retry_policy import RetryPolicy
from livy_uploads.utils.typeutils import as_type

S = TypeVar("S", SessionInfo, LivySession)

LOGGER = logging.getLogger(__name__)


class SessionManager:
    def __init__(self, endpoint: LivyEndpoint, config: SparkMagicConfig) -> None:
        self.endpoint = endpoint
        self.config = config
        self._log_offsets: dict[int, int] = {}
        self._log_lines: dict[int, list[str]] = {}
        self._created_at: dict[int, datetime] = {}

    def find(self, query: SessionQuery, refresh: bool = False, keep_logs: bool = False) -> Iterator[SessionInfo]:
        offset = 0
        page_size = self.config.http_config.page_size

        if query.id is not None and refresh:
            info = self.get(query.id, keep_logs=keep_logs)
            if info is not None and query.matches(info):
                yield info
            return

        while True:
            LOGGER.debug("fetching sessions from offset %d", offset)
            r = self.endpoint.request("GET", f"/sessions?from={offset}&size={page_size}")
            body = as_type(r.json(), dict)
            raw_sessions = as_type(body.get("sessions"), list, nullable=True) or []
            offset += len(raw_sessions)

            for raw_session in raw_sessions:
                info = self._parse(raw_session).trim(keep_logs=keep_logs)
                if not query.matches(info):
                    LOGGER.debug("session %r does not match query: %s", info.as_json(), query.as_dict())
                    continue

                if not refresh:
                    yield info
                else:
                    yield self.refresh(info, keep_logs=keep_logs)

            if not raw_sessions or len(raw_sessions) < page_size:
                break

    def find_all(self, query: SessionQuery, refresh: bool = False, keep_logs: bool = False) -> List[SessionInfo]:
        max_results = self.config.http_config.max_results
        return list(itertools.islice(self.find(query, refresh=refresh, keep_logs=keep_logs), max_results))

    def find_one(self, query: SessionQuery, refresh: bool = False, keep_logs: bool = False) -> Optional[SessionInfo]:
        if not (query.id is not None or query.name is not None or query.appId is not None):
            if not (name := self.config.as_session_info().name):
                raise ValueError("no identifier provided in query or in configuration")
            query = dataclasses.replace(query, name=name)

        infos = self.find_all(query, refresh=refresh, keep_logs=keep_logs)
        if not infos:
            return None
        elif len(infos) == 1:
            return infos[0]
        else:
            raise ValueError(f"multiple sessions found matching the filters: {query.as_dict()}")

    def get(self, id: int, keep_logs: bool = False) -> Optional[SessionInfo]:
        try:
            r = self.endpoint.request("GET", f"/sessions/{id}")
            info = self._parse(r.json())
            return info.trim(keep_logs=keep_logs)
        except LivyRequestError as e:
            if e.response.status_code == 404:
                self._uncache(id)
                return None
            else:
                raise

    def refresh(self, info: SessionInfo, keep_logs: bool = False) -> SessionInfo:
        new_info = self.get(info.id, keep_logs=keep_logs)
        if new_info is not None:
            return new_info
        else:
            self._uncache(info.id)
            return dataclasses.replace(info, state=SessionState.GONE)

    def poll(self, id: int) -> SessionState:
        try:
            r = self.endpoint.request("GET", f"/sessions/{id}/state")
            body = as_type(r.json(), dict)
            return SessionState.parse(body.get("state"))
        except LivyRequestError as e:
            if e.response.status_code == 404:
                self._uncache(id)
                return SessionState.GONE
            else:
                raise

    def create(self, name: Optional[str] = None, recreate: Optional[bool] = None) -> SessionInfo:
        info = self.find_one(SessionQuery(name=name))
        if info is not None:
            if not recreate:
                LOGGER.info("session %r already exists, reusing", info.as_json(compact=True))
                return info
            else:
                self.delete(info.id)
                self.wait(SessionQuery(id=info.id), SessionState.GONE, retry_policy=self.config.finish_policy)

        body = self.config.as_session_post_json()
        if not name and not (name := self.config.as_session_info().name):
            raise ValueError("no name provided in configuration")

        body["name"] = name
        path = "/sessions"
        doAs = body.pop("doAs", None)
        if doAs:
            path += f"?doAs={quote(doAs)}"

        LOGGER.info("creating session with name=%r", name)
        LOGGER.debug("POST path=%r body=%r", path, body)
        r = self.endpoint.request("POST", path=path, json=body)
        info = self._parse(r.json()).trim(keep_logs=False)
        LOGGER.info("session name=%r created with id=%d", name, info.id)
        return info

    def logs(self, id: int, offset: Optional[int] = None, info: Optional[SessionInfo] = None) -> list[SessionLog]:
        if info is not None and info.id != id:
            raise ValueError(f"session ids do not match: {info.id=} != {id=}")

        if info is None:
            name = "<GONE>"
        elif not info.name:
            name = "<ANONYMOUS>"
        else:
            name = info.name

        offset = offset if offset is not None else self._log_offsets.get(id, 0)
        batch_size = self.config.http_config.log_batch_size

        try:
            # a circular queue for some reason...
            r = self.endpoint.request("GET", f"/sessions/{id}/log?from=0&size={batch_size}")
            body = as_type(r.json(), dict)
            lines = as_type(body.get("log"), list, nullable=True) or []
        except LivyRequestError as e:
            if e.response.status_code == 404:
                LOGGER.warning("session id=%d not found", id)
                # display the session logs one last time
                if self._uncache(id):
                    lines = []
                else:
                    return []
            else:
                raise

        prev_lines = self._log_lines.get(id) or []
        if not prev_lines:
            new_lines: list[str] = lines
        else:
            new_lines = delta_rolling_list(prev_lines, lines)

        self._log_lines[id] = lines
        self._log_offsets[id] = offset + len(new_lines)
        return [SessionLog(id=id, text=line, offset=offset + i, name=name) for i, line in enumerate(new_lines)]

    def _uncache(self, id: int) -> bool:
        self._log_lines.pop(id, None)
        self._created_at.pop(id, None)
        try:
            self._log_offsets.pop(id)
            return True
        except KeyError:
            return False

    def _parse(self, body: Any) -> SessionInfo:
        info = SessionInfo.parse(body)
        # TODO: maybe there's a better way to get the created_at time?
        return dataclasses.replace(info, created_at=self._created_at.setdefault(info.id, datetime.now().astimezone()))

    def delete(self, id: int) -> None:
        LOGGER.info("deleting session id=%d", id)
        try:
            self.endpoint.request("DELETE", f"/sessions/{id}")
        except LivyRequestError as e:
            if e.response.status_code == 404:
                LOGGER.warning("session id=%d not found", id)
                self._uncache(id)
            else:
                raise

        self._uncache(id)

    def delete_all(self, query: SessionQuery) -> None:
        infos = self.find_all(query)
        if not infos:
            return
        ids = [info.id for info in infos]
        LOGGER.info("deleting %d sessions: %s", len(ids), ids)
        for info in self.find_all(query):
            self.delete(info.id)

    def watch(self, query: SessionQuery, include_logs: bool = False, refresh: bool = False) -> Iterator[SessionEvents]:
        def _get_infos() -> dict[int, SessionInfo]:
            return {info.id: info for info in self.find(query, refresh=refresh, keep_logs=False)}

        # prime the state
        curr_infos = _get_infos()
        yield SessionEvents()

        while True:
            events = SessionEvents()
            new_infos = _get_infos()

            for id, info in curr_infos.items():
                if id not in new_infos:
                    events.gone.append(dataclasses.replace(info, state=SessionState.GONE))

            for id, info in new_infos.items():
                if id not in curr_infos:
                    events.created.append(info)
                else:
                    diff = curr_infos[id].diff(new_infos[id])
                    if diff.changes:
                        events.changed.append(diff)

            if include_logs:
                for id in set(curr_infos.keys()) | set(new_infos.keys()):
                    events.logs.extend(self.logs(id, info=new_infos.get(id) or curr_infos.get(id)))

            curr_infos = new_infos
            yield events

    def wait(
        self,
        query: SessionQuery,
        state: SessionState,
        retry_policy: RetryPolicy,
        callback: Optional["SessionEventsCallback"] = None,
        stop_event: Optional[threading.Event] = None,
        delete: bool = True,
    ) -> None:

        info = self.find_one(query)
        if info is None:
            if state == SessionState.GONE:
                return
            else:
                raise UnexpectedStateError("no matching session found, expected state %r", state)

        LOGGER.info("waiting for session %r to reach state %r", info.as_json(compact=True), state)

        id_query = SessionQuery(id=info.id)
        stop_event = stop_event or threading.Event()
        it = self.watch(query=id_query, include_logs=callback is not None)

        def _wait_state_or_finished() -> SessionState:
            if stop_event and stop_event.is_set():
                raise OperationCanceledError("operation canceled")

            events = next(it)
            if callback is not None:
                callback(events)

            current_state = self.poll(info.id)

            # nothing else can happen...
            if current_state == SessionState.GONE:
                raise SessionGoneError(info.id)

            # got to the expected state
            if current_state == state:
                LOGGER.info("session %r reached state %r", info.as_json(compact=True), state)
                return current_state

            if current_state in SESSION_STATE_FINISHED:
                if delete:
                    self.delete(info.id)
                if state not in SESSION_STATE_FINISHED:
                    # can't wait for an OK state after the session finishes
                    raise UnexpectedStateError("session finished in state %r, expected state %r", current_state, state)

            LOGGER.info("session %r is still in state %r, waiting...", info.as_json(compact=True), current_state)
            return current_state

        try:
            retry_policy.run(func=_wait_state_or_finished, sentinel=state)
        except SessionGoneError:
            if state == SessionState.GONE:
                LOGGER.info("session is gone: %s", info.as_json(compact=True))
            else:
                raise

    def follow(
        self,
        query: SessionQuery,
        include_logs: bool = False,
        refresh: bool = False,
        callback: Optional["SessionEventsCallback"] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        callback = callback if callback is not None else StreamSessionEventsCallback()
        pause = self.config.http_config.poll_pause
        stop_event = stop_event or threading.Event()

        try:
            for session_events in self.watch(query, include_logs=include_logs, refresh=refresh):
                callback(session_events)
                if stop_event.wait(timeout=pause):
                    LOGGER.info("stop event set, interrupting follow")
                    break
        except KeyboardInterrupt:
            LOGGER.info("keyboard interrupt, interrupting follow")


class SessionEventsCallback(Protocol):
    def __call__(self, events: SessionEvents) -> Optional[bool]:
        """
        Invoked on each new session events.

        Returns:
            - `True` to stop the loop.
        """


@dataclasses.dataclass(frozen=True)
class StreamSessionEventsCallback(SessionEventsCallback):
    stream: TextIO = sys.stderr

    def __call__(self, events: SessionEvents) -> None:
        changed = False

        if events.created:
            changed = True
            LOGGER.info(
                "created %d sessions: %s", len(events.created), [info.as_json(compact=True) for info in events.created]
            )
        if events.gone:
            changed = True
            LOGGER.info("gone %d sessions: %s", len(events.gone), [info.as_json(compact=True) for info in events.gone])

        for diff in events.changed:
            changed = True
            LOGGER.info("changed session %s", diff.as_json())

        for log in events.logs:
            for line in self.format_lines(log):
                changed = True
                print(line, file=self.stream, flush=True)

        if not changed:
            LOGGER.info("no changes")

    @classmethod
    def format_lines(cls, log: SessionLog) -> list[str]:
        lines = log.text.strip().splitlines()
        name = json.dumps(log.name or "<unknown>")
        return [f"> id={log.id} name={name} {line}" for line in lines]
