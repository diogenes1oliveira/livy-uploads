from abc import ABC, abstractmethod
import itertools
from logging import getLogger, Logger
from threading import Lock
import time
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Set, TypeVar

import requests

from livy_uploads.exceptions import LivyError, LivyRequestError, LivyRetriableError
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.retry_policy import RetryPolicy, WithExceptionsPolicy


LOGGER = getLogger(__name__)
T = TypeVar('T')


class LivySessionEndpoint(LivyEndpoint):
    '''
    A class to query and interact with a specific Livy session.
    '''

    def __init__(
        self,
        url: str,
        session_id: int,
        session_info: Optional[Dict[str, Any]] = None,
        default_headers: Optional[Dict[str, str]] = None,
        verify: bool = True,
        auth=None,
        requests_session: Optional[requests.Session] = None,
        retry_policy: Optional[RetryPolicy] = None,
        logger: Optional[Logger] = None,
    ):
        '''
        Parameters:
        - url: the base URL of the Livy server
        - session_id: the ID of the Spark session to use
        - default_headers: a dictionary of headers to include in every request
        - verify: whether to verify the SSL certificate of the server
        - auth: an optional authentication object to pass to requests
        - requests_session: an optional requests.Session object to use for making requests
        - retries: the number of times to retry a request if it fails
        - pause: the number of seconds to wait between polling for the status of a statement
        '''
        super().__init__(
            url=url,
            default_headers=default_headers,
            verify=verify,
            auth=auth,
            requests_session=requests_session,
            retry_policy=retry_policy,
        )
        self.session_id = session_id
        self.session_info = session_info or {}
        self.session_info['id'] = session_id
        self.lock = Lock()
        self.logger = logger or getLogger(f'session#{session_id}')

    def __repr__(self):
        session_info = {}
        if self.session_info.get('name'):
            session_info['name'] = self.session_info['name']
        if self.session_info.get('state'):
            session_info['state'] = self.session_info['state']

        return f"{self.__class__.__name__}({self.url!r}, session_id={self.session_id}, session_info={(session_info or None)!r})"

    @classmethod
    def list(self, endpoint: LivyEndpoint, page_size: int = 20) -> Iterator['LivySessionEndpoint']:
        '''
        Retrieves a list of all active session endpoints
        '''
        offset = 0

        while True:
            LOGGER.info('fetching sessions from offset %d', offset)
            r = endpoint.get(f'/sessions?from={offset}&size={page_size}')
            body = r.json()
            sessions = body.get('sessions') or []
            offset += len(sessions)

            for session in sessions:
                yield LivySessionEndpoint(
                    url=endpoint.url,
                    session_id=session['id'],
                    session_info=session,
                    default_headers=endpoint.default_headers,
                    verify=endpoint.verify,
                    auth=endpoint.auth,
                    requests_session=endpoint.requests_session,
                )
            if not sessions or len(sessions) < page_size:
                break

    @classmethod
    def create_session(cls, endpoint: 'LivyEndpoint', name: str, doAs: Optional[str] = None, **kwargs) -> 'LivySessionEndpoint':
        '''
        Creates a new session in the Livy server

        Parameters:
        - endpoint: the Livy endpoint to use
        - name: the name of the session
        - doAs: an optional user to impersonate
        - kwargs: additional parameters to post to Livy. Check `POST /sessions` in the Livy API docs
        (https://livy.incubator.apache.org/docs/latest/rest-api.html#session) for more information.
        '''
        path = '/sessions'
        if doAs:
            path += f'?doAs={doAs}'

        LOGGER.info('creating new session named %r', name)
        r = endpoint.post(
            path='/sessions',
            json={**kwargs, 'name': name},
        )
        body = r.json()

        session = LivySessionEndpoint(
            url=endpoint.url,
            session_id=body['id'],
            default_headers=endpoint.default_headers,
            verify=endpoint.verify,
            auth=endpoint.auth,
            requests_session=endpoint.requests_session,
            retry_policy=endpoint.retry_policy,
        )

        time.sleep(1.0)  # just in case, in tests it seems to be kind of flaky
        session.refresh()
        session.logger.info('session created')
        return session

    @property
    def session_state(self) -> str:
        '''
        Returns the state of the session
        '''
        return self.session_info.get('state') or '<unknown>'

    @property
    def session_name(self) -> str:
        '''
        Returns the name of the session
        '''
        return self.session_info.get('name') or '<unknown>'

    def assert_idle(self):
        '''
        Asserts that the session is in the idle state
        '''
        self.refresh_state()

        if self.session_state != 'idle':
            raise LivyError(f'session is not idle: {self.session_state!r}')

    def wait_ready(self, retry_policy: RetryPolicy):
        '''
        Waits until the session is ready
        '''
        retry_policy = WithExceptionsPolicy(retry_policy, LivyError)

        def run():
            state = self.refresh_state()
            if state == 'idle':
                self.logger.info('session is ready')
                return
            if state in ('not_started', 'starting', 'busy'):
                self.logger.info('session is still in state %r, waiting...', state)
                raise LivyError
            if state != 'idle':
                raise RuntimeError(f'bad session state: {state!r}')

        retry_policy.run(run)

    def close(self):
        '''
        Sends the deletion request for the session
        '''
        self.logger.info('stopping session')
        self.delete(f'/sessions/{self.session_id}')
        self.logger.info('session deleted')

    def wait_closed(self, retry_policy: RetryPolicy):
        '''
        Waits until the session is deleted
        '''
        retry_policy = WithExceptionsPolicy(retry_policy, LivyRetriableError)

        def run():
            state = self.refresh_state()
            if state == 'dead':
                self.logger.info('session is deleted')
                return
            elif state in ('killed', 'success', 'error'):
                self.logger.info('session finished in state %r')
                return

            self.logger.info('session is still in state %r, waiting...', state)
            raise LivyRetriableError

        retry_policy.run(run)

    def refresh(self) -> Dict[str, Any]:
        '''
        Refreshes all the information for the session
        '''
        r = self.get(f'/sessions/{self.session_id}')
        body = r.json()
        body.pop('log', None)
        self.session_info = body
        return self.session_info

    def refresh_state(self) -> str:
        '''
        Refreshes the state of the session
        '''

        try:
            r = self.get(f'/sessions/{self.session_id}/state')
            body = r.json()
            state = body['state']
        except LivyRequestError as e:
            if e.response.status_code == 404:
                state = 'dead'
            else:
                raise

        self.session_info['state'] = state
        return state

    def follow(self, page_size: int = 500) -> Iterator[List[str]]:
        '''
        Iterates over the logs of the session.

        Livy rotates the logs returned in the API, so this method uses a very crude heuristic to try to
        detect the shift and deduplicate them.

        Parameters:
        - page_size: the number of log lines to read at once

        Yields:
        - the batch of log lines. If it returns one with zero lines, you probably should add a bigger pause
        before the next iteration to avoid hitting the server too hard.
        '''
        self.refresh_state()
        current_state = self.session_state

        previous_logs_set: Set[str] = set()

        while True:
            self.refresh_state()
            new_state = self.session_state
            stopped = new_state in ('error', 'dead', 'killed', 'success')

            try:
                r = self.get(f'/sessions/{self.session_id}/log?size={page_size}')
                body = r.json()
            except LivyRequestError as e:
                if e.response.status_code == 404:
                    new_state = 'dead'
                    stopped = True
                    logs = []
                else:
                    raise

            current_logs: List[str] = body.get('log') or []
            current_logs_set: Set[str] = set(current_logs)
            logs: List[str] = list(itertools.dropwhile(lambda l: l in previous_logs_set, current_logs))
            previous_logs_set = current_logs_set

            if logs or not stopped:
                yield logs

            if not logs and stopped:
                LOGGER.info('session %d is in state %r, stopping log retrieval', self.session_id, current_state)
                break

    def run(self, command: 'LivyCommand[T]') -> T:
        '''
        Runs a command in the remote Livy session.
        '''
        with self.lock:
            self.assert_idle()
            return command.run(self)


class LivyCommand(ABC, Generic[T]):
    '''
    Base class for Livy commands.
    '''
    @abstractmethod
    def run(self, session: LivySessionEndpoint) -> T:
        '''
        Runs the command in the specified Livy session.
        '''
