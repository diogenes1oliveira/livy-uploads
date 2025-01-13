import builtins
from typing import Any, List, Optional

from requests import Response


class LivyError(Exception):
    '''
    Base class
    '''


class LivyStatementError(LivyError):
    '''
    Error occurred during the execution of a Livy statement
    '''
    def __init__(self, ename: str, evalue: str, traceback: List[str]):
        super().__init__(f'error in Livy statement execution: {ename}({evalue}):\n{"".join(traceback)}')
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

    def as_builtin(self) -> Optional[BaseException]:
        '''
        Returns a built-in exception instance if the error type is a built-in
        '''
        exc_type = getattr(builtins, self.ename, None)
        if exc_type and issubclass(exc_type, BaseException):
            return exc_type(self.evalue)


class LivySessionDeadError(LivyError):
    '''
    Error occurred because the Livy session is dead
    '''
    def __init__(self, session_name: str):
        super().__init__(f'session {session_name!r} is no longer running')


class LivySessionBusyError(LivyError):
    '''
    Error occurred because the Livy session is dead
    '''
    def __init__(self, session_name: str):
        super().__init__(f'session {session_name!r} is no longer running')


class LivyRequestError(LivyError):
    '''
    Error occurred during the execution of a Livy request
    '''
    def __init__(self, response: Response, body: Any):
        message = f'unexpected response status={response.status_code}: {body!r}'
        super().__init__(message)

        self.response = response
        self.body = body


class LivyRetriableError(LivyError):
    '''
    Retriable error occurred during the execution of a Livy request
    '''
    pass
