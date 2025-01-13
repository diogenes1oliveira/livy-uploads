from abc import ABC, abstractmethod
import time
from typing import Callable, TypeVar

T = TypeVar('T')
E = TypeVar('E', bound=Exception)


class RetryPolicy(ABC):
    '''
    An abstract class for defining retry policies.
    '''

    def __new__(cls, *args, **kwargs):
        '''
        The __new__ method stores the arguments passed to the constructor so that the policy can be cloned later.
        '''
        instance = super().__new__(cls)
        instance._init_args = args
        instance._init_kwargs = kwargs
        return instance

    def clone(self) -> 'RetryPolicy':
        '''
        Clones this retry policy with its state reset.

        By default, this will recreate the policy with the same arguments passed to __init__.
        Reimplement this method if your class needs a custom reset behavior (mainly, if it receives a mutable argument)
        '''
        return self.__class__(*self._init_args, **self._init_kwargs)

    @abstractmethod
    def should_retry(self, exception: Exception) -> bool:
        '''
        Returns True if the exception should be retried.
        '''

    @abstractmethod
    def next_delay(self) -> float:
        '''
        Returns the delay before the next retry.
        '''

    def delay(self, seconds: float):
        '''
        Sleeps for the specified number of seconds.
        '''
        time.sleep(seconds)

    def run(self, func: Callable[..., T], *args, **kwargs) -> T:
        '''
        Runs the function with the specified retry policy.
        '''
        self = self.clone()

        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not self.should_retry(e):
                    raise
                self.delay(self.next_delay())


class DontRetryPolicy(RetryPolicy):
    '''
    A retry policy that never retries.
    '''

    def should_retry(self, e: Exception) -> bool:
        return False

    def next_delay(self) -> float:
        return 0


class LinearRetryPolicy(RetryPolicy):
    '''
    A retry policy that retries a fixed number of times with a linear delay.
    '''

    def __init__(self, max_tries: int, pause: float):
        self.max_tries = max_tries
        self.pause = pause
        self._current_try = 1

    def should_retry(self, e: Exception) -> bool:
        return self._current_try < self.max_tries

    def next_delay(self) -> float:
        self._current_try += 1
        return self.pause


class WithExceptionsPolicy(RetryPolicy):
    '''
    A derived retry policy that only retries on a specific set of exceptions.
    '''

    def __init__(self, base: RetryPolicy, *exceptions: E):
        if isinstance(base, WithExceptionsPolicy):
            base = base.base

        self.base = base
        self.exceptions = exceptions

    def clone(self) -> 'WithExceptionsPolicy':
        return self.__class__(self.base.clone(), *self.exceptions)

    def should_retry(self, e: Exception) -> bool:
        if not isinstance(e, self.exceptions):
            return False
        return self.base.should_retry(e)

    def next_delay(self) -> float:
        return self.base.next_delay()
