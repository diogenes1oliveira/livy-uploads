import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping, Optional, TypeVar

from typing_extensions import Self

from livy_uploads.utils import assert_type

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


class RetryPolicy(ABC):
    """
    An abstract class for defining retry policies.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """
        The __new__ method stores the arguments passed to the constructor so that the policy can be cloned later.
        """
        instance = super().__new__(cls)
        instance._init_args = args  # type: ignore
        instance._init_kwargs = kwargs  # type: ignore
        return instance

    def clone(self) -> "RetryPolicy":
        """
        Clones this retry policy with its state reset.

        By default, this will recreate the policy with the same arguments passed to __init__.
        Reimplement this method if your class needs a custom reset behavior (mainly, if it receives a mutable argument)
        """
        return self.__class__(*self._init_args, **self._init_kwargs)  # type: ignore

    @abstractmethod
    def should_retry(self, exception: Exception) -> bool:
        """
        Returns True if the exception should be retried.
        """

    @abstractmethod
    def next_delay(self) -> float:
        """
        Returns the delay before the next retry.
        """

    def delay(self, seconds: float) -> None:
        """
        Sleeps for the specified number of seconds.
        """
        time.sleep(seconds)

    def run(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Runs the function with the specified retry policy.
        """
        self = self.clone()

        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not self.should_retry(e):
                    raise
                self.delay(self.next_delay())

    @classmethod
    def from_config(cls, config: Optional[Mapping[str, Any]]) -> "RetryPolicy":
        if not config:
            return DontRetryPolicy()
        return LinearRetryPolicy(
            max_tries=assert_type(config["max_tries"], int),
            pause=assert_type(config["pause"], float),
        )


class DontRetryPolicy(RetryPolicy):
    """
    A retry policy that never retries.
    """

    def should_retry(self, e: Exception) -> bool:
        return False

    def next_delay(self) -> float:
        return 0


class LinearRetryPolicy(RetryPolicy):
    """
    A retry policy that retries a fixed number of times with a linear delay.
    """

    def __init__(self, max_tries: int, pause: float):
        self.max_tries = max_tries
        self.pause = pause
        self._current_try = 1

    def should_retry(self, e: Exception) -> bool:
        return self._current_try < self.max_tries

    def next_delay(self) -> float:
        self._current_try += 1
        return self.pause


class TimeoutRetryPolicy(RetryPolicy):
    """
    A retry policy that retries until a timeout is reached.
    """

    def __init__(self, timeout: float, pause: float):
        self.timeout = timeout
        self.pause = pause
        self._start_time = time.monotonic()

    def should_retry(self, e: Exception) -> bool:
        return time.monotonic() - self._start_time < self.timeout

    def next_delay(self) -> float:
        return self.pause


class WithExceptionsPolicy(RetryPolicy):
    """
    A derived retry policy that only retries on a specific set of exceptions.
    """

    def __init__(self, base: RetryPolicy, *exceptions: E):
        if isinstance(base, WithExceptionsPolicy):
            base = base.base  # type: ignore

        self.base = base
        self.exceptions = exceptions

    def clone(self) -> "WithExceptionsPolicy":
        return self.__class__(self.base.clone(), *self.exceptions)

    def should_retry(self, e: Exception) -> bool:
        if not isinstance(e, self.exceptions):  # type: ignore
            return False
        return self.base.should_retry(e)

    def next_delay(self) -> float:
        return self.base.next_delay()
