__all__ = (
    "RetryPolicy",
    "RetryPolicyWithTimeout",
    "NoRetry",
    "MaxTries",
    "MaxTime",
    "CustomIntervalRetry",
    "RetriableError",
    "NotReadyError",
)

import dataclasses
import functools
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, ClassVar, Iterable, Mapping, Optional, Type, TypeVar, Union, cast

from typing_extensions import Self

from livy_uploads.utils.typeutils import as_type

T = TypeVar("T")
C = TypeVar("C", bound=Callable)

LOGGER = logging.getLogger(__name__)


MISSING: Any = object()


class RetryPolicy(ABC):
    """
    A class to abstract the retry logic.
    """

    @abstractmethod
    def evaluate(self, i: int, dt: float) -> Optional[float]:
        """
        Checks if the exception should be retried.

        Args:
            i: The current try count.
            dt: The time since the first try.

        Returns:
            The time to wait before the next try, or None if the exception should not be retried.
        """
        raise NotImplementedError

    def apply(
        self,
        exceptions: Optional[tuple[Type[Exception], ...]] = None,
        check: Optional[Callable[[Exception], bool]] = None,
        sentinel: Optional[Union[Any, Callable[[Any], bool]]] = MISSING,
    ) -> Callable[[C], C]:
        """
        Decorator that wraps the given callable with :meth:`run` to automatically retry on the policy.
        """
        _check_conditions(check=check, exceptions=exceptions, sentinel=sentinel)

        def wrapper(func: C) -> C:
            @functools.wraps(func)
            def inner(*args: Any, **kwargs: Any) -> Any:
                return self.run(
                    func=func,
                    args=args,
                    kwargs=kwargs,
                    check=check,
                    exceptions=exceptions,
                    sentinel=sentinel,
                )

            return cast(C, inner)

        return wrapper

    def run(
        self,
        func: Callable[..., T],
        check: Optional[Callable[[Exception], bool]] = None,
        exceptions: Optional[tuple[Type[Exception], ...]] = None,
        args: Optional[Iterable[Any]] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
        sentinel: Optional[Union[T, Callable[[T], bool]]] = MISSING,
    ) -> T:
        """
        Runs the given function and retries if it fails, accordingly to the specific policy.

        Args:
            func: The function to run.
            check: The function to check if the exception should be retried. Can't be used together with `exceptions`.
            exceptions: The exceptions to check if they should be retried. Defaults to `RetriableError`.
            args: Positional arguments to pass to the function.
            kwargs: Keyword arguments to pass to the function.

        Returns:
            The result of the first successful try.
        """
        _check_conditions(check=check, exceptions=exceptions, sentinel=sentinel)
        if sentinel is not MISSING:
            check_func = lambda e: isinstance(e, NotReadyError)
        elif exceptions is not None:
            check_func = lambda e: isinstance(e, exceptions)
        else:
            assert check is not None
            check_func = check

        t0 = time.monotonic()
        i = 1
        args = args or []
        kwargs = kwargs or {}

        while True:
            try:
                value = func(*args, **kwargs)
                if sentinel is MISSING:
                    return value
                elif not callable(sentinel):
                    if value == sentinel:
                        return value
                elif sentinel(value):
                    return value
                raise NotReadyError(f"sentinel condition not met in result {value!r}")
            except Exception as e:
                if not check_func(e):
                    LOGGER.debug("exception is not retriable: %r", e)
                    raise

                dt = time.monotonic() - t0
                next_pause = self.evaluate(i, dt)
                if next_pause is None:
                    LOGGER.debug("no more retries available: %r", e)
                    raise
                i += 1
                time.sleep(next_pause)

    @classmethod
    def parse(cls, body: Any) -> Optional["RetryPolicy"]:
        if body is None:
            return None

        kwargs = as_type(body, dict)
        type = as_type(kwargs.get("type"), str)

        if type == "no-retry":
            return NoRetry()
        elif type == "max-tries":
            return MaxTries(
                count=as_type(kwargs.get("count"), int),
                pause=as_type(kwargs.get("pause"), float),
            )
        elif type == "max-time":
            return MaxTime(time=as_type(kwargs.get("time"), float), pause=as_type(kwargs.get("pause"), float))
        elif type == "custom-interval":
            return CustomIntervalRetry(
                retry_seconds_to_sleep_list=tuple(as_type(kwargs.get("retry_seconds_to_sleep_list"), list)),
                configurable_retry_policy_max_retries=as_type(kwargs.get("configurable_retry_policy_max_retries"), int),
            )
        else:
            raise ValueError(f"unknown retry policy type: {type}")

    @abstractmethod
    def as_json(self) -> dict[str, Any]:
        return {
            "type": self.__configurable_typename__,  # type: ignore[attr-defined]
        }


class RetryPolicyWithTimeout(RetryPolicy):
    """
    A retry policy that can inform a maximum possible timeout.
    """

    @property
    @abstractmethod
    def timeout(self) -> float:
        raise NotImplementedError


def _check_conditions(
    check: Optional[Callable[[Exception], bool]] = None,
    exceptions: Optional[tuple[Type[Exception], ...]] = None,
    sentinel: Optional[Union[Any, Callable[[Any], bool]]] = MISSING,
) -> None:
    condition_count = [check is not None, exceptions is not None, sentinel is not MISSING]
    if sum(condition_count) != 1:
        raise ValueError("Exactly one of check, exceptions, or sentinel must be provided")


@dataclasses.dataclass(frozen=True)
class NoRetry(RetryPolicy):
    """
    A retry policy that does not retry.
    """

    __configurable_typename__: ClassVar[str] = "no-retry"

    def evaluate(self, i: int, dt: float) -> Optional[float]:
        return None

    def as_json(self) -> dict[str, Any]:
        return super().as_json()


@dataclasses.dataclass(frozen=True)
class MaxTries(RetryPolicyWithTimeout):
    """
    A retry policy that retries a given number of times.
    """

    __configurable_typename__: ClassVar[str] = "max-tries"

    count: int
    pause: float

    @property
    def timeout(self) -> float:
        return self.count * self.pause

    def evaluate(self, i: int, dt: float) -> Optional[float]:
        if i >= self.count:
            return None
        return self.pause

    def as_json(self) -> dict[str, Any]:
        return super().as_json() | {
            "count": self.count,
            "pause": self.pause,
        }


@dataclasses.dataclass(frozen=True)
class MaxTime(RetryPolicyWithTimeout):
    """
    A retry policy that retries for a given duration.
    """

    __configurable_typename__: ClassVar[str] = "max-time"

    time: float
    pause: float

    def evaluate(self, i: int, dt: float) -> Optional[float]:
        if dt >= self.time:
            return None
        return self.pause

    @property
    def timeout(self) -> float:
        return self.time

    def as_json(self) -> dict[str, Any]:
        return super().as_json() | {
            "time": self.time,
            "pause": self.pause,
        }


@dataclasses.dataclass(frozen=True)
class PeriodicRetry(RetryPolicy):
    """
    A retry policy that uses a fixed interval.
    """

    interval: float

    def evaluate(self, i: int, dt: float) -> float:
        return self.interval

    def as_json(self) -> dict[str, Any]:
        return super().as_json() | {
            "interval": self.interval,
        }


@dataclasses.dataclass(frozen=True)
class CustomIntervalRetry(RetryPolicyWithTimeout):
    """
    A retry policy that uses a custom list of intervals and a maximum retry count.

    This policy cycles through the provided retry intervals and stops after reaching
    the maximum number of retries.
    """

    __configurable_typename__: ClassVar[str] = "custom-interval"

    retry_seconds_to_sleep_list: tuple[float, ...]
    configurable_retry_policy_max_retries: int

    def __post_init__(self) -> None:
        if not self.retry_seconds_to_sleep_list:
            raise ValueError("retry_seconds_to_sleep_list must not be empty")
        if self.configurable_retry_policy_max_retries < 1:
            raise ValueError("configurable_retry_policy_max_retries must be at least 1")

    @property
    def timeout(self) -> float:
        """Calculate the maximum timeout based on all intervals used."""
        total = 0.0
        for idx in range(self.configurable_retry_policy_max_retries):
            interval_idx = idx % len(self.retry_seconds_to_sleep_list)
            total += self.retry_seconds_to_sleep_list[interval_idx]
        return total

    def evaluate(self, i: int, dt: float) -> Optional[float]:
        """
        Evaluate whether to retry and return the sleep interval.

        Args:
            i: The current try count (1-indexed).
            dt: The time elapsed since the first try.

        Returns:
            The sleep interval in seconds, or None if max retries exceeded.
        """
        if i >= self.configurable_retry_policy_max_retries:
            return None

        # Cycle through the intervals list
        interval_idx = (i - 1) % len(self.retry_seconds_to_sleep_list)
        return self.retry_seconds_to_sleep_list[interval_idx]

    def as_json(self) -> dict[str, Any]:
        return super().as_json() | {
            "retry_seconds_to_sleep_list": list(self.retry_seconds_to_sleep_list),
            "configurable_retry_policy_max_retries": self.configurable_retry_policy_max_retries,
        }


class RetriableError(Exception):
    pass


class NotReadyError(Exception):
    pass
