import time
from unittest.mock import Mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from time_machine import TimeMachineFixture

from livy_uploads.utils.retry_policy import MaxTries, NotReadyError

# mypy: disable-error-code=no-untyped-def


class TestMaxTries:
    @pytest.fixture(autouse=True)
    def set_time(self, time_machine: TimeMachineFixture, monkeypatch: MonkeyPatch):
        monkeypatch.setattr("time.sleep", lambda t: time_machine.shift(t))
        time_machine.move_to(0.0, tick=False)

    def test_apply_working_first_time(self):
        policy = MaxTries(count=3, pause=1.0)
        mock = Mock(return_value=42)

        func = policy.apply(exceptions=(NotReadyError,))(mock)

        assert func() == 42
        assert mock.call_count == 1
        assert time.time() == 0.0

    def test_apply_retry_on_exception(self):
        policy = MaxTries(count=3, pause=1.0)
        mock = Mock(side_effect=[ValueError(), 42])

        func = policy.apply(exceptions=(ValueError,))(mock)

        assert func() == 42
        assert mock.call_count == 2
        assert time.time() == 1.0

    def test_apply_fail_on_exhaustion(self):
        policy = MaxTries(count=3, pause=1.0)
        mock = Mock(side_effect=ValueError())

        func = policy.apply(exceptions=(ValueError,))(mock)

        with pytest.raises(ValueError):
            func()

        assert mock.call_count == 3
        assert time.time() == 2.0

    def test_apply_fail_on_unknown_exception(self):
        policy = MaxTries(count=3, pause=1.0)
        mock = Mock(side_effect=RuntimeError())

        func = policy.apply(exceptions=(ValueError,))(mock)

        with pytest.raises(RuntimeError):
            func()

        assert mock.call_count == 1
        assert time.time() == 0.0

    def test_apply_sentinel_value(self):
        policy = MaxTries(count=3, pause=1.0)
        mock = Mock(side_effect=[None, 3.14, 42])

        func = policy.apply(sentinel=42)(mock)

        assert func() == 42
        assert mock.call_count == 3
        assert time.time() == 2.0
