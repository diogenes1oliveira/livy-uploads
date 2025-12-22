import time
from unittest.mock import Mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from time_machine import TimeMachineFixture

from livy_uploads.utils.retry_policy import CustomIntervalRetry, MaxTries, NotReadyError

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


class TestCustomIntervalRetry:
    @pytest.fixture(autouse=True)
    def set_time(self, time_machine: TimeMachineFixture, monkeypatch: MonkeyPatch):
        monkeypatch.setattr("time.sleep", lambda t: time_machine.shift(t))
        time_machine.move_to(0.0, tick=False)

    def test_init_with_empty_list_raises_error(self):
        with pytest.raises(ValueError, match="retry_seconds_to_sleep_list must not be empty"):
            CustomIntervalRetry(retry_seconds_to_sleep_list=(), configurable_retry_policy_max_retries=5)

    def test_init_with_invalid_max_retries_raises_error(self):
        with pytest.raises(ValueError, match="configurable_retry_policy_max_retries must be at least 1"):
            CustomIntervalRetry(retry_seconds_to_sleep_list=(0.2, 0.5), configurable_retry_policy_max_retries=0)

    def test_timeout_property_calculates_total(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=5,
        )

        assert policy.timeout == 0.2 + 0.5 + 1.0 + 0.2 + 0.5

    def test_evaluate_returns_correct_intervals(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=5,
        )

        assert policy.evaluate(1, 0.0) == 0.2
        assert policy.evaluate(2, 0.2) == 0.5
        assert policy.evaluate(3, 0.7) == 1.0

    def test_evaluate_cycles_through_intervals(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=5,
        )

        assert policy.evaluate(4, 1.7) == 0.2

    def test_evaluate_stops_at_max_retries(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=5,
        )

        assert policy.evaluate(5, 1.9) is None
        assert policy.evaluate(6, 2.9) is None

    def test_apply_working_first_time(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=3,
        )
        mock = Mock(return_value=42)

        func = policy.apply(exceptions=(NotReadyError,))(mock)

        assert func() == 42
        assert mock.call_count == 1
        assert time.time() == 0.0

    def test_apply_retry_with_custom_intervals(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1.0),
            configurable_retry_policy_max_retries=5,
        )
        mock = Mock(side_effect=[ValueError(), ValueError(), 42])

        func = policy.apply(exceptions=(ValueError,))(mock)

        assert func() == 42
        assert mock.call_count == 3
        assert time.time() == 0.2 + 0.5

    def test_apply_fail_on_exhaustion(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5),
            configurable_retry_policy_max_retries=3,
        )
        mock = Mock(side_effect=ValueError())

        func = policy.apply(exceptions=(ValueError,))(mock)

        with pytest.raises(ValueError):
            func()

        assert mock.call_count == 3
        assert time.time() == 0.2 + 0.5

    def test_apply_with_sparkmagic_config_values(self):
        policy = CustomIntervalRetry(
            retry_seconds_to_sleep_list=(0.2, 0.5, 1, 3, 5),
            configurable_retry_policy_max_retries=8,
        )
        # 7 failures + 1 success = 8 tries total
        # Pauses after tries 1-7: 0.2, 0.5, 1, 3, 5, 0.2, 0.5
        mock = Mock(side_effect=[ValueError()] * 7 + [42])

        func = policy.apply(exceptions=(ValueError,))(mock)

        assert func() == 42
        assert mock.call_count == 8
        assert time.time() == pytest.approx(0.2 + 0.5 + 1 + 3 + 5 + 0.2 + 0.5)
