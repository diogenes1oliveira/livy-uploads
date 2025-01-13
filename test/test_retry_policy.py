import pytest
from unittest.mock import Mock, patch

from livy_uploads.retry_policy import DontRetryPolicy, LinearRetryPolicy, WithExceptionsPolicy


class TestWithExceptionsLinearRetryPolicy:
    base = LinearRetryPolicy(3, 0.01)
    policy = WithExceptionsPolicy(base, ValueError)

    def test_wrapping_behavior(self):
        wrapped = WithExceptionsPolicy(self.policy, IndexError)
        assert wrapped.base is self.policy.base

    def test_no_errors(self):
        f = Mock()
        self.policy.run(f)
        assert f.call_count == 1

    @patch('time.sleep')
    def test_unrecognized_error(self, patched_time_sleep: Mock):
        f = Mock()
        f.side_effect = [ValueError(), IndexError(), 42]

        with pytest.raises(IndexError):
            self.policy.run(f)

        assert f.call_count == 2
        patched_time_sleep.assert_called_once_with(0.01)

    @patch('time.sleep')
    def test_recognized_error(self, patched_time_sleep: Mock):
        f = Mock()
        f.side_effect = [ValueError(), ValueError(), 42]

        assert self.policy.run(f) == 42
        assert f.call_count == 3
        assert patched_time_sleep.call_count == 2

    @patch('time.sleep')
    def test_exhaustion(self, patched_time_sleep: Mock):
        f = Mock()
        f.side_effect = [ValueError()] * 3

        with pytest.raises(ValueError):
            self.policy.run(f)

        assert f.call_count == 3
        assert patched_time_sleep.call_count == 2



class TestLinearRetryPolicy:
    policy = LinearRetryPolicy(2, 0.01)

    def test_no_errors(self):
        f = Mock()
        self.policy.run(f)
        assert f.call_count == 1

    @patch('time.sleep')
    def test_one_error(self, patched_time_sleep: Mock):
        f = Mock()
        f.side_effect = [IndexError(), 42]

        assert self.policy.run(f) == 42
        assert f.call_count == 2
        patched_time_sleep.assert_called_once_with(0.01)

    @patch('time.sleep')
    def test_exhaustion(self, patched_time_sleep: Mock):
        f = Mock()
        f.side_effect = [ValueError()] * 2

        with pytest.raises(ValueError):
            self.policy.run(f)

        assert f.call_count == 2
        assert patched_time_sleep.call_count == 1


class TestDontRetryPolicy:
    def test_no_errors(self):
        retry_policy = DontRetryPolicy()
        f = Mock()
        retry_policy.run(f)
        assert f.call_count == 1

    @patch('time.sleep')
    def test_error(self, patched_time_sleep: Mock):
        retry_policy = DontRetryPolicy()
        f = Mock()
        f.side_effect = [Exception(), 42]

        with pytest.raises(Exception):
            retry_policy.run(f)

        assert f.call_count == 1
        assert patched_time_sleep.call_count == 0
