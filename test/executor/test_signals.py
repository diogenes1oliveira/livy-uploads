import signal

import pytest

from livy_uploads.executor.signals import parse_signal


@pytest.mark.parametrize('s, expected', [
    ('SIGINT', signal.SIGINT),
    ('TERM', signal.SIGTERM),
    (9, signal.SIGKILL),
    ('9', signal.SIGKILL),
    ('424242', 424242),
    (42, 42),
])
def test_parse_good_signals(s, expected):
    assert parse_signal(s) == expected


@pytest.mark.parametrize('s', [
    'INTX',
    'SIGIDONTKNOW',
    'INVALID INT',
])
def test_parse_bad_signals(s):
    with pytest.raises(ValueError):
        parse_signal(s)
