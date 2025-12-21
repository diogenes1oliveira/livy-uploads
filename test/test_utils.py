from typing import Optional

import pytest

from livy_uploads.utils import assert_type

# mypy: disable-error-code="no-untyped-def"


@pytest.mark.parametrize(
    ["value", "expected_type"],
    [
        (1, int),
        (1.0, float),
        ("foo", str),
        (True, bool),
        (None, Optional[int]),
        (None, Optional[float]),
        (None, Optional[str]),
    ],
)
def test_assert_type_good(value, expected_type):
    assert assert_type(value, expected_type) == value


@pytest.mark.parametrize(
    ["value", "expected_type"],
    [
        (1, str),
        (1.0, str),
        (True, str),
        (None, str),
        (1, Optional[str]),
        (1.0, Optional[str]),
        (True, Optional[str]),
        (None, int),
    ],
)
def test_assert_type_bad(value, expected_type):
    with pytest.raises(ValueError):
        assert_type(value, expected_type)
