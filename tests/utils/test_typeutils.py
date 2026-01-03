from abc import ABC
from typing import Optional, Protocol

import pytest

from livy_uploads.utils.typeutils import assert_type, is_concrete

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


@pytest.mark.parametrize(
    ["obj", "t", "expected"],
    [
        # Concrete classes without parent check
        (type("ConcreteClass", (), {}), None, True),
        (type("AnotherConcrete", (), {"x": 1}), None, True),
        # ABC and Protocol themselves are not concrete
        (ABC, None, False),
        (Protocol, None, False),
        # Abstract class with __abstractmethods__ (must be non-empty frozenset)
        (
            type(
                "AbstractClass",
                (ABC,),
                {"__abstractmethods__": frozenset(["method"])},
            ),
            None,
            False,
        ),
        # Class inheriting from ABC
        (type("ABCClass", (ABC,), {}), None, False),
        # Class inheriting from Protocol
        (type("ProtocolClass", (Protocol,), {}), None, False),  # type: ignore
        # Non-class objects
        (42, None, False),
        ("string", None, False),
        ([], None, False),
        ({}, None, False),
        # Built-in concrete classes
        (int, None, True),
        (str, None, True),
        (list, None, True),
        (dict, None, True),
        # With parent type checking
        (type("Child", (int,), {}), int, True),
        (type("Child", (str,), {}), int, False),
        (int, object, True),
        (str, int, False),
    ],
)
def test_is_concrete(obj, t, expected):
    assert is_concrete(obj, t) == expected
