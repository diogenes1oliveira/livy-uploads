#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Utility functions for the executor cluster.
'''

__all__ = ('assert_type',)

import inspect
from typing import Any, Tuple, Union, TypeVar, Type, overload


T = TypeVar('T')


def assert_type(value: Any, expected_type: Type[T]) -> T:
    """
    Type assertion utility function.
    """
    try:
        origin = getattr(expected_type, '__origin__')
        if origin is Union:
            args = expected_type.__args__
            if len(args) == 2 and args[1] is type(None):
                nullable = True
                expected_type = args[0]
    except AttributeError:
        nullable = False

    if nullable and value is None:
        return value

    if not isinstance(value, expected_type):
        raise ValueError(f'Expected {expected_type}, got {type(value)}')

    return value


def parse_type(type: Type[T]) -> Tuple[Type[T], bool]:
    """
    Parses the type and returns the type and whether it is nullable.
    """
    origin = getattr(type, '__origin__', None)
    cls = type
    nullable = False

    if origin is Union:
        args = type.__args__
        if len(args) == 2 and args[1] is type(None):
            cls = args[0]
            nullable = True

    if not inspect.isclass(cls):
        raise TypeError(f'Not a type object: {type}')

    return type, nullable
