#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Data models for cluster operations.
'''

__all__ = ('WorkerInfo', 'PollResult', 'HttpResponse')


from typing import NamedTuple, Optional, Mapping, TypeVar

from livy_uploads.executor.cluster.utils import assert_type


T = TypeVar('T')


class WorkerInfo(NamedTuple):
    """
    Worker process information.
    """
    name: str
    "Worker main process name"
    pid: int
    "OS PID of the worker process"
    url: str
    "Advertised URL of the worker server"

    @classmethod
    def fromdict(cls, kwargs: Mapping) -> 'WorkerInfo':
        return cls(
            name=assert_type(kwargs['name'], str),
            pid=assert_type(kwargs['pid'], int),
            url=assert_type(kwargs['url'], str),
        )

    def asdict(self) -> dict:
        return dict(self._asdict())


class PollResult(NamedTuple):
    """
    Result of a worker process poll request.
    """
    stdout: bytes
    "Stdout content"
    returncode: Optional[int]
    "Return code of the worker process"


class HttpResponse(NamedTuple):
    """
    Result of an HTTP request.
    """
    status: int
    "HTTP status code"
    data: Optional[bytes]
    "Response body"

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300
