from datetime import datetime, timedelta
import hashlib
import os
from pathlib import Path
import shlex
import signal
import time
from uuid import uuid4

import pytest
from unittest.mock import Mock


from livy_uploads.exceptions import LivyStatementError
from livy_uploads.session import LivyEndpoint, LivySession
from livy_uploads.retry_policy import LinearRetryPolicy
from livy_uploads.executor.commands import (
    LivyPrepareMaster,
    LivyStartProcess,
)

endpoint = LivyEndpoint('http://localhost:8998')


@pytest.fixture
def livy_session():
    session = LivySession.create(
        endpoint,
        name='test-' + str(uuid4()),
        ttl='60s',
        heartbeatTimeoutInSecond=60,
    )
    session.wait_ready(LinearRetryPolicy(30, 1.0))
    try:
        yield session
    finally:
        session.delete()