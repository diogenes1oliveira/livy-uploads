from uuid import uuid4

import pytest


from livy_uploads.executor.cluster import PollResult
from livy_uploads.executor.client import LivyExecutorClient
from livy_uploads.session import LivyEndpoint, LivySession
from livy_uploads.retry_policy import LinearRetryPolicy


endpoint = LivyEndpoint('http://localhost:8998')


class TestLivyExecutorClient:
    def test_happy_path(self, livy_session: LivySession):
        executor = LivyExecutorClient(
            session=livy_session,
            proxy='http://localhost:8090',
        )
        executor.setup()

        client = executor.start(
            command='echo',
            args=['Hello World!'],
        )
        assert client.poll() == PollResult(stdout=b'Hello World!\n', returncode=None)
        assert client.poll() == PollResult(stdout=b'', returncode=0)


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
