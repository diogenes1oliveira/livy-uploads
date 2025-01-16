import logging
from threading import Event, Thread
import textwrap
import time
from uuid import uuid4

import pytest

from livy_uploads.exceptions import LivyRequestError
from livy_uploads.session import LivyEndpoint, LivySessionEndpoint
from livy_uploads.retry_policy import LinearRetryPolicy


logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s:%(funcName)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TestLivySessionEndpoint:
    endpoint = LivyEndpoint('http://localhost:8998')

    def test_lifecycle(self):
        sessions_before = {s.session_id: s for s in LivySessionEndpoint.list(self.endpoint)}

        name = 'test-' + str(uuid4())
        session = LivySessionEndpoint.create_session(self.endpoint, name=name, ttl='60s', heartbeatTimeoutInSecond=60)

        sessions_now = {s.session_id: s for s in LivySessionEndpoint.list(self.endpoint)}
        new_session_ids = list(set(sessions_now) - set(sessions_before))
        assert len(new_session_ids) == 1
        new_session = sessions_now[new_session_ids[0]]

        assert new_session.session_name == name
        assert new_session.session_id == session.session_id

        # WARNING: Livy seems to fail to cleanup if we close the session too quickly
        session.wait_ready(LinearRetryPolicy(30, 1.0))

        with pytest.raises(LivyRequestError) as e:
            LivySessionEndpoint.create_session(self.endpoint, name=name)

        assert 'duplicate' in str(e).lower()

        session.close()
        session.wait_closed(LinearRetryPolicy(10, 1.0))
        assert session.refresh_state() == 'dead'

    def test_ttl(self):
        session = LivySessionEndpoint.create_session(
            self.endpoint,
            name=f'test-{uuid4()}',
            ttl='10s',
            heartbeatTimeoutInSecond=10,
        )

        stopped = Event()
        do_refresh = True
        exc = None

        def run():
            try:
                while do_refresh:
                    session.refresh_state()
                    time.sleep(1.0)
            except Exception as e:
                nonlocal exc
                exc = e
            finally:
                stopped.set()

        Thread(target=run, daemon=True).start()

        session.wait_ready(LinearRetryPolicy(30, 1.0))

        do_refresh = False
        if not stopped.wait(10.0):
            raise RuntimeError('session refresh thread did not stop as expected')

        logging.info('waiting 1m to check session expires')
        time.sleep(60)

        assert session.refresh_state() == 'dead'

    def test_follow(self):
        session = LivySessionEndpoint.create_session(
            self.endpoint,
            name=f'test-{uuid4()}',
            ttl='60s',
            heartbeatTimeoutInSecond=60,
        )
        session.wait_ready(LinearRetryPolicy(60, 1.0))

        logs_iter = session.follow()
        creation_logs = []
        for logs in logs_iter:
            if not logs:
                break
            creation_logs += logs
        assert 'Created Spark session.' in '\n'.join(creation_logs)

        code = textwrap.dedent('''
            from datetime import datetime
            import time
            import traceback

            from pyspark import InheritableThread

            def run():
                println = sc._gateway.jvm.java.lang.System.err.println
                try:
                    while True:
                        now = datetime.now().astimezone().isoformat()
                        println('test_follow: ' + now)
                        time.sleep(1.0)
                except Exception:
                    println(traceback.format_exc())

            thread = InheritableThread(target=run, daemon=True)
            thread.start()
        ''')
        r = session.post(
            f"/sessions/{session.session_id}/statements",
            json={
                'kind': 'pyspark',
                'code': code,
            },
        )

        seen = set(creation_logs)
        time.sleep(5.0)
        logs = next(logs_iter)
        assert len(logs) < 20
        assert all(log not in seen for log in logs)

        seen.update(logs)
        time.sleep(5.0)
        logs = next(logs_iter)
        assert len(logs) < 20
        assert all(log not in seen for log in logs)
