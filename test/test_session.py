import logging
import textwrap
import time
from threading import Event, Thread
from uuid import uuid4

import pytest

from livy_uploads.exceptions import LivyRequestError
from livy_uploads.retry_policy import TimeoutRetryPolicy
from livy_uploads.session import LivyEndpoint, LivySession

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s:%(funcName)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

from conftest import LIVY_TEST_SESSION_READINESS_TIMEOUT, LIVY_TEST_SESSION_TTL

readiness_policy = TimeoutRetryPolicy(LIVY_TEST_SESSION_READINESS_TIMEOUT, 1.0)
stop_policy = TimeoutRetryPolicy(10.0, 1.0)


class TestLivySessionEndpoint:
    endpoint = LivyEndpoint("http://localhost:8998")

    def test_lifecycle(self) -> None:
        sessions_before = {s.session_id: s for s in LivySession.list(self.endpoint)}

        name = "test-" + str(uuid4())
        session = LivySession.create(self.endpoint, name=name, ttl=LIVY_TEST_SESSION_TTL, heartbeatTimeoutInSecond=60)

        sessions_now = {s.session_id: s for s in LivySession.list(self.endpoint)}
        new_session_ids = list(set(sessions_now) - set(sessions_before))
        assert len(new_session_ids) == 1
        new_session = sessions_now[new_session_ids[0]]

        assert new_session.session_name == name
        assert new_session.session_id == session.session_id

        # WARNING: Livy seems to fail to cleanup if we close the session too quickly
        session.wait_ready(readiness_policy)

        with pytest.raises(LivyRequestError) as e:
            LivySession.create(self.endpoint, name=name)

        assert "duplicate" in str(e).lower()

        session.delete()
        session.wait_done(stop_policy)
        assert session.refresh_state() == "dead"

    def test_ttl(self) -> None:
        current_ttl = int(LIVY_TEST_SESSION_TTL.split("s")[0])
        test_ttl = f"{current_ttl // 2 + 3}s"
        session = LivySession.create(
            self.endpoint,
            name=f"test-{uuid4()}",
            ttl=test_ttl,
            heartbeatTimeoutInSecond=10,
        )
        try:

            stopped = Event()
            do_refresh = True
            exc = None

            def run() -> None:
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

            session.wait_ready(readiness_policy)

            do_refresh = False
            if not stopped.wait(10.0):
                raise RuntimeError("session refresh thread did not stop as expected")

            logging.info("waiting 1m30s to check session expires")
            time.sleep(90)

            assert session.refresh_state() == "dead"
        finally:
            try:
                session.delete()
            except Exception as e:
                logging.exception("error deleting session: %s", e)

    def test_follow(self) -> None:
        session = LivySession.create(
            self.endpoint,
            name=f"test-{uuid4()}",
            ttl=LIVY_TEST_SESSION_TTL,
            heartbeatTimeoutInSecond=60,
        )
        session.wait_ready(readiness_policy)
        try:

            logs_iter = session.follow()
            creation_logs = []
            for logs in logs_iter:
                if not logs:
                    break
                creation_logs += logs
            assert "\t final status: UNDEFINED" in "\n".join(creation_logs)

            # TODO: YARN logs don't work like this
            # code = textwrap.dedent(
            #     """
            #     from datetime import datetime
            #     import time
            #     import traceback

            #     from pyspark import InheritableThread

            #     def run():
            #         println = sc._gateway.jvm.java.lang.System.err.println
            #         try:
            #             while True:
            #                 now = datetime.now().astimezone().isoformat()
            #                 println('test_follow: ' + now)
            #                 time.sleep(1.0)
            #         except Exception:
            #             println(traceback.format_exc())

            #     thread = InheritableThread(target=run, daemon=True)
            #     thread.start()
            # """
            # )
            # r = session.request(
            #     "POST",
            #     f"/sessions/{session.session_id}/statements",
            #     json={
            #         "kind": "pyspark",
            #         "code": code,
            #     },
            # )

            # seen = set(creation_logs)
            # time.sleep(5.0)
            # logs = next(logs_iter)
            # assert len(logs) < 30
            # assert all(log not in seen for log in logs if log.strip() not in ["stderr:", "stdout:"])

            # seen.update(logs)
            # time.sleep(5.0)
            # logs = next(logs_iter)
            # assert len(logs) < 30
            # assert all(log not in seen for log in logs if log.strip() not in ["stderr:", "stdout:"])
        finally:
            try:
                session.delete()
            except Exception as e:
                logging.exception("error deleting session: %s", e)
