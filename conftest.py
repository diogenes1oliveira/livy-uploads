import logging
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LIVY_TEST_SESSION_TTL = os.environ.get("LIVY_TEST_SESSION_TTL") or "120s"
LIVY_TEST_SESSION_READINESS_TIMEOUT = float(os.environ.get("LIVY_TEST_SESSION_READINESS_TIMEOUT") or "60")
