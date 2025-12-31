import dataclasses
import logging
import os
import re
from json import JSONDecodeError
from typing import Optional, TypeVar

import requests
from bs4 import BeautifulSoup

from livy_uploads.models.spark import AttemptInfo, JobInfo, JobSummary, TaskInfo

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")

_n_bytes_pattern = re.compile(r"Showing (\d+) bytes")
_n_invalid_pattern = re.compile(r"Invalid start and end values.\s+Start:\s+\[(\d+)\],?\s+end\[(?P<size>\d+)\]")
_dummy_max_offset = int(os.environ.get("SPARKRL_HADOOP_DUMMY_MAX_OFFSET") or "88888888888")
_initial_approx_line_size = int(os.environ.get("SPARKRL_HADOOP_INITIAL_APPROX_LINE_SIZE") or "160")


def get_yarn_logs(
    url: str,
    n: int,
    offset: Optional[int] = 0,
    session: Optional[requests.Session] = None,
    approx_line_size: Optional[int] = None,
) -> tuple[list[str], int]:
    """
    Gets the logs from a YARN log URL.

    Args:
        url: The URL to get the logs from.
        n: The number of lines to get.
        offset: The bytes offset to start from.
        session: A custom requests session to use.
        approx_line_size: The approximate size of a line, in bytes.

    Returns:
        A tuple of the log lines and the offset.
    """
    approx_line_size = approx_line_size or _initial_approx_line_size
    session = session or requests.Session()
    offset = offset or 0
    log_lines: list[str] = []

    if offset < 0:
        # get the real offset for negative offsets
        _, max_size, _ = _get_log_data(url, _dummy_max_offset, None, session)
        offset += max_size

    while True:
        if len(log_lines) >= n:
            break

        start = offset
        delta = max(n - len(log_lines), 3)  # just not to get only one
        end = start + approx_line_size * delta
        lines, size, eof = _get_log_data(url, start, end, session)
        if eof:
            offset = size
            break
        if not lines:
            break
        for line in lines:
            log_lines.append(line)
            offset += len(line.encode("utf8")) + 1
            if len(log_lines) == n:
                break

    return log_lines, offset


def _get_log_data(
    url: str, start: Optional[int], end: Optional[int], session: requests.Session
) -> tuple[list[str], int, bool]:
    url, _, _ = url.partition("?")
    response = session.get(url, params={"start": start, "end": end})
    response.raise_for_status()
    print(f"fetched from url={response.request.url!r}")
    return _parse_log_data(response.text)


def get_task_infos(
    ui_url: str,
    app_id: str,
    session: Optional[requests.Session] = None,
) -> list[TaskInfo]:
    """
    Gets the task infos from a Spark UI URL.

    Args:
        ui_url: The URL of the Spark UI.
        app_id: The ID of the Spark application.
        session: A custom requests session to use.

    Returns:
        A list of task infos.
    """
    session = session or requests.Session()
    base_url = ui_url.rstrip("/") + f"/api/v1/applications/{app_id}"
    task_infos = []

    job_summaries = _get_json(session, f"{base_url}/jobs", list[JobSummary])
    for job_summary in job_summaries:
        job_info = _get_json(session, f"{base_url}/jobs/{job_summary.jobId}", JobInfo)
        for stage_id in job_info.stageIds:
            attempts = _get_json(session, f"{base_url}/stages/{stage_id}", list[AttemptInfo])
            for attempt in attempts:
                for task_info in (attempt.tasks or {}).values():
                    task_info = dataclasses.replace(
                        task_info,
                        jobId=job_info.jobId,
                        jobName=job_info.name,
                        jobGroup=job_summary.jobGroup,
                    )
                    task_infos.append(task_info)
    return task_infos


def _parse_log_data(html: str) -> tuple[list[str], int, bool]:
    soup = BeautifulSoup(html, "html.parser")
    td = soup.find("td", class_="content")
    if not td:
        LOGGER.warning("no content element: %s", html)
        raise IOError("No content element found in response")

    invalid_msg = td.find("h1", string=_n_invalid_pattern)  # type: ignore
    pre_element = td.find("pre")

    if invalid_msg:
        max_size = int(_n_invalid_pattern.search(invalid_msg.text).group("size"))  # type: ignore
        LOGGER.warning("invalid offset, total size is %d", max_size)
        return [], max_size, True
    elif not pre_element:
        LOGGER.warning("no <pre> element: %s", html)
        raise IOError("no <pre> element found in response")

    text: str = pre_element.text
    size = len(text.encode("utf8"))
    lines = text.splitlines()
    if lines and not lines[-1].endswith("\n"):
        last = lines.pop()
        size -= len(last.encode("utf8"))

    return lines, size, False


def _get_json(session: requests.Session, url: str, type: type[T]) -> T:
    import cattrs

    response = session.get(url)
    response.raise_for_status()

    try:
        body = response.json()
    except JSONDecodeError as e:
        text = response.text
        raise ValueError(f"failed to parse JSON from {url} - {text[:100]}...: {e}") from None

    if "/jobs/" in url:
        breakpoint()

    try:
        return cattrs.structure(body, type)
    except cattrs.errors.BaseValidationError as e:
        raise ValueError(f"failed to parse JSON types: {e}") from e
