__all__ = (
    "TaskInfo",
    "JobSummary",
    "JobInfo",
    "AttemptInfo",
)

import dataclasses
from typing import Optional


@dataclasses.dataclass(frozen=True)
class TaskInfo:
    taskId: int
    index: int
    attempt: int
    host: str
    status: str
    executorId: str
    executorLogs: Optional[dict[str, str]] = None
    jobId: Optional[int] = None
    jobName: Optional[str] = None
    jobGroup: Optional[str] = None
    workerId: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class JobSummary:
    jobGroup: str
    jobId: int
    status: str
    numActiveTasks: int
    numActiveStages: int
    numCompletedStages: int
    numSkippedStages: int
    numFailedStages: int
    killedTasksSummary: dict
    numCompletedIndices: int
    numKilledTasks: int
    numFailedTasks: int
    numSkippedTasks: int
    numCompletedTasks: int
    numTasks: int


@dataclasses.dataclass(frozen=True)
class JobInfo:
    jobId: int
    name: str
    description: str
    stageIds: list[int]
    submissionTime: str
    numTasks: int
    numActiveTasks: int
    numCompletedTasks: int
    numSkippedTasks: int
    numFailedTasks: int
    numKilledTasks: int
    numCompletedIndices: int
    numActiveStages: int
    numCompletedStages: int
    numSkippedStages: int
    numFailedStages: int


@dataclasses.dataclass(frozen=True)
class AttemptInfo:
    status: str
    stageId: int
    attemptId: int
    numTasks: int
    numActiveTasks: int
    numCompleteTasks: int
    tasks: Optional[dict[str, TaskInfo]] = None
