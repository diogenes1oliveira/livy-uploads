from typing import TYPE_CHECKING, Any, Iterable, Optional

from livy_uploads.commands.base import SessionCommand

if TYPE_CHECKING:
    from livy_uploads.project import Project
else:
    Project = Any


def get_commands(project: Optional[Project] = None) -> Iterable[type[SessionCommand]]:
    from livy_uploads.project import Project

    project = project or Project.get()
    return project.load_plugins(SessionCommand, "commands")


def get_command(name: str, project: Optional[Project] = None) -> type[SessionCommand]:
    commands = get_commands(project=project)
    for command in commands:
        if command.__command__ == name:
            return command
    raise ValueError(f"no such command: {name!r}")
