import grp
import logging
import os
import pwd
from collections.abc import Mapping
from getpass import getuser
from pathlib import Path
from typing import Optional

from livy_uploads.plugins.base import SetupPlugin

LOGGER = logging.getLogger(__name__)


class UserIdsSetup(SetupPlugin):
    """
    Sets $USERNAME, $USER_UID, $USER_GID and $DOCKER_GID based on the current user's UID and GID.
    """

    def setup(self, basedir: os.PathLike, env_filename: str, env: Mapping[str, str]) -> dict[str, str]:
        username = os.getenv("USERNAME") or getuser()
        if not username:
            raise ValueError("couldn't get username from environment or login")

        user_uid = str(os.getuid())
        user_gid = str(os.getgid())

        if (gid := self.get_docker_gid()) is not None and self.user_has_group(username, gid):
            docker_gid = str(gid)
        else:
            docker_gid = ""

        return {
            "USER": username,
            "USERNAME": username,
            "HOME": str(Path.home()),
            "USER_UID": user_uid,
            "USER_GID": user_gid,
            "DOCKER_GID": docker_gid,
        }

    @classmethod
    def get_docker_gid(cls, group_name: Optional[str] = None) -> Optional[int]:
        """
        Gets the GID of the `docker` group.
        """

        group_name = group_name or "docker"
        try:
            return grp.getgrnam(group_name).gr_gid
        except KeyError:
            return None

    @classmethod
    def user_has_group(cls, username: str, gid: int) -> bool:
        """
        Checks if the user is in that group, either directly or via a secondary group.
        """
        try:
            user_info = pwd.getpwnam(username)
            primary_gid = user_info.pw_gid

            # Check if it's the primary group
            if primary_gid == gid:
                return True

            # Check secondary groups
            groups = os.getgrouplist(username, primary_gid)
            return gid in groups
        except KeyError:
            return False
