import dataclasses
import os
from collections.abc import Iterator
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any, ClassVar, Optional

from typing_extensions import Self

from livy_uploads.plugins import constants
from livy_uploads.plugins.base import FoundPath, NoCodeMixin, PluginLoader

_MISSING: Any = object()


@dataclasses.dataclass(frozen=True)
class ProfileFileLoader(NoCodeMixin, PluginLoader):
    """
    Loads files based on a set of profiles.
    """

    __impl_typename__: ClassVar[str] = "profile"

    basenames: Optional[tuple[str, ...]] = None
    "Basenames of the profile files to load."

    profiles: Optional[tuple[str, ...]] = None

    basedir: Optional[Path] = dataclasses.field(default=None, init=False, repr=False, hash=False, compare=False)
    "Base directory to find the profile files, once resolved."

    def __post_init__(self) -> None:
        if self.profiles is not None:
            profiles = tuple(dict.fromkeys(self.profiles).keys())
            object.__setattr__(self, "profiles", profiles)

    @property
    def default_basenames(self) -> tuple[str, ...]:
        return ()

    def get_filename(self, basename: str, profile: str) -> str:
        return basename

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses a profile loader URI part

        >>> ProfileFileLoader.parse(".env,env?profiles=prod+override")
        ProfileFileLoader(basenames=('.env', 'env'), profiles=('prod', 'override'))

        >>> ProfileFileLoader.parse(".env?profiles=")
        ProfileFileLoader(basenames=('.env',), profiles=())

        >>> ProfileFileLoader.parse(".env")
        ProfileFileLoader(basenames=('.env',), profiles=None)
        """
        name, sep, profile = value.partition("?profiles=")

        names = name.replace(",", "  ").split()
        profiles = tuple(profile.replace("+", "  ").split()) if sep else None
        return cls(basenames=tuple(names), profiles=profiles)

    @property
    def uri(self) -> str:
        """
        >>> ProfileFileLoader(basenames=(".env", "env",), profiles=("prod","override",)).uri
        'profile://.env,env?profiles=prod+override'

        >>> ProfileFileLoader(basenames=(".env",), profiles=()).uri
        'profile://.env?profiles='

        >>> ProfileFileLoader(basenames=(".env",), profiles=None).uri
        'profile://.env'
        """
        if self.profiles:
            profiles = "?profiles=" + "+".join(self.profiles)
        elif self.profiles is None:
            profiles = ""
        else:
            profiles = "?profiles="

        files = ",".join(self.basenames or ())
        return f"profile://{files}{profiles}"

    def named_uri(self, name: Optional[str], profiles: Optional[tuple[str, ...]] = _MISSING) -> str:
        """
        >>> ProfileFileLoader(basenames=(".env", ".prod.env")).named_uri(".hm.env")  # doctest: +ELLIPSIS
        'profile://.hm.env'
        """
        profiles = profiles if profiles is not _MISSING else self.profiles

        if profiles:
            suffix = "?profiles=" + "+".join(profiles)
        elif profiles is None:
            suffix = ""
        else:
            suffix = "?profiles="

        if name is None:
            names = ",".join(self.basenames or ())
        else:
            names = name

        return f"profile://{names}{suffix}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        >>> from pathlib import Path
        >>> this_file = Path(__file__).absolute()
        >>> this_dir = this_file.parent
        >>> [this_file, this_dir]  # doctest: +ELLIPSIS
        [PosixPath('/.../plugins/profiles.py'), PosixPath('/.../plugins')]

        Relativizes paths within the project:

        >>> loader, = ProfileFileLoader(basenames=(this_file,), profiles=('prod',)).resolve(basedir=this_dir)
        >>> loader
        ProfileFileLoader(basenames=('profiles.py',), profiles=('default', 'prod'))

        Relativizes paths within the home directory:

        >>> bashrc_file = Path.home() / ".bashrc"
        >>> bashrc_file  # doctest: +ELLIPSIS
        PosixPath('/home/.../.bashrc')
        >>> loader, = ProfileFileLoader(basenames=(bashrc_file,)).resolve()
        >>> loader
        ProfileFileLoader(basenames=('~/.bashrc',), profiles=('default', 'override'))

        Otherwise, keeps as an absolute path:
        >>> loader, = ProfileFileLoader(basenames=("/etc/krb5.conf",), profiles=()).resolve()
        >>> loader
        ProfileFileLoader(basenames=('/etc/krb5.conf',), profiles=('default',))
        """
        basedir = (basedir or Path.cwd()).absolute()
        profiles = get_current_profiles() if self.profiles is None else _fix_profiles(self.profiles)

        if self.basenames is not None:
            basenames = tuple([try_relativize(b, basedir) for b in self.basenames])
        else:
            basenames = self.default_basenames

        resolved = dataclasses.replace(self, profiles=profiles, basenames=basenames)
        object.__setattr__(resolved, "basedir", basedir)
        return (resolved,)

    def find_paths(self, *, pattern: str) -> Iterator[FoundPath]:
        """
        Finds relative paths that match the pattern.
        """
        assert self.basedir is not None, f".path not resolved yet in {self}"
        if not self.profiles:
            return

        for profile in self.profiles:
            for basename in self.basenames or ():
                filename = self.get_filename(basename, profile=profile)
                if not fnmatch(filename, pattern):
                    continue

                path = Path(PurePosixPath(filename)).expanduser()
                if not path.is_absolute():
                    path = (self.basedir / path).absolute()
                yield FoundPath(
                    path=path,
                    uri=self.named_uri(filename, profiles=(profile,)),
                    pattern=pattern,
                    loader=self,
                )

        raise NotImplementedError


def get_current_profiles() -> tuple[str, ...]:
    try:
        value = os.environ[constants.PROFILES_ENV]
    except KeyError:
        return (
            "default",
            "override",
        )

    normalized = value.strip()
    for char in ",;|+":
        normalized = normalized.replace(char, " ")

    return _fix_profiles(tuple(normalized.split()))


def _fix_profiles(profiles: tuple[str, ...]) -> tuple[str, ...]:
    profiles = tuple({p: None for p in profiles if p != "default"}.keys())
    return ("default",) + profiles


def try_relativize(filename: str, basedir: Path) -> str:
    path = Path(PurePosixPath(filename))  # hopefully this works in Windows
    if not path.is_absolute():
        path = (basedir / path).absolute()

    try:
        in_project = path.relative_to(basedir)
        return in_project.as_posix()
    except ValueError:
        try:
            in_home = path.relative_to(Path.home())
            return PurePosixPath("~").joinpath(in_home).as_posix()
        except ValueError:
            return path.absolute().as_posix()
