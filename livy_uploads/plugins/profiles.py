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

    This loader generates paths by combining basenames with profiles. Each profile
    represents a configuration variant (e.g., 'prod', 'dev', 'staging'). The loader
    automatically prepends a 'default' profile to ensure base configuration is always
    loaded first.

    Profiles can be specified explicitly via the profiles parameter, or loaded from
    the environment variable specified in constants.PROFILES_ENV. If not specified,
    defaults to ('default', 'override').

    The loader does not check if files exist - it generates all possible paths for
    each basename/profile combination and yields them when they match the search pattern.
    """

    __impl_typename__: ClassVar[str] = "profile"

    basenames: Optional[tuple[str, ...]] = None
    "Basenames of the profile files to load."

    profiles: Optional[tuple[str, ...]] = None
    """Profile names to use for file resolution.

    - None: Load from environment or use defaults ('default', 'override')
    - (): Empty tuple, will be normalized to ('default',) after resolve()
    - ('prod', 'dev'): Specific profiles, will be normalized to ('default', 'prod', 'dev')
    """

    basedir: Optional[Path] = dataclasses.field(default=None, init=False, repr=False, hash=False, compare=False)
    "Base directory to find the profile files, once resolved."

    def __post_init__(self) -> None:
        """Deduplicates profile names while preserving order."""
        if self.profiles is not None:
            profiles = tuple(dict.fromkeys(self.profiles).keys())
            object.__setattr__(self, "profiles", profiles)

    @property
    def default_basenames(self) -> tuple[str, ...]:
        """Returns default basenames when none are specified. Override in subclasses."""
        return ()

    def get_filenames(self, basename: str, profile: str) -> tuple[str, ...]:
        """
        Generates the filenames for a given basename and profile combination.

        The base implementation simply returns the basename unchanged. Subclasses can
        override this to implement profile-specific naming conventions, such as:
        - Inserting profile into the name: ".env" -> ".prod.env" for profile="prod"
        - Using profile as a directory: "config.yml" -> "prod/config.yml"
        - Adding profile as a suffix: "app.conf" -> "app-prod.conf"

        Args:
            basename: The base filename to transform
            profile: The profile name to incorporate

        Returns:
            The filenames to use for this basename/profile combination
        """
        return (basename,)

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
        Resolves the loader by normalizing profiles and relativizing paths.

        This method performs three key transformations:

        1. Profile Resolution:
           - If profiles is None: loads from environment or uses defaults
           - If profiles is (): normalizes to ('default',)
           - Otherwise: prepends 'default' and deduplicates

        2. Path Relativization:
           - Paths within basedir: converted to relative paths
           - Paths within home directory: converted to ~/relative/path
           - Other absolute paths: kept as absolute
           - Relative paths: kept as-is

        3. Sets the basedir attribute for use by find_paths()

        Args:
            basedir: Base directory for path resolution. Defaults to current working directory.

        Returns:
            A tuple containing the resolved loader instance.

        Examples:

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
        Finds paths that match the given pattern across all profile/basename combinations.

        This method generates paths by iterating over all profiles and basenames, creating
        a Cartesian product of possibilities. For each combination:

        1. Calls get_filenames(basename, profile) to get the actual filenames
        2. Checks if each filename matches the pattern using fnmatch
        3. Resolves the path to an absolute Path (handling ~ expansion and relative paths)
        4. Yields a FoundPath with the resolved path and a profile-specific URI

        Important behaviors:

        - Does NOT check if files exist - yields paths whether they exist or not
        - Returns empty if profiles is empty (though resolve() always adds 'default')
        - Must be called after resolve() - raises AssertionError if basedir is None
        - Pattern matching uses fnmatch, so supports wildcards like *.env or config.*
        - Each yielded path gets a unique URI identifying its specific profile

        Args:
            pattern: Filename pattern to match (supports fnmatch wildcards)

        Yields:
            FoundPath instances for each basename/profile combination matching the pattern.
            Each FoundPath contains:
            - path: Absolute path to the file (may not exist)
            - uri: Profile-specific URI like "profile://.env?profiles=prod"
            - pattern: The pattern that was matched
            - loader: Reference to this loader instance

        Raises:
            AssertionError: If called before resolve() (basedir is None)

        Examples:
            With 2 basenames and 3 profiles, and a pattern that matches both basenames,
            this will yield 6 paths (2 * 3).
        """
        assert self.basedir is not None, f".path not resolved yet in {self}"
        if not self.profiles:
            return

        for profile in self.profiles:
            for basename in self.basenames or ():
                filenames = self.get_filenames(basename, profile=profile)
                for filename in filenames:
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


def get_current_profiles() -> tuple[str, ...]:
    """
    Loads the current profiles from the environment or returns defaults.

    This function reads profile names from the environment variable specified in
    constants.PROFILES_ENV (typically SPARKRL_PROFILES). It supports multiple
    separator characters for flexibility: comma, semicolon, pipe, and plus.

    Profile Loading Behavior:

    1. If environment variable is not set: returns ('default', 'override')
    2. If set: parses the value, splits by separators, and normalizes
    3. Always ensures 'default' is first and removes duplicates

    Supported separators: , ; | +

    Examples:
        SPARKRL_PROFILES="prod,staging" -> ('default', 'prod', 'staging')
        SPARKRL_PROFILES="prod+test" -> ('default', 'prod', 'test')
        SPARKRL_PROFILES="  prod , test  " -> ('default', 'prod', 'test')
        (not set) -> ('default', 'override')

    Returns:
        Tuple of profile names with 'default' always first, duplicates removed.
    """
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
    """
    Normalizes a profile tuple by ensuring 'default' is always first.

    This function performs two operations:
    1. Removes any existing 'default' entries from the input
    2. Prepends 'default' to the beginning
    3. Removes duplicates while preserving order (excluding 'default')

    This ensures that the 'default' profile is always loaded first, providing
    a base configuration that can be overridden by subsequent profiles.

    Args:
        profiles: Tuple of profile names (may be empty, may contain 'default')

    Returns:
        Normalized tuple with 'default' first, no duplicates

    Examples:
        ('prod', 'staging') -> ('default', 'prod', 'staging')
        ('default', 'prod') -> ('default', 'prod')
        () -> ('default',)
        ('prod', 'test', 'prod') -> ('default', 'prod', 'test')
    """
    profiles = tuple({p: None for p in profiles if p != "default"}.keys())
    return ("default",) + profiles


def try_relativize(filename: str, basedir: Path) -> str:
    """
    Attempts to convert a file path to a relative or tilde-prefixed representation.

    This function tries multiple strategies to make paths more portable and readable:

    1. If path is relative: resolves it against basedir first
    2. Try to make relative to basedir (project-relative path)
    3. If outside basedir: try to make relative to home directory (~/...)
    4. If outside home: keep as absolute path

    The goal is to generate paths that are portable across different machines and
    user accounts when possible. Paths within the project directory become relative,
    paths in the home directory use ~, and system paths remain absolute.

    Args:
        filename: File path to relativize (can be relative or absolute)
        basedir: Base directory for project-relative paths

    Returns:
        Relativized path as a POSIX-style string:
        - "config/.env" if within basedir
        - "~/.bashrc" if within home directory
        - "/etc/krb5.conf" if outside both

    Examples:
        try_relativize("/project/src/config.py", Path("/project"))
        -> "src/config.py"

        try_relativize("/home/user/.profile", Path("/project"))
        -> "~/.profile"

        try_relativize("/etc/hosts", Path("/project"))
        -> "/etc/hosts"

        try_relativize("./local/config.py", Path("/project"))
        -> "local/config.py"
    """
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
