import dataclasses
import itertools
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar, Optional, TypeVar
from urllib.parse import parse_qs

from typing_extensions import Self

from livy_uploads.configs.utils import split_envvar
from livy_uploads.plugins.base import FoundObject, FoundPath, FoundType, Matcher, PluginLoader, Predicate

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class CombinedLoader(PluginLoader):
    """
    A loader that combines multiple loaders.
    """

    __impl_typename__: ClassVar[str] = "combined"

    loaders: list[PluginLoader]
    "The loaders to combine."

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses a spec URL like `combined://[name/]?loader=<uri1>&loader=<uri2>...` or a list of comma-separated
        loader URIs or specs.

        >>> CombinedLoader.parse("module://os,file://path/to/file.py")
        CombinedLoader(loaders=[ModuleLoader(module_name='os'), FileLoader(path=PurePosixPath('path/to/file.py'))])

        >>> CombinedLoader.parse("?loader=module://os&loader=entrypoint://.*")
        CombinedLoader(loaders=[ModuleLoader(module_name='os'), EntryPointsLoader(groups=('sparkrl.plugins.*',))])
        """
        from livy_uploads.plugins.load import get_loader

        if "?" in value:
            _, _, value = value.partition("?")
            qs = parse_qs(value, keep_blank_values=False)
            sources = qs.get("loader", []) or []
        else:
            sources = split_envvar(value)

        loaders = [get_loader(s) for s in sources]
        return cls(loaders=loaders)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `combined://[name/]?loader=<uri1>&loader=<uri2>...`.

        >>> loader = CombinedLoader.parse("module://os,file://path/to/file.py")
        >>> loader.named_uri(None)
        'combined://?loader=module%3A%2F%2Fos&loader=file%3A%2F%2Fpath%2Fto%2Ffile.py'

        >>> loader.named_uri("myfield")
        'combined://myfield/?loader=module%3A%2F%2Fos&loader=file%3A%2F%2Fpath%2Fto%2Ffile.py'
        """
        from urllib.parse import urlencode

        loader_uris = [loader.uri for loader in self.loaders]
        query = urlencode([("loader", uri) for uri in loader_uris])
        prefix = f"{name or ''}/" if name else ""
        return f"combined://{prefix}?{query}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves all underlying loaders.

        >>> loader = CombinedLoader.parse("module://os,entrypoint://.p*")
        >>> (resolved_loader,) = loader.resolve()
        >>> [l.uri for l in resolved_loader.loaders]
        ['module://os', 'entrypoint://sparkrl.plugins.patches/']
        """
        resolved_loaders = list(
            itertools.chain.from_iterable(loader.resolve(basedir=basedir) for loader in self.loaders)
        )
        resolved_self = dataclasses.replace(self, loaders=resolved_loaders)
        return (resolved_self,)

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Finds paths from all underlying loaders.

        >>> (loader,) = CombinedLoader.parse("module://os").resolve()
        >>> paths = list(loader.find_paths(pattern="*.py"))
        >>> len(paths) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_paths(pattern=pattern, basedir=basedir)

    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Finds types from all underlying loaders.

        >>> from livy_uploads.commands.base import SessionCommand
        >>> (loader,) = CombinedLoader.parse("entrypoint://.*").resolve()
        >>> types = list(loader.find_types(SessionCommand, pattern="*"))
        >>> len(types) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_types(t, pattern=pattern, match=match, predicate=predicate)

    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Finds objects from all underlying loaders.

        >>> (loader,) = CombinedLoader.parse("entrypoint://.*").resolve()
        >>> objects = list(loader.find_objects(object, pattern="*", match=Matcher.any()))
        >>> len(objects) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_objects(t, pattern=pattern, match=match, predicate=predicate)
