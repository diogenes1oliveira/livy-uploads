import copy
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar, Union, cast, overload

from livy_uploads.configs.base import Configurable
from livy_uploads.converters.base import Converter
from livy_uploads.converters.paths import get_path_resolve_annotation, resolve_path_or_content
from livy_uploads.utils.datautils import get_nested_key

if TYPE_CHECKING:
    from cattrs import Converter as cattrs_Converter
else:
    cattrs_Converter = Any

NO_DEFAULT: Any = object()

T = TypeVar("T")


class CattrsConverter(Converter, Configurable):
    """
    Converts the raw config values using cattrs
    """

    def __init__(self) -> None:
        self.instance: Optional[cattrs_Converter] = None
        "the converter instance once configured"

        self.typenames: dict[str, type] = {}
        "the registered type names"

    def setup(self) -> None:
        import cattrs

        self.instance = cattrs.Converter()

    def get_type_by_name(self, typename: str) -> type:
        return self.typenames[typename]

    def register_typename(self, t: type, typename: str) -> None:
        self.typenames[typename] = t

    # When type T is provided
    @overload
    def __call__(
        self, raw: Mapping[str, Any], keys: Iterable[str], t: type[T], *, nullable: Literal[True]
    ) -> Optional[T]: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], t: type[T], *, nullable: Literal[False]) -> T: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], t: type[T], *, default: None) -> Optional[T]: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], t: type[T], *, default: T) -> T: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], t: type[T]) -> T: ...

    # When type is not provided
    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], *, nullable: Literal[True]) -> Optional[Any]: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str], *, default: Any) -> Any: ...

    @overload
    def __call__(self, raw: Mapping[str, Any], keys: Iterable[str]) -> Any: ...

    def __call__(
        self,
        raw: Mapping[str, Any],
        keys: Iterable[str],
        t: Optional[type[T]] = None,
        *,
        nullable: Optional[bool] = None,
        default: Any = NO_DEFAULT,
    ) -> Any:
        """
        Gets a value from the raw config

        Args:
            raw: the raw config
            keys: the key path to get the value from
            t: the type to convert the value to. If not given, the value is returned as is.
            nullable: whether the value can be null or missing.
            default: the default value to return if the key is not found or the value is null.

        Raises:
            KeyError: if the key is not found or the value is null for a required config.
            ValueError: if there's a value, but it cannot be converted to the given type.
            TypeError: if both nullable and default are specified.
        """
        if nullable is not None and default is not NO_DEFAULT:
            raise TypeError("cannot specify both nullable and default")

        try:
            raw_value = get_nested_key(raw, keys)
            if raw_value is None:
                raise KeyError(f"null value found for key .{_join_key(keys)!r}")
        except TypeError:
            raise KeyError(f"nested key .{_join_key(keys)!r} not found") from None
        except KeyError:
            if default is not NO_DEFAULT:
                return copy.deepcopy(default)
            elif nullable:
                return None
            raise

        if t is None:
            return raw_value

        assert self.instance is not None, "converter not initialized"
        return self.instance.structure(raw_value, t)


def _join_key(keys: Iterable[str]) -> str:
    if not keys:
        return "$"
    else:
        return "$." + ".".join(keys)


def register_cattrs_path_resolver(converter: Converter) -> None:
    from typing import get_args

    import cattrs

    from livy_uploads.project.project import Project

    instance = getattr(converter, "instance", None)
    assert isinstance(instance, cattrs.Converter), f"not a cattrs converter: {converter!r}"

    def structure_hook_path_project_resolver(value: Any, t: type) -> Union[Path, PurePosixPath]:
        resolved = get_path_resolve_annotation(t)
        assert resolved is not None, f"no path resolve annotation found for t={t!r}"
        mode, filename = resolved

        # Extract the actual type from Annotated
        actual_type = get_args(t)[0]
        assert issubclass(actual_type, (Path, PurePosixPath)), f"expected path type, got {actual_type=!r} instead"

        if not isinstance(value, (str, Path, PurePosixPath)):
            raise TypeError(f"expected path-like value, got {type(value)} instead")

        project = Project.get()
        result = resolve_path_or_content(
            value=value,
            mode=mode,
            filename=filename,
            t=cast(Any, actual_type),
            basedir=project.basedir,
            cachedir=project.cachedir,
        )
        return result  # type: ignore

    instance.register_structure_hook_func(
        lambda t: get_path_resolve_annotation(t) is not None,
        structure_hook_path_project_resolver,
    )
