from typing import Any, Iterable, Literal, Mapping, Optional, Protocol, TypeVar, overload

from livy_uploads.configs.base import Configurable
from livy_uploads.utils.datautils import get_nested_key

NO_DEFAULT: Any = object()

T = TypeVar("T")


class Converter(Protocol):

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
        raise NotImplementedError


class CattrsConverter(Converter, Configurable):
    """
    Converts the raw config values using cattrs
    """

    def __init__(self) -> None:
        self.converter: Any = None
        "the converter instance once configured"

    def setup(self) -> None:
        import cattrs

        self.converter = cattrs.Converter()

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
                return copy.deepcopy(default)  # type: ignore
            elif nullable:
                return None
            raise

        if t is None:
            return raw_value

        assert self.converter is not None, "converter not initialized"
        return self.converter.structure(raw_value, t)


def _join_key(keys: Iterable[str]) -> str:
    if not keys:
        return "$"
    else:
        return "$." + ".".join(keys)
