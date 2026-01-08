from abc import abstractmethod
from typing import Any, ClassVar, Iterable, Literal, Mapping, Optional, Protocol, TypeVar, overload

from livy_uploads.configs.base import Configurable
from livy_uploads.plugins.impls import Implementation

NO_DEFAULT: Any = object()

T = TypeVar("T")


class Converter(Protocol):

    def setup(self) -> None:
        pass

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

    def get_type_by_name(self, typename: str) -> type:
        raise NotImplementedError

    def register_typename(self, t: type, typename: str) -> None:
        raise NotImplementedError


class ConverterCustomizer(Implementation):
    """
    Customizes a converter.
    """

    __plugin_group__: ClassVar[str] = "sparkrl.plugins.converters"
    "The group of this converter customizer."

    @abstractmethod
    def customize_converter(self, converter: Converter) -> Converter:
        raise NotImplementedError
