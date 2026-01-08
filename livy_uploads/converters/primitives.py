__all__ = ("AddPrimitiveNames",)

from livy_uploads.converters.base import Converter, ConverterCustomizer


class AddPrimitiveNames(ConverterCustomizer):
    """
    Registers the primitive type names.
    """

    def customize_converter(self, converter: Converter) -> Converter:
        typenames = {
            "bool": bool,
            "int": int,
            "float": float,
            "str": str,
            "list": list,
            "dict": dict,
        }
        for typename, type in typenames.items():
            converter.register_typename(type, typename)
        return converter
