import logging

from livy_uploads.converters.base import Converter, ConverterCustomizer
from livy_uploads.models.http import HttpConfig
from livy_uploads.models.kerberos import KerberosConfig
from livy_uploads.models.session import LivyClientConfig

LOGGER = logging.getLogger(__name__)


class AddConfigNames(ConverterCustomizer):
    """
    Registers the model type names.
    """

    def customize_converter(self, converter: Converter) -> Converter:
        classes = (HttpConfig, KerberosConfig, LivyClientConfig)
        for cls in classes:
            converter.register_typename(cls)
        return converter
