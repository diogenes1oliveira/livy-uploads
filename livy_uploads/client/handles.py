from livy_uploads.client.managers import SessionManager
from livy_uploads.models.session import SessionInfo
from livy_uploads.models.sparkmagic import SparkMagicConfig
from livy_uploads.endpoint import LivyEndpoint


class SessionHandle:
    def __init__(
        self,
        info: SessionInfo,
        manager: SessionManager,
        endpoint: LivyEndpoint,
        config: SparkMagicConfig,
    ) -> None:
        self.info = info
        self.manager = manager
        self.endpoint = endpoint
        self.config = config
