# ruff: noqa: E402

from .config import Settings

_config = Settings.get_config()

from celery import Celery
from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_fastapi_common.exception import BaseExceptionHandler
from openg2p_g2p_bridge_agency_allocator.app import (
    Initializer as AgencyAllocatorInitializer,
)
from openg2p_g2p_bridge_bank_connectors.app import (
    Initializer as BankConnectorInitializer,
)
from openg2p_g2p_bridge_geo_resolver.app import Initializer as GeoResolversInitializer
from openg2p_g2p_bridge_mapper_connectors.app import (
    Initializer as MapperConnectorInitializer,
)
from openg2p_g2p_bridge_notification_connectors.app import (
    Initializer as NotificationConnectorInitializer,
)
from openg2p_g2p_bridge_warehouse_allocator.app import (
    Initializer as WarehouseAllocatorInitializer,
)
from openg2p_g2pconnect_mapper_lib.app import Initializer as MapperInitializer

from .helpers import AgencyHelper, ResolveHelper, WarehouseHelper


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().init_logger()
        super().init_app()
        BaseExceptionHandler()

        BankConnectorInitializer()
        GeoResolversInitializer()
        AgencyAllocatorInitializer()
        WarehouseAllocatorInitializer()
        NotificationConnectorInitializer()
        MapperConnectorInitializer()
        MapperInitializer()
        ResolveHelper()
        WarehouseHelper()
        AgencyHelper()


celery_app = Celery(
    "g2p_bridge_celery_worker",
    broker=_config.celery_broker_url,
    backend=_config.celery_backend_url,
    include=["openg2p_g2p_bridge_celery_workers.tasks"],
)

celery_app.conf.timezone = "UTC"
