import logging

from openg2p_g2p_bridge_models.models import (
    DisbursementResolutionGeoAddress,
    ProcessStatus,
)
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_engine = get_engine()
_logger = logging.getLogger("beneficiary_notification_beat_producer")


@celery_app.task(name="beneficiary_notification_beat_producer")
def beneficiary_notification_beat_producer():
    session_maker = sessionmaker(_engine, expire_on_commit=False)
    with session_maker() as session:
        result = session.execute(
            select(DisbursementResolutionGeoAddress)
            .where(
                DisbursementResolutionGeoAddress.beneficiary_notification_status
                == ProcessStatus.PENDING.value
            )
            .limit(_config.no_of_tasks_to_process)
        )
        disbursement_resolution_geo_addresses = result.scalars().all()
        for disbursement_resolution_geo_address in disbursement_resolution_geo_addresses:
            _logger.info(
                f"Sending beneficiary_notification_worker task for disbursement_id: {disbursement_resolution_geo_address.disbursement_id}"
            )
            disbursement_resolution_geo_address.beneficiary_notification_status = (
                ProcessStatus.PROCESSING.value
            )
            session.add(disbursement_resolution_geo_address)
            session.commit()
            celery_app.send_task(
                "beneficiary_notification_worker",
                args=[disbursement_resolution_geo_address.disbursement_id],
                queue="g2p_bridge_celery_worker_tasks",
            )
        _logger.info("Finished beneficiary_notification_beat_producer")
