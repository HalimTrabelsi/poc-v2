import logging

from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControlGeo,
    ProcessStatus,
)
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_engine = get_engine()
_logger = logging.getLogger("warehouse_notification_beat_producer")


@celery_app.task(name="warehouse_notification_beat_producer")
def warehouse_notification_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        result = session.execute(
            select(DisbursementBatchControlGeo)
            .where(DisbursementBatchControlGeo.warehouse_notification_status == ProcessStatus.PENDING.value)
            .limit(_config.no_of_tasks_to_process)
        )
        disbursement_batch_control_geos = result.scalars().all()
        for disbursement_batch_control_geo in disbursement_batch_control_geos:
            _logger.info(
                f"Sending warehouse_notification_worker task for disbursement_control_geo_id: {disbursement_batch_control_geo.id}"
            )
            disbursement_batch_control_geo.warehouse_notification_status = ProcessStatus.PROCESSING.value
            session.commit()
            celery_app.send_task(
                "warehouse_notification_worker",
                args=[disbursement_batch_control_geo.id],
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Finished warehouse_notification_beat_producer")
