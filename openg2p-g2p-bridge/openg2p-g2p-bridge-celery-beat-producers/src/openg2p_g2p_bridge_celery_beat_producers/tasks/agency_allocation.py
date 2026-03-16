import logging

from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControl,
    ProcessStatus,
)
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_engine = get_engine()
_logger = logging.getLogger("agency_allocation_beat_producer")


@celery_app.task(name="agency_allocation_beat_producer")
def agency_allocation_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        result = session.execute(
            select(DisbursementBatchControl)
            .where(
                DisbursementBatchControl.agency_allocation_status == ProcessStatus.PENDING.value,
            )
            .limit(_config.no_of_tasks_to_process)
        )
        disbursement_batch_controls = result.scalars().all()
        for disbursement_batch_control in disbursement_batch_controls:
            _logger.info(
                f"Sending agency_allocation_worker task for batch_control_id: {disbursement_batch_control.id}"
            )
            disbursement_batch_control.agency_allocation_status = ProcessStatus.PROCESSING.value
            session.commit()
            celery_app.send_task(
                "agency_allocation_worker",
                args=[disbursement_batch_control.id],
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Finished agency_allocation_beat_producer")
