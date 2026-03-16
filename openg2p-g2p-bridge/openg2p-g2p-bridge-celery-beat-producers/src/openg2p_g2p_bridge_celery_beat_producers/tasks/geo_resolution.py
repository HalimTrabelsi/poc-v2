import logging

from openg2p_g2p_bridge_models.models import DisbursementBatchControl, ProcessStatus
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="geo_resolution_beat_producer")
def geo_resolution_beat_producer():
    _logger.info("Checking for disbursement batches to perform geo resolution")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        disbursement_batch_controls = (
            session.execute(
                select(DisbursementBatchControl)
                .filter(DisbursementBatchControl.geo_resolution_status == ProcessStatus.PENDING.value)
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )

        for disbursement_batch_control in disbursement_batch_controls:
            _logger.info(f"Sending geo resolution task for batch: {disbursement_batch_control.id}")

            disbursement_batch_control.geo_resolution_status = ProcessStatus.PROCESSING.value
            session.commit()
            celery_app.send_task(
                "geo_resolution_worker",
                args=(disbursement_batch_control.id,),
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Completed checking for disbursement batches to perform geo resolution")
