import logging
from datetime import datetime, timedelta

from openg2p_g2p_bridge_models.models import DisbursementBatchControl, ProcessStatus
from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mapper_resolution_beat_producer")
def mapper_resolution_beat_producer():
    """
    A Celery beat producer that periodically checks for disbursement batches
    that require mapper resolution and triggers the mapper resolution worker.
    """
    _logger.info("Mapper Resolution Producer running...")

    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        # Get the setting for stale tasks
        stale_at = datetime.now() - timedelta(minutes=_config.task_stale_threshold_minutes)
        # 1. Reset tasks that are in progress for too long (stale)
        session.execute(
            update(DisbursementBatchControl)
            .where(
                DisbursementBatchControl.fa_resolution_status == ProcessStatus.PROCESSING.value,
                DisbursementBatchControl.updated_at > stale_at,
            )
            .values(fa_resolution_status=ProcessStatus.PENDING.value)
        )

        # 2. Select pending tasks
        disbursement_batch_controls = session.scalars(
            select(DisbursementBatchControl)
            .where(
                DisbursementBatchControl.fa_resolution_status == ProcessStatus.PENDING.value,
            )
            .limit(_config.no_of_tasks_to_process)
        ).all()

        if not disbursement_batch_controls:
            _logger.info("No pending disbursement batches for mapper resolution.")
            return

        for disbursement_batch_control in disbursement_batch_controls:
            # 3. Mark as in progress
            disbursement_batch_control.fa_resolution_status = ProcessStatus.PROCESSING.value
            session.add(disbursement_batch_control)
            session.commit()

            # 4. Publish to Celery queue
            celery_app.send_task(
                "mapper_resolution_worker",
                queue="g2p_bridge_celery_worker_tasks",
                args=[disbursement_batch_control.id],
            )
            _logger.info(
                f"Published disbursement batch {disbursement_batch_control.id} to mapper-resolution-worker."
            )

        _logger.info(
            f"Published {len(disbursement_batch_controls)} disbursement batches for mapper resolution."
        )
