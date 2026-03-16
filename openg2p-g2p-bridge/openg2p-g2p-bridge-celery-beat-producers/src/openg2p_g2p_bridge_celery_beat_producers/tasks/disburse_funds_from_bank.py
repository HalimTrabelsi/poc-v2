import logging
from datetime import datetime, timedelta

from openg2p_g2p_bridge_models.models import (
    CancellationStatus,
    DisbursementBatchControl,
    DisbursementEnvelope,
    EnvelopeBatchStatusForCash,
    FundsBlockedWithBankEnum,
    ProcessStatus,
)
from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="disburse_funds_from_bank_beat_producer")
def disburse_funds_from_bank_beat_producer():
    _logger.info("Running disburse_funds_from_bank_beat_producer")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        # 1. Reset stale 'PROCESSING' batches back to 'PENDING'
        stale_at = datetime.now() - timedelta(minutes=_config.task_stale_threshold_minutes)
        reset_stmt = (
            update(DisbursementBatchControl)
            .where(
                DisbursementBatchControl.sponsor_bank_dispatch_status == ProcessStatus.PROCESSING.value,
                DisbursementBatchControl.updated_at < stale_at,
            )
            .values(sponsor_bank_dispatch_status=ProcessStatus.PENDING.value)
        )
        session.execute(reset_stmt)
        session.commit()

        disbursement_batch_controls: list[DisbursementBatchControl] = (
            session.execute(
                select(DisbursementBatchControl)
                .filter(
                    DisbursementBatchControl.sponsor_bank_dispatch_status == ProcessStatus.PENDING.value,
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(disbursement_batch_controls)} pending batch controls")

        for disbursement_batch_control in disbursement_batch_controls:
            if check_envelope_status(session, disbursement_batch_control):
                # 2. Mark as PROCESSING
                disbursement_batch_control.sponsor_bank_dispatch_status = ProcessStatus.PROCESSING.value
                session.add(disbursement_batch_control)
                session.commit()
                celery_app.send_task(
                    "disburse_funds_from_bank_worker",
                    (disbursement_batch_control.id,),
                    queue="g2p_bridge_celery_worker_tasks",
                )
                _logger.info(f"Sent tasks to disburse funds for {len(disbursement_batch_controls)} batches")
            else:
                _logger.warning(
                    f"Disbursement batch control {disbursement_batch_control.id} does not meet the criteria for processing."
                )


def check_envelope_status(session, disbursement_batch_control) -> bool:
    disbursement_envelope = (
        session.execute(
            select(DisbursementEnvelope).filter(
                DisbursementEnvelope.id == disbursement_batch_control.disbursement_envelope_id,
            )
        )
        .scalars()
        .first()
    )

    envelope_batch_status_for_cash = (
        session.query(EnvelopeBatchStatusForCash)
        .filter(
            EnvelopeBatchStatusForCash.disbursement_envelope_id
            == disbursement_batch_control.disbursement_envelope_id
        )
        .first()
    )

    if disbursement_envelope.cancellation_status == CancellationStatus.CANCELLED.value:
        _logger.warning(f"Disbursement Envelope {disbursement_envelope.id} is cancelled.")
        return False

    if (
        not envelope_batch_status_for_cash.funds_blocked_with_bank
        == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value
    ):
        _logger.warning(f"Funds are not blocked for envelope {disbursement_envelope.id}.")
        return False

    return True
