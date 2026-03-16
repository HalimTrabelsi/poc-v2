import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    CancellationStatus,
    DisbursementEnvelope,
    EnvelopeBatchStatusForCash,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from sqlalchemy import and_, literal, or_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="block_funds_with_bank_beat_producer")
def block_funds_with_bank_beat_producer():
    _logger.info("Checking for envelopes to block funds with bank")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        # Check if the disbursement schedule date is today if the configuration is
        # not set to process future disbursement schedules
        date_condition = (
            DisbursementEnvelope.disbursement_schedule_date == datetime.now().date()
            if not _config.process_future_disbursement_schedules
            else literal(True)
        )

        envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .join(
                    EnvelopeBatchStatusForCash,
                    DisbursementEnvelope.id == EnvelopeBatchStatusForCash.disbursement_envelope_id,
                )
                .filter(
                    date_condition,
                    DisbursementEnvelope.cancellation_status == CancellationStatus.NOT_CANCELLED.value,
                    EnvelopeBatchStatusForCash.funds_available_with_bank
                    == FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value,
                    or_(
                        and_(
                            EnvelopeBatchStatusForCash.funds_blocked_with_bank
                            == FundsBlockedWithBankEnum.PENDING_CHECK.value
                        ),
                        and_(
                            EnvelopeBatchStatusForCash.funds_blocked_with_bank
                            == FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE.value
                        ),
                    ),
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )

        for envelope in envelopes:
            _logger.info(f"Blocking funds with bank for envelope: {envelope.id}")
            envelope_batch_status_for_cash = (
                session.query(EnvelopeBatchStatusForCash)
                .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == envelope.id)
                .first()
            )

            envelope_batch_status_for_cash.funds_blocked_with_bank = (
                FundsBlockedWithBankEnum.CHECK_IN_PROGRESS.value
            )
            session.commit()

            celery_app.send_task(
                "block_funds_with_bank_worker",
                args=(envelope.id,),
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Completed checking for envelopes to block funds with bank")
