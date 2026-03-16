import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    CancellationStatus,
    DisbursementEnvelope,
    EnvelopeBatchStatusForCash,
    EnvelopeControl,
    FundsAvailableWithBankEnum,
)
from sqlalchemy import and_, literal, or_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="check_funds_with_bank_beat_producer")
def check_funds_with_bank_beat_producer():
    _logger.info("Checking funds with bank")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        # Check if the disbursement schedule date is today if the configuration is
        # not set to process future disbursement schedules
        date_condition = (
            DisbursementEnvelope.disbursement_schedule_date == datetime.now().date()
            if not _config.process_future_disbursement_schedules
            else literal(True)
        )

        disbursement_envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .join(
                    EnvelopeBatchStatusForCash,
                    DisbursementEnvelope.id == EnvelopeBatchStatusForCash.disbursement_envelope_id,
                )
                .join(
                    EnvelopeControl,
                    DisbursementEnvelope.id == EnvelopeControl.disbursement_envelope_id,
                )
                .filter(
                    date_condition,
                    DisbursementEnvelope.cancellation_status == CancellationStatus.NOT_CANCELLED.value,
                    DisbursementEnvelope.number_of_disbursements
                    == EnvelopeControl.number_of_disbursements_received,
                    DisbursementEnvelope.total_disbursement_quantity
                    == EnvelopeControl.total_disbursement_quantity_received,
                    or_(
                        and_(
                            EnvelopeBatchStatusForCash.funds_available_with_bank
                            == FundsAvailableWithBankEnum.PENDING_CHECK.value
                        ),
                        and_(
                            EnvelopeBatchStatusForCash.funds_available_with_bank
                            == FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE.value
                        ),
                    ),
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )

        for disbursement_envelope in disbursement_envelopes:
            _logger.info(f"Sending task to check funds with bank for envelope {disbursement_envelope.id}")
            envelope_batch_status_for_cash = (
                session.query(EnvelopeBatchStatusForCash)
                .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == disbursement_envelope.id)
                .first()
            )

            envelope_batch_status_for_cash.funds_available_with_bank = (
                FundsAvailableWithBankEnum.CHECK_IN_PROGRESS.value
            )
            session.commit()
            celery_app.send_task(
                "check_funds_with_bank_worker",
                args=(disbursement_envelope.id,),
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Checking funds with bank beat tasks push completed")
