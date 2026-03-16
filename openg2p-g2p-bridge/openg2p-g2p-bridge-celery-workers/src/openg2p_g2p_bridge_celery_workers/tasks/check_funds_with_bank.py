import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import (
    BankConnectorFactory,
)
from openg2p_g2p_bridge_models.models import (
    DisbursementEnvelope,
    EnvelopeBatchStatusForCash,
    FundsAvailableWithBankEnum,
)
from openg2p_g2p_bridge_models.schemas import (
    SponsorBankConfiguration,
)
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine
from ..helpers import WarehouseHelper

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="check_funds_with_bank_worker")
def check_funds_with_bank_worker(disbursement_envelope_id: str):
    _logger.info(f"Checking funds with bank for envelope: {disbursement_envelope_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)

    with session_maker() as session:
        disbursement_envelope = (
            session.query(DisbursementEnvelope)
            .filter(DisbursementEnvelope.id == disbursement_envelope_id)
            .first()
        )

        if not disbursement_envelope:
            _logger.error(f"Disbursement Envelope not found for envelope id: {disbursement_envelope_id}")
            return

        envelope_batch_status_for_cash = (
            session.query(EnvelopeBatchStatusForCash)
            .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == disbursement_envelope_id)
            .first()
        )

        if not envelope_batch_status_for_cash:
            _logger.error(
                f"Envelope Batch Status For Digital Cash not found for envelope id: {disbursement_envelope_id}"
            )
            return

        sponsor_bank_configuration: (
            SponsorBankConfiguration
        ) = WarehouseHelper.get_component().retrieve_sponsor_bank_configuration(
            disbursement_envelope.benefit_program_id,
            disbursement_envelope.benefit_code_id,
        )

        total_funds_needed = disbursement_envelope.total_disbursement_quantity
        _logger.info(
            f"Check funds in bank {sponsor_bank_configuration.sponsor_bank_code} for account {sponsor_bank_configuration.program_account_number} for amount {total_funds_needed} {disbursement_envelope.measurement_unit}"
        )
        bank_connector = BankConnectorFactory.get_component().get_bank_connector(
            sponsor_bank_configuration.sponsor_bank_code
        )

        try:
            funds_available = (
                bank_connector.check_funds(
                    sponsor_bank_configuration.program_account_number,
                    disbursement_envelope.measurement_unit,
                    total_funds_needed,
                ).status
                == FundsAvailableWithBankEnum.FUNDS_AVAILABLE
            )

            if funds_available:
                envelope_batch_status_for_cash.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value
                )
            else:
                envelope_batch_status_for_cash.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE.value
                )

            envelope_batch_status_for_cash.funds_available_latest_timestamp = datetime.now()
            envelope_batch_status_for_cash.funds_available_latest_error_code = None
            envelope_batch_status_for_cash.funds_available_attempts += 1

        except Exception as e:
            _logger.error(f"Error checking funds with bank for envelope {disbursement_envelope_id}: {e}")
            envelope_batch_status_for_cash.funds_available_latest_timestamp = datetime.now()
            envelope_batch_status_for_cash.funds_available_latest_error_code = str(e)
            envelope_batch_status_for_cash.funds_available_attempts += 1
            if (
                envelope_batch_status_for_cash.funds_available_attempts
                >= _config.check_funds_with_bank_max_attempts
            ):
                envelope_batch_status_for_cash.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.ERROR.value
                )
            else:
                envelope_batch_status_for_cash.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.PENDING_CHECK.value
                )
        _logger.info(f"Checked funds with bank for envelope: {disbursement_envelope_id}")
        session.commit()
