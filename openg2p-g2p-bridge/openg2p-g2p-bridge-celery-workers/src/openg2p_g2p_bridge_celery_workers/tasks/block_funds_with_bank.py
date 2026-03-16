import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface import (
    BankConnectorInterface,
    BlockFundsResponse,
)
from openg2p_g2p_bridge_models.models import (
    DisbursementEnvelope,
    EnvelopeBatchStatusForCash,
    FundsBlockedWithBankEnum,
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


@celery_app.task(name="block_funds_with_bank_worker")
def block_funds_with_bank_worker(disbursement_envelope_id: str):
    _logger.info(f"Blocking funds with bank for envelope: {disbursement_envelope_id}")
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
                f"Disbursement Envelope Batch Status not found for envelope id: {disbursement_envelope_id}"
            )
            return

        sponsor_bank_configuration: (
            SponsorBankConfiguration
        ) = WarehouseHelper.get_component().retrieve_sponsor_bank_configuration(
            disbursement_envelope.benefit_program_id,
            disbursement_envelope.benefit_code_id,
        )
        _logger.info(
            f"Sponsor bank configuration retrieved for: {sponsor_bank_configuration.sponsor_bank_code}"
        )

        total_funds_needed = disbursement_envelope.total_disbursement_quantity
        bank_connector: BankConnectorInterface = BankConnectorFactory.get_component().get_bank_connector(
            sponsor_bank_configuration.sponsor_bank_code
        )

        try:
            funds_blocked: BlockFundsResponse = bank_connector.block_funds(
                sponsor_bank_configuration.program_account_number,
                disbursement_envelope.measurement_unit,
                total_funds_needed,
            )

            if funds_blocked.status == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS:
                envelope_batch_status_for_cash.funds_blocked_with_bank = (
                    FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value
                )
                envelope_batch_status_for_cash.funds_blocked_reference_number = (
                    funds_blocked.block_reference_no
                )
                envelope_batch_status_for_cash.funds_blocked_latest_error_code = None

            else:
                envelope_batch_status_for_cash.funds_blocked_with_bank = (
                    FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE.value
                )
                envelope_batch_status_for_cash.funds_blocked_reference_number = ""
                envelope_batch_status_for_cash.funds_blocked_latest_error_code = funds_blocked.error_code
                raise ValueError(
                    f"Failed to block funds with bank for envelope {disbursement_envelope_id}: {funds_blocked.error_code}"
                )

            envelope_batch_status_for_cash.funds_blocked_latest_timestamp = datetime.now()

            envelope_batch_status_for_cash.funds_blocked_attempts += 1

        except Exception as e:
            _logger.error(f"Error blocking funds with bank for envelope {disbursement_envelope_id}: {str(e)}")
            envelope_batch_status_for_cash.funds_blocked_latest_timestamp = datetime.now()
            envelope_batch_status_for_cash.funds_blocked_latest_error_code = str(e)
            envelope_batch_status_for_cash.funds_blocked_attempts += 1
            envelope_batch_status_for_cash.funds_blocked_reference_number = ""
            if (
                envelope_batch_status_for_cash.funds_blocked_attempts
                >= _config.block_funds_with_bank_max_attempts
            ):
                envelope_batch_status_for_cash.funds_blocked_with_bank = (
                    FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE.value
                )
            else:
                envelope_batch_status_for_cash.funds_blocked_with_bank = FundsBlockedWithBankEnum.ERROR.value
            session.commit()

        session.commit()
        _logger.info(f"Completed blocking funds with bank for envelope: {disbursement_envelope_id}")
