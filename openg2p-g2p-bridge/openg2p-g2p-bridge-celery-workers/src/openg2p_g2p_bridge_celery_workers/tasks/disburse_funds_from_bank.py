import logging
import random
import time
from datetime import datetime
from typing import List

from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface.bank_connector_interface import (
    BankConnectorInterface,
    DisbursementPaymentPayload,
    PaymentStatus,
)
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    Disbursement,
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementRecon,
    DisbursementResolutionFinancialAddress,
    EnvelopeBatchStatusForCash,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import (
    AgencyDetailForPayment,
    SponsorBankConfiguration,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine
from ..helpers import AgencyHelper, WarehouseHelper

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="disburse_funds_from_bank_worker")
def disburse_funds_from_bank_worker(disbursement_batch_control_id: str):
    _logger.info(f"Disbursing funds with bank for batch: {disbursement_batch_control_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)

    with session_maker() as session:
        disbursement_batch_control = (
            session.query(DisbursementBatchControl)
            .filter(
                DisbursementBatchControl.id == disbursement_batch_control_id,
            )
            .first()
        )

        disbursement_envelope = (
            session.query(DisbursementEnvelope)
            .filter(DisbursementEnvelope.id == disbursement_batch_control.disbursement_envelope_id)
            .first()
        )
        if not disbursement_envelope:
            _logger.error("No Disbursement Envelope found")
            return

        envelope_batch_status_for_cash = (
            session.query(EnvelopeBatchStatusForCash)
            .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == disbursement_envelope.id)
            .first()
        )
        if not envelope_batch_status_for_cash:
            _logger.error("No EnvelopeBatchStatusForDigitalCash found")
            return

        sponsor_bank_configuration: (
            SponsorBankConfiguration
        ) = WarehouseHelper.get_component().retrieve_sponsor_bank_configuration(
            disbursement_envelope.benefit_program_id,
            disbursement_envelope.benefit_code_id,
        )

        disbursement_payment_payloads: List[DisbursementPaymentPayload]

        if disbursement_envelope.benefit_type == BenefitType.CASH_DIGITAL.value:
            disbursement_payment_payloads, zero_quantity_reconciled_count = (
                construct_disbursement_payloads_for_digital_cash(
                    disbursement_batch_control_id,
                    session,
                    disbursement_envelope,
                    envelope_batch_status_for_cash,
                    sponsor_bank_configuration,
                )
            )

        elif disbursement_envelope.benefit_type == BenefitType.CASH_PHYSICAL.value:
            disbursement_payment_payloads, zero_quantity_reconciled_count = (
                construct_disbursement_payloads_for_physical_cash(
                    disbursement_batch_control_id,
                    session,
                    disbursement_envelope,
                    envelope_batch_status_for_cash,
                    sponsor_bank_configuration,
                )
            )

        bank_connector: BankConnectorInterface = BankConnectorFactory.get_component().get_bank_connector(
            sponsor_bank_configuration.sponsor_bank_code
        )

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                _logger.info(
                    f"Locking envelope {disbursement_envelope.id}, attempt {attempt} / {max_retries}"
                )
                envelope_batch_status_for_cash = (
                    session.query(EnvelopeBatchStatusForCash)
                    .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == disbursement_envelope.id)
                    .with_for_update(nowait=True)
                    .populate_existing()
                    .one()
                )
                _logger.info(f"Lock acquired for envelope {disbursement_envelope.id}")
                _logger.info(f"Total number of disbursements: {len(disbursement_payment_payloads)}")
                # fire the payment
                payment_response = bank_connector.initiate_payment(disbursement_payment_payloads)
                _logger.info(
                    f"Payment response for envelope {disbursement_envelope.id} on attempt {attempt}: {payment_response.status}"
                )

                # update envelope status
                if payment_response.status == PaymentStatus.SUCCESS:
                    disbursement_batch_control.sponsor_bank_dispatch_status = ProcessStatus.PROCESSED.value
                    disbursement_batch_control.sponsor_bank_dispatch_latest_error_code = None
                    disbursement_batch_control.sponsor_bank_dispatch_timestamp = datetime.now()
                    disbursement_batch_control.sponsor_bank_dispatch_attempts += 1
                    envelope_batch_status_for_cash.number_of_disbursements_shipped += len(
                        disbursement_payment_payloads
                    )
                    if zero_quantity_reconciled_count:
                        envelope_batch_status_for_cash.number_of_disbursements_reconciled += (
                            zero_quantity_reconciled_count
                        )
                else:
                    raise ValueError(
                        f"Payment failed for envelope {disbursement_envelope.id}: {payment_response.error_code}"
                    )

                session.commit()
                break

            except OperationalError as oe:
                session.rollback()
                _logger.warning(f"Attempt {attempt} to lock envelope {disbursement_envelope.id} failed: {oe}")
                if attempt < max_retries:
                    time.sleep(random.uniform(8, 15))
                else:
                    _logger.error(f"Could not lock after {max_retries} tries, marking pending")
                    raise oe

            except Exception as e:
                session.rollback()
                _logger.error(
                    f"Unexpected error during disbursement for envelope {disbursement_envelope.id}: {e}"
                )
                disbursement_batch_control.sponsor_bank_dispatch_latest_error_code = str(e)
                disbursement_batch_control.sponsor_bank_dispatch_timestamp = datetime.now()
                disbursement_batch_control.sponsor_bank_dispatch_attempts += 1
                if (
                    disbursement_batch_control.sponsor_bank_dispatch_attempts
                    >= _config.disburse_funds_with_bank_max_attempts
                ):
                    disbursement_batch_control.sponsor_bank_dispatch_status = ProcessStatus.ERROR.value
                else:
                    disbursement_batch_control.sponsor_bank_dispatch_status = ProcessStatus.PENDING.value
                session.commit()
                break

        _logger.info(f"Disbursement task for batch {disbursement_batch_control_id} completed")


def construct_disbursement_payloads_for_digital_cash(
    disbursement_batch_control_id,
    session,
    envelope,
    envelope_batch_status_for_digital_cash,
    sponsor_bank_configuration,
) -> tuple[List[DisbursementPaymentPayload], int]:
    _logger.info(f"Constructing disbursement payloads for digital cash: {disbursement_batch_control_id}")
    disbursements = (
        session.query(Disbursement)
        .filter(Disbursement.disbursement_batch_control_id == disbursement_batch_control_id)
        .all()
    )

    disbursement_payment_payloads = []
    zero_quantity_reconciled_count = 0

    for disbursement in disbursements:
        # If disbursement quantity is less than or equal to 0, don't send
        if disbursement.disbursement_quantity <= 0:
            _logger.warning(
                f"Skipping disbursement {disbursement.id} with non-positive quantity: {disbursement.disbursement_quantity}"
            )
            # For digital cash, we also record a recon row to mark as reconciled
            disbursement_recon = DisbursementRecon(
                disbursement_batch_control_id=disbursement.disbursement_batch_control_id,
                disbursement_id=disbursement.id,
                disbursement_batch_control_geo_id=None,
                disbursement_envelope_id=envelope.id,
            )
            session.add(disbursement_recon)
            zero_quantity_reconciled_count += 1
            continue
        disbursement_resolution_financial_address = (
            session.query(DisbursementResolutionFinancialAddress)
            .filter(DisbursementResolutionFinancialAddress.disbursement_id == disbursement.id)
            .first()
        )
        beneficiary_name = "N/A"
        if disbursement.beneficiary_name and len(disbursement.beneficiary_name) > 0:
            beneficiary_name = disbursement.beneficiary_name
        elif (
            disbursement_resolution_financial_address
            and disbursement_resolution_financial_address.mapper_resolved_name
        ):
            beneficiary_name = disbursement_resolution_financial_address.mapper_resolved_name
        else:
            _logger.warning(
                f"Disbursement {disbursement.id} has no beneficiary name or resolved name, using 'N/A'"
            )

        disbursement_payment_payloads.append(
            DisbursementPaymentPayload(
                disbursement_id=disbursement.id,
                remitting_account=sponsor_bank_configuration.program_account_number,
                remitting_account_currency=envelope.measurement_unit,
                remitting_account_type=sponsor_bank_configuration.program_account_type,
                remitting_account_branch_code=sponsor_bank_configuration.program_account_branch_code,
                payment_amount=disbursement.disbursement_quantity,
                compute_elements=disbursement.compute_elements,
                funds_blocked_reference_number=envelope_batch_status_for_digital_cash.funds_blocked_reference_number,
                beneficiary_account=(
                    disbursement_resolution_financial_address.bank_account_number
                    if disbursement_resolution_financial_address
                    else None
                ),
                beneficiary_account_currency=envelope.measurement_unit,
                beneficiary_bank_code=(
                    disbursement_resolution_financial_address.bank_code
                    if disbursement_resolution_financial_address
                    else None
                ),
                beneficiary_branch_code=(
                    disbursement_resolution_financial_address.branch_code
                    if disbursement_resolution_financial_address
                    else None
                ),
                payment_date=str(datetime.date(datetime.now())),
                beneficiary_id=disbursement.beneficiary_id,
                beneficiary_name=beneficiary_name,
                beneficiary_account_type=disbursement_resolution_financial_address.mapper_resolved_fa_type,
                beneficiary_phone_no=(
                    disbursement_resolution_financial_address.mobile_number
                    if disbursement_resolution_financial_address
                    else None
                ),
                beneficiary_mobile_wallet_provider=(
                    disbursement_resolution_financial_address.mobile_wallet_provider
                    if disbursement_resolution_financial_address
                    else None
                ),
                beneficiary_email_wallet_provider=(
                    disbursement_resolution_financial_address.email_wallet_provider
                    if disbursement_resolution_financial_address
                    else None
                ),
                beneficiary_email=(
                    disbursement_resolution_financial_address.email_address
                    if disbursement_resolution_financial_address
                    else None
                ),
                disbursement_narrative=disbursement.narrative,
                benefit_program_mnemonic=envelope.benefit_program_mnemonic,
                cycle_code_mnemonic=envelope.cycle_code_mnemonic,
            )
        )

    _logger.info(
        f"Digital cash disbursement payloads constructed: {len(disbursement_payment_payloads)} payloads"
    )
    return disbursement_payment_payloads, zero_quantity_reconciled_count


def construct_disbursement_payloads_for_physical_cash(
    disbursement_batch_control_id,
    session,
    envelope,
    envelope_batch_status_for_digital_cash,
    sponsor_bank_configuration,
) -> tuple[List[DisbursementPaymentPayload], int]:
    _logger.info(f"Constructing disbursement payloads for physical cash: {disbursement_batch_control_id}")
    disbursement_batch_control_geos: List[DisbursementBatchControlGeo] = (
        session.query(DisbursementBatchControlGeo)
        .filter(DisbursementBatchControlGeo.disbursement_batch_control_id == disbursement_batch_control_id)
        .all()
    )

    disbursement_payloads = []
    zero_quantity_reconciled_count = 0

    for disbursement_batch_control_geo in disbursement_batch_control_geos:
        if disbursement_batch_control_geo.total_quantity <= 0:
            _logger.warning(
                f"Skipping disbursement for geo {disbursement_batch_control_geo.id} with non-positive quantity"
            )
            # Add to disbursement recon
            disbursement_recon = DisbursementRecon(
                disbursement_batch_control_id=disbursement_batch_control_geo.disbursement_batch_control_id,
                disbursement_id=None,
                disbursement_batch_control_geo_id=disbursement_batch_control_geo.id,
                disbursement_envelope_id=envelope.id,
            )
            session.add(disbursement_recon)
            zero_quantity_reconciled_count += 1
            continue
        agency_detail_for_payment: (
            AgencyDetailForPayment
        ) = AgencyHelper.get_component().retrieve_agency_details(
            disbursement_batch_control_geo.agency_id,
            envelope.benefit_program_id,
            envelope.benefit_code_id,
        )
        # Fetch agency_admin_email and agency_admin_phone from DisbursementBatchControlGeoAttributes
        disbursement_batch_control_geo_attributes = (
            session.query(DisbursementBatchControlGeoAttributes)
            .filter(
                DisbursementBatchControlGeoAttributes.disbursement_batch_control_id
                == disbursement_batch_control_geo.id
            )
            .first()
        )
        agency_email_address = (
            disbursement_batch_control_geo_attributes.agency_admin_email
            if disbursement_batch_control_geo_attributes
            else None
        )
        agency_phone_number = (
            disbursement_batch_control_geo_attributes.agency_admin_phone
            if disbursement_batch_control_geo_attributes
            else None
        )

        disbursement_payload = DisbursementPaymentPayload(
            disbursement_id=disbursement_batch_control_geo.id,
            remitting_account=sponsor_bank_configuration.program_account_number,
            remitting_account_currency=envelope.measurement_unit,
            remitting_account_type=sponsor_bank_configuration.program_account_type,
            remitting_account_branch_code=sponsor_bank_configuration.program_account_branch_code,
            payment_amount=disbursement_batch_control_geo.total_quantity,
            funds_blocked_reference_number=envelope_batch_status_for_digital_cash.funds_blocked_reference_number,
            beneficiary_account=agency_detail_for_payment.agency_account_number,
            beneficiary_account_currency=envelope.measurement_unit,
            beneficiary_bank_code=agency_detail_for_payment.agency_account_bank_code,
            beneficiary_branch_code=agency_detail_for_payment.agency_account_branch_code,
            payment_date=str(datetime.date(datetime.now())),
            beneficiary_id=disbursement_batch_control_geo.agency_id,
            beneficiary_name=agency_detail_for_payment.agency_name,
            beneficiary_account_type=(
                agency_detail_for_payment.agency_account_type
                if agency_detail_for_payment.agency_account_type
                else "BANK_ACCOUNT"
            ),  # TODO: Check this property
            beneficiary_phone_no=agency_phone_number,
            beneficiary_mobile_wallet_provider=None,
            beneficiary_email_wallet_provider=None,
            beneficiary_email=agency_email_address,
            disbursement_narrative="PAYMENT_TO_AGENCY_FOR_PHYSICAL_CASH_DISTRIBUTION",
            benefit_program_mnemonic=envelope.benefit_program_mnemonic,
            cycle_code_mnemonic=envelope.cycle_code_mnemonic,
        )
        disbursement_payloads.append(disbursement_payload)

    _logger.info(f"Physical cash disbursement payloads constructed: {len(disbursement_payloads)} payloads")
    return disbursement_payloads, zero_quantity_reconciled_count
