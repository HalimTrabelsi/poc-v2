import logging
import random
import time
from datetime import datetime
from typing import List

import mt940
from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface.bank_connector_interface import (
    BankConnectorInterface,
)
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.models import (
    AccountStatement,
    AccountStatementLob,
    Disbursement,
    DisbursementBatchControlGeo,
    DisbursementErrorRecon,
    DisbursementRecon,
    EnvelopeBatchStatusForCash,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import SponsorBankConfiguration
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine
from ..helpers import WarehouseHelper

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mt940_processor_worker")
def mt940_processor_worker(statement_id: str):
    _logger.info(f"Processing account statement with statement_id: {statement_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)

    with session_maker() as session:
        account_statement = (
            session.query(AccountStatement).filter(AccountStatement.statement_id == statement_id).first()
        )

        if not account_statement:
            return

        lob = (
            session.query(AccountStatementLob)
            .filter(AccountStatementLob.statement_id == statement_id)
            .first()
        )

        if not lob:
            return

        try:
            # Parsing header section
            account_number_parser = mt940.tags.AccountIdentification()
            statement_number_parser = mt940.tags.StatementNumber()
            transaction_reference_parser = mt940.tags.TransactionReferenceNumber()

            statement_parser = mt940.tags.Statement()
            _logger.info(f"Parsing MT940 statement for statement id: {statement_id}")
            mt940_statement = mt940.models.Transactions(
                processors={
                    "pre_statement": [mt940.processors.add_currency_pre_processor("")],
                },
                tags={
                    account_number_parser.id: account_number_parser,
                    statement_number_parser.id: statement_number_parser,
                    transaction_reference_parser.id: transaction_reference_parser,
                    statement_parser.id: statement_parser,
                },
            )

            mt940_statement.parse(lob.statement_lob)

            account_statement.account_number = mt940_statement.data.get("account_identification", "")
            account_statement.reference_number = mt940_statement.data.get("transaction_reference", "")
            account_statement.statement_number = mt940_statement.data.get("statement_number", "")
            account_statement.sequence_number = mt940_statement.data.get("sequence_number", "")
            _logger.info("Parsed account statement header")
            _logger.info(
                "Account number: %s, Reference number: %s, Statement number: %s, Sequence number: %s",
                account_statement.account_number,
                account_statement.reference_number,
                account_statement.statement_number,
                account_statement.sequence_number,
            )

            # Get the benefit program configuration
            sponsor_bank_configuration: (
                SponsorBankConfiguration
            ) = WarehouseHelper.get_component().retrieve_sponsor_bank_configuration_for_account_number(
                account_statement.account_number
            )

            if not sponsor_bank_configuration:
                _logger.error(
                    f"No SponsorBankConfiguration found for account number: {account_statement.account_number}"
                )

                account_statement.statement_process_status = ProcessStatus.ERROR.value
                account_statement.statement_process_error_code = (
                    G2PBridgeErrorCodes.INVALID_ACCOUNT_NUMBER.value
                )
                account_statement.statement_process_timestamp = datetime.now()
                account_statement.statement_process_attempts += 1
                session.commit()
                return

            bank_connector: BankConnectorInterface = BankConnectorFactory.get_component().get_bank_connector(
                sponsor_bank_configuration.sponsor_bank_code
            )

            # Parsing transactions
            parsed_transactions_d = []
            parsed_transactions_rd = []
            entry_sequence = 0
            for transaction in mt940_statement:
                entry_sequence += 1
                debit_credit_indicator = transaction.data["status"]
                _logger.info(f"Debit/Credit Indicator:{transaction.data['status']}")
                if debit_credit_indicator in ["D"]:
                    parsed_transaction = construct_parsed_transaction(
                        bank_connector,
                        debit_credit_indicator,
                        entry_sequence,
                        transaction,
                        session,
                    )
                    parsed_transactions_d.append(parsed_transaction)

                if debit_credit_indicator in ["RD"]:
                    parsed_transaction = construct_parsed_transaction(
                        bank_connector,
                        debit_credit_indicator,
                        entry_sequence,
                        transaction,
                        session,
                    )
                    parsed_transactions_rd.append(parsed_transaction)

            # End of for loop of mt940 statement transactions
            disbursement_error_recons = []
            disbursement_recons_d = []

            # Process only debit transactions
            process_debit_transactions(
                account_statement,
                disbursement_error_recons,
                disbursement_recons_d,
                parsed_transactions_d,
                session,
                statement_id,
            )

            # Start processing reversal transactions - rd
            disbursement_recons_rd = []
            process_reversal_of_debits(
                account_statement,
                disbursement_error_recons,
                disbursement_recons_rd,
                parsed_transactions_rd,
                session,
                statement_id,
            )

            update_envelope_batch_status_reconciled(disbursement_recons_d, session)
            update_envelope_batch_status_reversed(disbursement_recons_rd, session)

            session.add_all(disbursement_error_recons)

            # Update account statement with parsed data
            account_statement.statement_process_status = ProcessStatus.PROCESSED.value
            account_statement.statement_process_error_code = None
            account_statement.statement_process_timestamp = datetime.now()
            account_statement.statement_process_attempts += 1

            session.add(account_statement)

            session.commit()
            _logger.info(
                f"Processed account statement for account number: {account_statement.account_number}"
            )

        except Exception as e:
            _logger.error(
                f"Error processing account statement for statement id: {statement_id} with error: {str(e)}",
            )
            account_statement.statement_process_error_code = str(e)
            account_statement.statement_process_timestamp = datetime.now()
            account_statement.statement_process_attempts += 1
            if account_statement.statement_process_attempts > _config.mt940_processor_max_attempts:
                account_statement.statement_process_status = ProcessStatus.ERROR.value
            else:
                account_statement.statement_process_status = ProcessStatus.PENDING.value
            session.commit()


def process_reversal_of_debits(
    account_statement,
    disbursement_error_recons,
    disbursement_recons_rd,
    parsed_transactions_rd,
    session,
    statement_id,
):
    _logger.info(f"Processing reversal of debits for statement: {statement_id}")
    for parsed_transaction in parsed_transactions_rd:
        disbursement: Disbursement | None = check_valid_disbursement_id(parsed_transaction, session)
        disbursement_batch_control_geo: DisbursementBatchControlGeo | None = None
        _logger.info(f"Disbursement ID from parsed_transaction: {disbursement}")

        if not disbursement:
            disbursement_batch_control_geo = check_valid_disbursement_batch_control_geo_id(
                parsed_transaction, session
            )
            if not disbursement_batch_control_geo:
                disbursement_error_recons.append(
                    construct_disbursement_error_recon(
                        statement_id,
                        account_statement.statement_number,
                        account_statement.sequence_number,
                        parsed_transaction,
                        G2PBridgeErrorCodes.INVALID_RECONCILIATION_ID,
                    )
                )
                continue

        disbursement_recon = get_disbursement_recon(parsed_transaction, session)

        if not disbursement_recon:
            disbursement_error_recons.append(
                construct_disbursement_error_recon(
                    statement_id,
                    account_statement.statement_number,
                    account_statement.sequence_number,
                    parsed_transaction,
                    G2PBridgeErrorCodes.INVALID_REVERSAL,
                )
            )
        else:
            update_existing_disbursement_recon(
                disbursement_recon,
                parsed_transaction,
                statement_id,
                account_statement.statement_number,
                account_statement.sequence_number,
            )
            session.add(disbursement_recon)
            disbursement_recons_rd.append(disbursement_recon)


def process_debit_transactions(
    account_statement,
    disbursement_error_recons,
    disbursement_recons_d,
    parsed_transactions_d,
    session,
    statement_id,
):
    _logger.info(f"Processing debit transactions for statement: {statement_id}")
    for parsed_transaction in parsed_transactions_d:
        disbursement: Disbursement | None = check_valid_disbursement_id(parsed_transaction, session)
        disbursement_batch_control_geo: DisbursementBatchControlGeo | None = None
        _logger.info(f"Disbursement ID from parsed_transaction: {disbursement}")
        if not disbursement:
            disbursement_batch_control_geo = check_valid_disbursement_batch_control_geo_id(
                parsed_transaction, session
            )
            if not disbursement_batch_control_geo:
                disbursement_error_recons.append(
                    construct_disbursement_error_recon(
                        statement_id,
                        account_statement.statement_number,
                        account_statement.sequence_number,
                        parsed_transaction,
                        G2PBridgeErrorCodes.INVALID_RECONCILIATION_ID,
                    )
                )
                continue

        disbursement_recon = get_disbursement_recon(parsed_transaction, session)

        if disbursement_recon:
            disbursement_error_recons.append(
                construct_disbursement_error_recon(
                    statement_id,
                    account_statement.statement_number,
                    account_statement.sequence_number,
                    parsed_transaction,
                    G2PBridgeErrorCodes.DUPLICATE_DISBURSEMENT,
                )
            )
            continue

        disbursement_recon = construct_new_disbursement_recon(
            disbursement,
            disbursement_batch_control_geo,
            parsed_transaction,
            statement_id,
            account_statement.statement_number,
            account_statement.sequence_number,
            session,
        )
        session.add(disbursement_recon)
        disbursement_recons_d.append(disbursement_recon)


def get_disbursement_recon(parsed_transaction, session):
    _logger.info(
        f"Looking up DisbursementRecon for reconciliation_id: {parsed_transaction['reconciliation_id']}"
    )
    disbursement_recon = (
        session.query(DisbursementRecon)
        .filter(DisbursementRecon.disbursement_id == parsed_transaction["reconciliation_id"])
        .first()
    )
    if not disbursement_recon:
        disbursement_recon = (
            session.query(DisbursementRecon)
            .filter(
                DisbursementRecon.disbursement_batch_control_geo_id == parsed_transaction["reconciliation_id"]
            )
            .first()
        )
    _logger.info(f"DisbursementRecon found: {disbursement_recon}")
    return disbursement_recon


def check_valid_disbursement_id(parsed_transaction, session) -> Disbursement | None:
    # Look up the Disbursement by disbursement_id
    disbursement: Disbursement = (
        session.query(Disbursement).filter(Disbursement.id == parsed_transaction["reconciliation_id"]).first()
    )
    if not disbursement:
        return None
    return disbursement


def check_valid_disbursement_batch_control_geo_id(
    parsed_transaction, session
) -> DisbursementBatchControlGeo | None:
    # Look up the DisbursementBatchControlGeo by disbursement_id
    disbursement_batch_control_geo: DisbursementBatchControlGeo = (
        session.query(DisbursementBatchControlGeo)
        .filter(DisbursementBatchControlGeo.id == parsed_transaction["reconciliation_id"])
        .first()
    )
    if not disbursement_batch_control_geo:
        return None
    return disbursement_batch_control_geo


def construct_disbursement_error_recon(
    statement_id,
    statement_number,
    statement_sequence,
    parsed_transaction,
    g2p_bridge_error_code,
):
    return DisbursementErrorRecon(
        statement_id=statement_id,
        statement_number=statement_number,
        statement_sequence=statement_sequence,
        entry_sequence=parsed_transaction["remittance_entry_sequence"],
        entry_date=parsed_transaction["remittance_entry_date"],
        value_date=parsed_transaction["remittance_value_date"],
        error_reason=g2p_bridge_error_code.value,
        reconciliation_id=parsed_transaction["reconciliation_id"],
        bank_reference_number=parsed_transaction["remittance_reference_number"],
    )


def update_existing_disbursement_recon(
    disbursement_recon,
    parsed_transaction,
    statement_id,
    statement_number,
    statement_sequence,
):
    disbursement_recon.reversal_found = True
    disbursement_recon.reversal_statement_id = statement_id
    disbursement_recon.reversal_statement_number = statement_number
    disbursement_recon.reversal_statement_sequence = statement_sequence
    disbursement_recon.reversal_entry_sequence = parsed_transaction["reversal_entry_sequence"]
    disbursement_recon.reversal_entry_date = parsed_transaction["reversal_entry_date"]
    disbursement_recon.reversal_value_date = parsed_transaction["reversal_value_date"]
    disbursement_recon.reversal_reason = parsed_transaction["reversal_reason"]


def construct_new_disbursement_recon(
    disbursement,
    disbursement_batch_control_geo,
    parsed_transaction,
    statement_id,
    statement_number,
    statement_sequence,
    session,
):
    if disbursement:
        _logger.info(f"Disbursement ID For Recon: {disbursement.id}")
    disbursement_recon = DisbursementRecon(
        # If disbursement is present, then it is for DIGITAL CASH
        disbursement_batch_control_id=disbursement.disbursement_batch_control_id if disbursement else None,
        disbursement_id=disbursement.id if disbursement else None,
        # If disbursement_batch_control_geo is present, then it is for PHYSICAL CASH
        # PHYSICAL CASH means transfer for agency accounts
        disbursement_batch_control_geo_id=(
            disbursement_batch_control_geo.id if disbursement_batch_control_geo else None
        ),
        disbursement_envelope_id=get_disbursement_envelope_id(
            parsed_transaction["reconciliation_id"], session
        ),
        beneficiary_name_from_bank=parsed_transaction["beneficiary_name_from_bank"],
        remittance_reference_number=parsed_transaction["remittance_reference_number"],
        remittance_statement_id=statement_id,
        remittance_statement_number=statement_number,
        remittance_statement_sequence=statement_sequence,
        remittance_entry_sequence=parsed_transaction["remittance_entry_sequence"],
        remittance_entry_date=parsed_transaction["remittance_entry_date"],
        remittance_value_date=parsed_transaction["remittance_value_date"],
    )
    return disbursement_recon


def construct_parsed_transaction(
    bank_connector, debit_credit_indicator, entry_sequence, transaction, session
) -> dict:
    parsed_transaction = {}
    transaction_amount = transaction.data["amount"].amount
    customer_reference = transaction.data["customer_reference"]
    remittance_reference_number = transaction.data["bank_reference"]
    narratives = transaction.data["transaction_details"].split("\n")
    reconciliation_id = bank_connector.retrieve_reconciliation_id(
        remittance_reference_number, customer_reference, narratives
    )
    beneficiary_name_from_bank = None
    remittance_entry_sequence = None
    remittance_entry_date = None
    remittance_value_date = None

    reversal_found = False
    reversal_entry_sequence = None
    reversal_entry_date = None
    reversal_value_date = None
    reversal_reason = None

    if debit_credit_indicator == "D":
        reversal_found = False
        beneficiary_name_from_bank = bank_connector.retrieve_beneficiary_name(narratives)
        remittance_entry_sequence = entry_sequence
        remittance_entry_date = transaction.data["entry_date"]
        remittance_value_date = transaction.data["date"]

    if debit_credit_indicator == "RD":
        reversal_found = True
        reversal_entry_sequence = entry_sequence
        reversal_entry_date = transaction.data["entry_date"]
        reversal_value_date = transaction.data["date"]
        reversal_reason = bank_connector.retrieve_reversal_reason(narratives)

    parsed_transaction.update(
        {
            "reconciliation_id": reconciliation_id,
            "transaction_amount": transaction_amount,
            "debit_credit_indicator": debit_credit_indicator,
            "beneficiary_name_from_bank": beneficiary_name_from_bank,
            "remittance_reference_number": remittance_reference_number,
            "remittance_entry_sequence": remittance_entry_sequence,
            "remittance_entry_date": remittance_entry_date,
            "remittance_value_date": remittance_value_date,
            "reversal_found": reversal_found,
            "reversal_entry_sequence": reversal_entry_sequence,
            "reversal_entry_date": reversal_entry_date,
            "reversal_value_date": reversal_value_date,
            "reversal_reason": reversal_reason,
        }
    )
    return parsed_transaction


def get_disbursement_envelope_id(disbursement_id, session):
    disbursement = session.query(Disbursement).filter(Disbursement.id == disbursement_id).first()

    if disbursement:
        return disbursement.disbursement_envelope_id
    else:
        disbursement_batch_control_geo = (
            session.query(DisbursementBatchControlGeo)
            .filter(DisbursementBatchControlGeo.id == disbursement_id)
            .first()
        )
        return (
            disbursement_batch_control_geo.disbursement_envelope_id
            if disbursement_batch_control_geo
            else None
        )


def update_envelope_batch_status_reconciled(disbursement_recons: List[DisbursementRecon], session):
    # Count how many reversals per envelope
    disbursement_envelope_id_count = {}
    for disbursement_recon in disbursement_recons:
        eid = disbursement_recon.disbursement_envelope_id
        disbursement_envelope_id_count[eid] = disbursement_envelope_id_count.get(eid, 0) + 1

    # Update each envelope, retrying on lock conflicts
    for envelope_id, count in disbursement_envelope_id_count.items():
        max_retries = 5
        last_exc = None

        while max_retries:
            try:
                envelope_batch_status_for_cash = (
                    session.query(EnvelopeBatchStatusForCash)
                    .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == envelope_id)
                    .with_for_update(nowait=True)
                    .populate_existing()
                    .first()
                )
                break

            except OperationalError as e:
                last_exc = e
                wait = random.randint(8, 15)
                _logger.warning(
                    f"Lock attempt failed for envelope {envelope_id}: {e}. "
                    f"{max_retries} retries left, sleeping {wait}s…"
                )
                session.rollback()
                time.sleep(wait)
                max_retries -= 1

        else:
            _logger.error(f"Could not acquire lock for envelope {envelope_id} after retries")
            raise last_exc

        envelope_batch_status_for_cash.number_of_disbursements_reconciled += count
        session.add(envelope_batch_status_for_cash)
        session.commit()


def update_envelope_batch_status_reversed(disbursement_recons: List[DisbursementRecon], session):
    # Get the unique disbursement envelope ids and count of disbursements
    disbursement_envelope_id_count = {}
    for disbursement_recon in disbursement_recons:
        if disbursement_recon.disbursement_envelope_id in disbursement_envelope_id_count:
            disbursement_envelope_id_count[disbursement_recon.disbursement_envelope_id] += 1
        else:
            disbursement_envelope_id_count[disbursement_recon.disbursement_envelope_id] = 1

    # Update the disbursement envelope batch status
    for disbursement_envelope_id, count in disbursement_envelope_id_count.items():
        max_retries = 5
        last_exc = None

        while max_retries:
            try:
                envelope_batch_status_for_cash = (
                    session.query(EnvelopeBatchStatusForCash)
                    .filter(EnvelopeBatchStatusForCash.disbursement_envelope_id == disbursement_envelope_id)
                    .with_for_update(nowait=True)
                    .populate_existing()
                    .first()
                )
                break

            except OperationalError as e:
                last_exc = e
                wait = random.randint(8, 15)
                _logger.warning(
                    f"Lock attempt failed for envelope {disbursement_envelope_id}: {e}. "
                    f"{max_retries} retries left, sleeping {wait}s…"
                )
                session.rollback()
                time.sleep(wait)
                max_retries -= 1

        else:
            _logger.error(f"Could not acquire lock for envelope {disbursement_envelope_id} after retries")
            raise last_exc

        envelope_batch_status_for_cash.number_of_disbursements_reversed += count
        session.add(envelope_batch_status_for_cash)
        session.commit()
