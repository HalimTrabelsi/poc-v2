import logging
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from openg2p_g2p_bridge_celery_workers.tasks.mt940_processor import (
    construct_parsed_transaction,
    get_disbursement_envelope_id,
    mt940_processor_worker,
    process_debit_transactions,
    process_reversal_of_debits,
    update_envelope_batch_status_reconciled,
    update_envelope_batch_status_reversed,
)
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.models import (
    AccountStatement,
    AccountStatementLob,
    BenefitType,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelope,
    DisbursementFrequency,
    DisbursementRecon,
    EnvelopeBatchStatusForCash,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import SponsorBankConfiguration


class MockSession:
    def __init__(self):
        self.committed = False
        self.flushed = False
        self.added = False
        self.account_statement = AccountStatement(
            statement_id="test_statement_id",
            account_number="test_account_number",
            statement_process_status=ProcessStatus.PENDING.value,
            statement_process_attempts=0,
        )
        self.account_statement_lob = AccountStatementLob(
            statement_id="test_statement_id",
            statement_lob="""
            :20:1234567890
            :25:12345678901234567890
            :28C:123/1
            :60F:C000000000000,00
            :61:2012123456789,00DTRFREF123//123456789
            BENEFICIARY/123456789
            :86:PAYMENT TO BENEFICIARY
            :62F:C000000000000,00
            """,
        )
        self.benefit_program_configuration = SponsorBankConfiguration(
            program_account_number="test_account_number",
            program_account_type=None,
            program_account_branch_code="test_branch",
            sponsor_bank_code="test_bank",
        )
        self.disbursement = Disbursement(
            id="test_disbursement_id",
            disbursement_envelope_id="test_envelope_id",
            beneficiary_id="test_beneficiary_id",
            beneficiary_name="Test Beneficiary",
            disbursement_quantity=100.0,
            narrative="Test disbursement",
            disbursement_cycle_id=1,
            disbursement_batch_control_id="test_batch_control_id",
        )
        self.disbursement_envelope = DisbursementEnvelope(
            id="test_envelope_id",
            benefit_program_mnemonic="test_program",
            benefit_code_id=1,
            benefit_type=BenefitType.CASH_DIGITAL,
            disbursement_cycle_id=1,
            disbursement_frequency=DisbursementFrequency.Monthly,
            cycle_code_mnemonic="test_cycle_mnemonic",
            number_of_beneficiaries=10,
            number_of_disbursements=10,
            total_disbursement_quantity=1000,
            measurement_unit="KES",
            disbursement_schedule_date=date.today(),
        )
        self.disbursement_envelope_batch_status = EnvelopeBatchStatusForCash(
            disbursement_envelope_id="test_envelope_id",
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value,
            number_of_disbursements_reconciled=0,
            number_of_disbursements_reversed=0,
        )
        self.disbursement_recon = None
        self.disbursement_batch_control = DisbursementBatchControl(
            id="test_batch_control_id",
            disbursement_cycle_id=1,
            disbursement_envelope_id="test_envelope_id",
            fa_resolution_status=ProcessStatus.PENDING.value,
            sponsor_bank_dispatch_status=ProcessStatus.PENDING.value,
            geo_resolution_status=ProcessStatus.PENDING.value,
            warehouse_allocation_status=ProcessStatus.PENDING.value,
            agency_allocation_status=ProcessStatus.PENDING.value,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def query(self, *args):
        self.query_args = args
        return self

    def filter(self, *args):
        self.filter_args = args
        return self

    def first(self):
        if self.query_args[0] is AccountStatement:
            return self.account_statement
        elif self.query_args[0] is AccountStatementLob:
            return self.account_statement_lob
        elif self.query_args[0] is SponsorBankConfiguration:
            return self.benefit_program_configuration
        elif self.query_args[0] is Disbursement:
            return self.disbursement
        elif self.query_args[0] is EnvelopeBatchStatusForCash:
            return self.disbursement_envelope_batch_status
        elif self.query_args[0] is DisbursementRecon:
            return self.disbursement_recon
        elif self.query_args[0] is DisbursementBatchControl:
            return self.disbursement_batch_control
        return None

    def with_for_update(self, nowait=False):
        return self

    def populate_existing(self):
        return self

    def add(self, obj):
        self.added = True
        pass

    def add_all(self, items):
        pass

    def commit(self):
        self.committed = True

    def flush(self):
        self.flushed = True
        if hasattr(self, "disbursement_envelope_batch_status"):
            self.disbursement_envelope_batch_status.number_of_disbursements_reversed = 2


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_bank_connector_factory():
    mock_bank_connector = MagicMock()
    mock_bank_factory = MagicMock()
    mock_bank_factory.get_bank_connector.return_value = mock_bank_connector

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.BankConnectorFactory.get_component",
        return_value=mock_bank_factory,
    ):
        yield mock_bank_connector


def test_mt940_processor_success(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.retrieve_disbursement_id.return_value = "test_disbursement_id"
    mock_bank_connector_factory.retrieve_beneficiary_name.return_value = "Test Beneficiary"
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration_for_account_number.return_value = (
        mock_session_maker.benefit_program_configuration
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        mt940_processor_worker("test_statement_id")

    assert mock_session_maker.account_statement.statement_process_status == ProcessStatus.PROCESSED.value
    assert mock_session_maker.account_statement.statement_process_error_code is None
    assert isinstance(mock_session_maker.account_statement.statement_process_timestamp, datetime)
    assert mock_session_maker.committed


def test_mt940_processor_invalid_account(mock_session_maker, mock_bank_connector_factory):
    mock_session_maker.benefit_program_configuration = None
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration_for_account_number.return_value = None
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        mt940_processor_worker("test_statement_id")

    assert mock_session_maker.account_statement.statement_process_status == ProcessStatus.ERROR.value
    assert (
        mock_session_maker.account_statement.statement_process_error_code
        == G2PBridgeErrorCodes.INVALID_ACCOUNT_NUMBER.value
    )
    assert isinstance(mock_session_maker.account_statement.statement_process_timestamp, datetime)
    assert mock_session_maker.committed


def test_mt940_processor_statement_not_found(mock_session_maker):
    mock_session_maker.account_statement = None

    mt940_processor_worker("test_statement_id")

    assert not mock_session_maker.committed


def test_mt940_processor_lob_not_found(mock_session_maker):
    mock_session_maker.account_statement_lob = None

    mt940_processor_worker("test_statement_id")

    assert not mock_session_maker.committed


def test_mt940_processor_exception(mock_session_maker, mock_bank_connector_factory, caplog):
    # Mock mt940.models.Transactions to raise an exception
    with patch("mt940.models.Transactions") as mock_transactions:
        mock_transactions.side_effect = Exception("TEST_ERROR")
        mock_warehouse_helper = MagicMock()
        mock_warehouse_helper.retrieve_sponsor_bank_configuration_for_account_number.return_value = (
            mock_session_maker.benefit_program_configuration
        )
        with patch(
            "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.WarehouseHelper.get_component",
            return_value=mock_warehouse_helper,
        ):
            with caplog.at_level(logging.ERROR):
                mt940_processor_worker("test_statement_id")

        assert "TEST_ERROR" in caplog.text
        assert mock_session_maker.account_statement.statement_process_status == ProcessStatus.PENDING.value
        assert mock_session_maker.account_statement.statement_process_error_code == "TEST_ERROR"
        assert isinstance(mock_session_maker.account_statement.statement_process_timestamp, datetime)
        assert mock_session_maker.committed


def test_get_disbursement_envelope_id_success(mock_session_maker):
    result = get_disbursement_envelope_id("test_disbursement_id", mock_session_maker)

    assert result == "test_envelope_id"


def test_get_disbursement_envelope_id_not_found(mock_session_maker):
    mock_session_maker.disbursement = None
    disbursement_envelope_id = get_disbursement_envelope_id("test_disbursement_id", mock_session_maker)

    assert disbursement_envelope_id is None


def test_construct_parsed_transaction(mock_session_maker, mock_bank_connector_factory):
    mock_transaction = MagicMock()
    mock_transaction.data = {
        "amount": MagicMock(amount=100),
        "customer_reference": "test_reference",
        "bank_reference": "test_bank_ref",
        "transaction_details": "test details",
        "entry_date": datetime.now(),
        "date": datetime.now(),
    }

    mock_bank_connector_factory.retrieve_disbursement_id.return_value = "test_disbursement_id"
    mock_bank_connector_factory.retrieve_beneficiary_name.return_value = "Test Beneficiary"

    result = construct_parsed_transaction(
        mock_bank_connector_factory, "D", 1, mock_transaction, mock_session_maker
    )
    # Ensure result contains all expected keys
    assert "disbursement_id" in result
    assert result["disbursement_id"] == "test_disbursement_id"
    assert result["disbursement_envelope_id"] == "test_envelope_id"
    assert result["transaction_amount"] == 100
    assert result["debit_credit_indicator"] == "D"
    assert result["beneficiary_name_from_bank"] == "Test Beneficiary"


def test_process_debit_transactions_success(mock_session_maker, mock_bank_connector_factory):
    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "test_disbursement_id",
            "reconciliation_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "disbursement_batch_control_id": "test_batch_control_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    process_debit_transactions(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_d,
        parsed_transactions_d,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_d) == 1
    assert len(disbursement_error_recons) == 0
    assert disbursement_recons_d[0].disbursement_id == "test_disbursement_id"


def test_process_debit_transactions_invalid_disbursement(mock_session_maker, mock_bank_connector_factory):
    # Set disbursement_batch_control to None for this test
    mock_session_maker.disbursement_batch_control = None

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "INVALID_ID",  # Set to an invaild id for this test
            "reconciliation_id": "INVALID_ID",
            "disbursement_envelope_id": "test_envelope_id",
            "disbursement_batch_control_id": "test_batch_control_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    # Remove patch for get_bank_batch_id (does not exist)
    process_debit_transactions(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_d,
        parsed_transactions_d,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_d) == 0
    assert len(disbursement_error_recons) == 1
    assert disbursement_error_recons[0].error_reason == G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID


def test_process_debit_transactions_duplicate(mock_session_maker):
    # Add a mock DisbursementRecon to simulate duplicate
    mock_session_maker.disbursement_recon = DisbursementRecon(
        disbursement_id="test_disbursement_id",
        remittance_statement_id="test_statement_id",
        disbursement_envelope_id="test_envelope_id",
        remittance_reference_number="test_ref",
        remittance_entry_sequence=1,
        remittance_entry_date=datetime.now(),
        remittance_value_date=datetime.now(),
    )

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "test_disbursement_id",
            "reconciliation_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "disbursement_batch_control_id": "test_batch_control_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    process_debit_transactions(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_d,
        parsed_transactions_d,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_d) == 0
    assert len(disbursement_error_recons) == 1
    assert disbursement_error_recons[0].error_reason == G2PBridgeErrorCodes.DUPLICATE_DISBURSEMENT


def test_process_reversal_of_debits_success(mock_session_maker):
    # Add existing DisbursementRecon to mock
    mock_session_maker.disbursement_recon = DisbursementRecon(
        disbursement_id="test_disbursement_id",
        disbursement_envelope_id="test_envelope_id",
    )

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_rd = []
    parsed_transactions_rd = [
        {
            "disbursement_id": "test_disbursement_id",
            "reconciliation_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "disbursement_batch_control_id": "test_batch_control_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "RD",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
            "reversal_entry_sequence": 1,
            "reversal_entry_date": datetime.now(),
            "reversal_value_date": datetime.now(),
            "reversal_reason": "TEST_REASON",
        }
    ]

    process_reversal_of_debits(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_rd,
        parsed_transactions_rd,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_rd) == 1
    assert len(disbursement_error_recons) == 0
    assert disbursement_recons_rd[0].disbursement_id == "test_disbursement_id"


def test_update_envelope_batch_status_reconciled(mock_session_maker):
    disbursement_recons = [
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_1",
        ),
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_2",
        ),
    ]

    update_envelope_batch_status_reconciled(disbursement_recons, mock_session_maker)

    assert mock_session_maker.disbursement_envelope_batch_status.number_of_disbursements_reconciled == 2
    assert mock_session_maker.added
    assert mock_session_maker.committed


def test_update_envelope_batch_status_reversed(mock_session_maker):
    disbursement_recons = [
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_1",
            remittance_reference_number="test_ref_1",
            remittance_entry_sequence=1,
            remittance_entry_date=datetime.now(),
            remittance_value_date=datetime.now(),
        ),
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_2",
            remittance_reference_number="test_ref_2",
            remittance_entry_sequence=2,
            remittance_entry_date=datetime.now(),
            remittance_value_date=datetime.now(),
        ),
    ]

    update_envelope_batch_status_reversed(disbursement_recons, mock_session_maker)

    assert mock_session_maker.disbursement_envelope_batch_status.number_of_disbursements_reversed == 2
    assert mock_session_maker.added
