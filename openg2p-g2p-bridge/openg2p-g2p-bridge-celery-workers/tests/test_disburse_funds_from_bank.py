import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from openg2p_g2p_bridge_bank_connectors.bank_interface import (
    PaymentResponse,
    PaymentStatus,
)
from openg2p_g2p_bridge_celery_workers.tasks.disburse_funds_from_bank import (
    disburse_funds_from_bank_worker,
)
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelope,
    DisbursementFrequency,
    DisbursementResolutionFinancialAddress,
    EnvelopeBatchStatusForCash,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import SponsorBankConfiguration


class MockSession:
    def __init__(self):
        self.committed = False
        self.rollbacked = False
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
            disbursement_schedule_date=datetime.now().date(),
        )
        self.disbursement_envelope_batch_status = EnvelopeBatchStatusForCash(
            disbursement_envelope_id="test_envelope_id",
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
            funds_blocked_reference_number="test_block_ref",
            number_of_disbursements_shipped=0,
        )
        self.bank_disbursement_batch_status = EnvelopeBatchStatusForCash(
            disbursement_envelope_id="test_envelope_id",
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
            funds_blocked_reference_number="test_block_ref",
            number_of_disbursements_shipped=0,
        )
        self.benefit_program_configuration = SponsorBankConfiguration(
            program_account_number="test_account_number",
            program_account_type=None,
            program_account_branch_code="test_branch",
            sponsor_bank_code="EXAMPLE",
        )
        self.disbursement = Disbursement(
            id="test_disbursement_id",
            disbursement_envelope_id="test_envelope_id",
            beneficiary_id="test_beneficiary",
            beneficiary_name="Test Beneficiary",
            disbursement_quantity=100,
            narrative="Test payment",
            disbursement_cycle_id=1,
            disbursement_batch_control_id="test_batch_control_id",
        )
        self.disbursement_batch_status = DisbursementBatchControl(
            id="test_batch_control_id",
            disbursement_cycle_id=1,
            disbursement_envelope_id="test_envelope_id",
            fa_resolution_status=ProcessStatus.PROCESSED,
            sponsor_bank_dispatch_status=ProcessStatus.PENDING,
            geo_resolution_status=ProcessStatus.PROCESSED,
            warehouse_allocation_status=ProcessStatus.PROCESSED,
            agency_allocation_status=ProcessStatus.PROCESSED,
        )
        self.disbursement_resolution_financial_address = DisbursementResolutionFinancialAddress(
            disbursement_id="test_disbursement_id",
            bank_account_number="test_bank_account",
            bank_code="test_bank",
            branch_code="test_branch",
            mobile_number="1234567890",
            email_address="test@example.com",
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def query(self, *args):
        self.query_args = args
        return self

    def filter(self, *args):
        self.filter_args = args
        return self

    def one(self):
        return self.first()

    def first(self):
        # Handle DisbursementBatchControl queries
        if self.query_args[0] is DisbursementBatchControl:
            # Check if the filter is for the test_batch_id
            if (
                hasattr(self, "filter_args")
                and len(self.filter_args) > 0
                and hasattr(self.filter_args[0], "right")
                and getattr(self.filter_args[0].right, "value", None) == "test_batch_id"
            ):
                # Allow test to override for negative cases
                if hasattr(self, "bank_disbursement_batch_status"):
                    return self.bank_disbursement_batch_status
                return self.disbursement_batch_status
            else:
                return None
        if self.query_args[0] is DisbursementEnvelope:
            return self.disbursement_envelope
        elif self.query_args[0] is EnvelopeBatchStatusForCash:
            if hasattr(self.filter_args[0], "right") and self.filter_args[0].right.value == "test_batch_id":
                return self.bank_disbursement_batch_status
            else:
                return self.disbursement_envelope_batch_status
        elif self.query_args[0] is SponsorBankConfiguration:
            return self.benefit_program_configuration
        elif self.query_args[0] is DisbursementResolutionFinancialAddress:
            return self.disbursement_resolution_financial_address
        elif self.query_args[0] is Disbursement:
            return [self.disbursement]
        return None

    def all(self):
        if self.query_args[0] is DisbursementBatchControl:
            return [self.disbursement_batch_status]
        elif self.query_args[0] is Disbursement:
            return [self.disbursement]
        return []

    def with_for_update(self, nowait=False):
        return self

    def populate_existing(self):
        return self

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.disburse_funds_from_bank.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_bank_connector_factory():
    mock_bank_connector = MagicMock()
    mock_bank_factory = MagicMock()
    mock_bank_factory.get_bank_connector.return_value = mock_bank_connector

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.disburse_funds_from_bank.BankConnectorFactory.get_component",
        return_value=mock_bank_factory,
    ):
        yield mock_bank_connector


@pytest.fixture(autouse=True)
def patch_bank_connector_factory_global():
    mock_bank_connector = MagicMock()
    mock_bank_factory = MagicMock()
    mock_bank_factory.get_bank_connector.return_value = mock_bank_connector
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.disburse_funds_from_bank.BankConnectorFactory.get_component",
        return_value=mock_bank_factory,
    ):
        yield


def get_mock_warehouse_helper():
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    return mock_warehouse_helper


def test_disburse_funds_success(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.initiate_payment.return_value = PaymentResponse(
        status=PaymentStatus.SUCCESS,
        error_code="",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        disburse_funds_from_bank_worker("test_batch_id")

    assert (
        mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_status
        == ProcessStatus.PROCESSED.value
    )
    assert mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_latest_error_code is None
    assert mock_session_maker.disbursement_envelope_batch_status.number_of_disbursements_shipped == 1
    assert mock_session_maker.committed


def test_disburse_funds_failure(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.initiate_payment.return_value = PaymentResponse(
        status=PaymentStatus.ERROR,
        error_code="TEST_ERROR",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        disburse_funds_from_bank_worker("test_batch_id")

    assert (
        mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_status
        == ProcessStatus.PENDING.value
    )
    assert (
        mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_latest_error_code == "TEST_ERROR"
    )
    assert mock_session_maker.committed


def test_disburse_funds_exception(mock_session_maker, mock_bank_connector_factory, caplog):
    mock_bank_connector_factory.initiate_payment.side_effect = Exception("TEST_EXCEPTION")
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        with caplog.at_level(logging.ERROR):
            disburse_funds_from_bank_worker("test_batch_id")

    assert "TEST_EXCEPTION" in caplog.text
    assert (
        mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_status
        == ProcessStatus.PENDING.value
    )
    assert (
        mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_latest_error_code
        == "TEST_EXCEPTION"
    )
    assert mock_session_maker.disbursement_batch_status.sponsor_bank_dispatch_attempts == 1
    assert mock_session_maker.committed


def test_disburse_funds_batch_not_found(mock_session_maker):
    mock_session_maker.bank_disbursement_batch_status = None
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        disburse_funds_from_bank_worker("test_batch_id")

    assert not mock_session_maker.committed


def test_disburse_funds_envelope_not_found(mock_session_maker):
    mock_session_maker.disbursement_envelope = None
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        disburse_funds_from_bank_worker("test_batch_id")

    assert not mock_session_maker.committed


def test_disburse_funds_envelope_batch_status_not_found(mock_session_maker):
    mock_session_maker.disbursement_envelope_batch_status = None
    with patch(
        "openg2p_g2p_bridge_celery_workers.helpers.warehouse_helper.WarehouseHelper.get_component",
        return_value=get_mock_warehouse_helper(),
    ):
        disburse_funds_from_bank_worker("test_batch_id")

    assert not mock_session_maker.committed
