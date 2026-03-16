from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from openg2p_g2p_bridge_bank_connectors.bank_interface import BlockFundsResponse
from openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank import (
    block_funds_with_bank_worker,
)
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    DisbursementEnvelope,
    DisbursementFrequency,
    EnvelopeBatchStatusForCash,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from openg2p_g2p_bridge_models.schemas.payment_schemas import SponsorBankConfiguration


class MockSession:
    def __init__(self):
        self.committed = False
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
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.PENDING_CHECK.value,
            funds_blocked_attempts=0,
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

    def first(self):
        if self.query_args[0] is DisbursementEnvelope:
            return self.disbursement_envelope

        elif self.query_args[0] is EnvelopeBatchStatusForCash:
            return self.disbursement_envelope_batch_status

        return None

    def commit(self):
        self.committed = True

    def close(self):
        pass


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_bank_connector_factory():
    mock_bank_connector = MagicMock()
    mock_bank_factory = MagicMock()
    mock_bank_factory.get_bank_connector.return_value = mock_bank_connector

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.BankConnectorFactory.get_component",
        return_value=mock_bank_factory,
    ):
        yield mock_bank_connector


def test_block_funds_with_bank_success(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.block_funds.return_value = BlockFundsResponse(
        status=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
        block_reference_no="test_block_reference_no",
        error_code="",
    )
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        block_funds_with_bank_worker("test_envelope_id")

    assert (
        mock_session_maker.disbursement_envelope_batch_status.funds_blocked_with_bank
        == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value
    )
    assert mock_session_maker.committed


def test_block_funds_with_bank_failure(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.block_funds.return_value = BlockFundsResponse(
        status=FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE,
        block_reference_no="",
        error_code="TEST_ERROR",
    )
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        block_funds_with_bank_worker("test_envelope_id")

    assert mock_session_maker.disbursement_envelope_batch_status.funds_blocked_with_bank == "ERROR"
    assert mock_session_maker.committed


def test_block_funds_with_bank_exception(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.block_funds.side_effect = Exception("TEST_EXCEPTION")
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        block_funds_with_bank_worker("test_envelope_id")

    assert mock_session_maker.disbursement_envelope_batch_status.funds_blocked_with_bank == "ERROR"
    assert (
        mock_session_maker.disbursement_envelope_batch_status.funds_blocked_latest_error_code
        == "TEST_EXCEPTION"
    )
    assert mock_session_maker.committed


def test_check_funds_with_bank_envelope_not_found(mock_session_maker, mock_bank_connector_factory):
    mock_session_maker.disbursement_envelope = None
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        block_funds_with_bank_worker("test_envelope_id")

    assert not mock_session_maker.committed


def test_check_funds_with_bank_envelope_batch_status_not_found(
    mock_session_maker, mock_bank_connector_factory
):
    mock_session_maker.disbursement_envelope_batch_status = None
    mock_warehouse_helper = MagicMock()
    mock_warehouse_helper.retrieve_sponsor_bank_configuration.return_value = SponsorBankConfiguration(
        program_account_number="test_account_number",
        program_account_type=None,
        program_account_branch_code="test_branch",
        sponsor_bank_code="EXAMPLE",
    )
    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.block_funds_with_bank.WarehouseHelper.get_component",
        return_value=mock_warehouse_helper,
    ):
        block_funds_with_bank_worker("test_envelope_id")

    assert not mock_session_maker.committed
