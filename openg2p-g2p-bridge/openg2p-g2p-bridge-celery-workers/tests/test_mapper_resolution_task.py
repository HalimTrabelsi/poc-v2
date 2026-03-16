from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task import (
    make_resolve_request,
    mapper_resolution_worker,
    process_and_store_resolution,
)
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelope,
    DisbursementFrequency,
    ProcessStatus,
)


class MockSession:
    def __init__(self):
        self.committed = False
        self.flushed = False
        self.details_list = []
        self.updates = []
        self.query_args = ()
        self.disbursement_batch_controls = [
            DisbursementBatchControl(
                id="test_batch_control_id",
                disbursement_cycle_id=1,
                disbursement_envelope_id="test_envelope_id",
                fa_resolution_status=ProcessStatus.PENDING,
                sponsor_bank_dispatch_status=ProcessStatus.PENDING,
                geo_resolution_status=ProcessStatus.PENDING,
                warehouse_allocation_status=ProcessStatus.PENDING,
                agency_allocation_status=ProcessStatus.PENDING,
            ),
        ]
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
        self.disbursements = [self.disbursement]
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def execute(self, *args):
        # Simulate SQLAlchemy select queries
        select_obj = args[0]
        model_cls = select_obj.column_descriptions[0]["type"]
        query_type = None
        if model_cls is DisbursementBatchControl:
            query_type = "batch_control"
        elif model_cls is Disbursement:
            query_type = "disbursement"

        class ScalarResult:
            def __init__(self, parent, query_type):
                self.parent = parent
                self.query_type = query_type

            def scalars(self):
                class AllResult:
                    def all(inner_self):
                        if self.query_type == "batch_control":
                            return self.parent.disbursement_batch_controls
                        elif self.query_type == "disbursement":
                            return [self.parent.disbursement]
                        return []

                    def first(inner_self):
                        if self.query_type == "batch_control":
                            if not self.parent.disbursement_batch_controls:
                                return None
                            return self.parent.disbursement_batch_controls[0]
                        elif self.query_type == "disbursement":
                            if not self.parent.disbursements:
                                return None
                            return self.parent.disbursements[0]
                        return None

                return AllResult()

            def first(self):
                # Return the first batch control or disbursement depending on context
                if hasattr(self, "disbursement_batch_controls") and self.disbursement_batch_controls:
                    return self.disbursement_batch_controls[0]
                if hasattr(self, "disbursements") and self.disbursements:
                    return self.disbursements[0]
                return None

        return ScalarResult(self, query_type)

    def scalars(self):
        return self

    def all(self):
        if not self.query_args:
            return []
        if self.query_args[0] is DisbursementBatchControl:
            return self.disbursement_batch_controls
        elif self.query_args[0] is Disbursement:
            return self.disbursements
        return []

    def query(self, *args):
        self.query_args = args
        return self

    def filter(self, *args):
        self.filter_args = args
        return self

    def update(self, *args, **kwargs):
        # Always append a dict to self.updates for both success and error paths
        for arg in args:
            if isinstance(arg, dict):
                self.updates.append(arg)
        if kwargs:
            self.updates.append(kwargs)
        return True

    def add_all(self, items):
        self.details_list.extend(items)

    def commit(self):
        self.committed = True

    def flush(self):
        self.flushed = True

    def first(self):
        if not self.query_args:
            return None
        if self.query_args[0] is DisbursementBatchControl:
            return self.disbursement_batch_controls[0]
        elif self.query_args[0] is Disbursement:
            return self.disbursements[0]
        return None


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_resolve_helper():
    # Use MagicMock for the helper and set async methods with AsyncMock
    mock_helper = MagicMock()
    mock_helper.create_jwt_token = AsyncMock(return_value="mocked_jwt_token")
    mock_helper.construct_single_resolve_request.return_value = MagicMock()
    mock_helper.construct_resolve_request.return_value = MagicMock(
        dict=MagicMock(return_value={"key": "value"})  # Mock the dict method
    )
    mock_helper.deconstruct_fa.return_value = {
        "mapper_resolved_fa_type": "BANK",
        "bank_account_number": "123",
        "bank_code": "ABC",
        "branch_code": "001",
    }

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.ResolveHelper.get_component",
        return_value=mock_helper,
    ):
        yield mock_helper


@pytest.fixture
def mock_resolve_client():
    mock_mapper_resolve_client = AsyncMock()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.MapperResolveClient",
        return_value=mock_mapper_resolve_client,
    ):
        yield mock_mapper_resolve_client


def test_mapper_resolution_worker_success(mock_session_maker, mock_resolve_helper, mock_resolve_client):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [
        MagicMock(
            id="test_beneficiary_id",
            fa="test_fa",
            account_provider_info=MagicMock(name="TEST_NAME"),
        )
    ]
    mock_resolve_client.resolve_request.return_value = mock_response
    mock_resolve_helper.deconstruct_fa.return_value = {
        "mapper_resolved_fa_type": "BANK",
        "bank_account_number": "123",
        "bank_code": "ABC",
        "branch_code": "001",
    }

    mock_resolve_helper.create_jwt_token.return_value = "mocked_jwt_token"

    # Only pass the batch control ID (session is handled by patching sessionmaker)
    mapper_resolution_worker("test_batch_control_id")

    assert len(mock_session_maker.details_list) != 0
    update_values = next(
        (item for item in mock_session_maker.updates if "fa_resolution_status" in item),
        None,
    )
    assert update_values is not None
    assert update_values["fa_resolution_status"] == ProcessStatus.PROCESSED
    assert isinstance(update_values["fa_resolution_timestamp"], datetime)
    assert update_values["fa_resolution_latest_error_code"] is None

    assert mock_session_maker.flushed
    assert mock_session_maker.committed


def test_mapper_resolution_worker_failure(mock_session_maker, mock_resolve_helper, mock_resolve_client):
    mock_resolve_client.resolve_request.side_effect = Exception("TEST_ERROR")

    mock_resolve_helper.create_jwt_token.return_value = "mocked_jwt_token"

    mapper_resolution_worker("test_batch_id")

    update_values = next(
        (item for item in mock_session_maker.updates if "fa_resolution_status" in item),
        None,
    )
    assert update_values is not None
    assert update_values["fa_resolution_status"] == ProcessStatus.PENDING
    assert "Failed to resolve the request: TEST_ERROR" in update_values["fa_resolution_latest_error_code"]

    assert mock_session_maker.committed


@pytest.mark.asyncio
async def test_make_resolve_request_success(mock_resolve_helper, mock_resolve_client):
    disbursements = [
        Disbursement(
            id="test_disbursement_id",
            disbursement_envelope_id="test_envelope_id",
            beneficiary_id="test_beneficiary_id",
            beneficiary_name="Test Beneficiary",
            disbursement_quantity=100.0,
            narrative="Test disbursement",
            disbursement_cycle_id=1,
            disbursement_batch_control_id="test_batch_control_id",
        )
    ]
    mock_response = "RESOLVE_RESPONSE"
    mock_resolve_client.resolve_request.return_value = mock_response

    response, error = await make_resolve_request(disbursements)
    assert response == mock_response
    assert error is None


@pytest.mark.asyncio
async def test_make_resolve_request_failure(mock_resolve_helper, mock_resolve_client):
    disbursements = [
        Disbursement(
            id="test_disbursement_id",
            disbursement_envelope_id="test_envelope_id",
            beneficiary_id="test_beneficiary_id",
            beneficiary_name="Test Beneficiary",
            disbursement_quantity=100.0,
            narrative="Test disbursement",
            disbursement_cycle_id=1,
            disbursement_batch_control_id="test_batch_control_id",
        )
    ]
    mock_resolve_client.resolve_request.side_effect = Exception("TEST_ERROR")

    mock_resolve_helper.create_jwt_token.return_value = "mocked_jwt_token"

    response, error_msg = await make_resolve_request(disbursements)
    assert response is None
    assert "TEST_ERROR" in error_msg


def test_process_and_store_resolution_success(mock_session_maker, mock_resolve_helper):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [
        MagicMock(
            id="test_beneficiary_id",
            fa="test_fa",
            account_provider_info=MagicMock(name="Test Name"),
        )
    ]
    beneficiary_map = {"test_beneficiary_id": "test_disbursement_id"}

    mock_resolve_helper.deconstruct_fa.return_value = {
        "mapper_resolved_fa_type": "BANK",
        "bank_account_number": "123",
        "bank_code": "ABC",
        "branch_code": "001",
    }

    process_and_store_resolution("test_batch_control_id", mock_response, beneficiary_map, mock_session_maker)

    assert len(mock_session_maker.details_list) == 1
    update_values = next(
        (item for item in mock_session_maker.updates if "fa_resolution_status" in item),
        None,
    )
    assert update_values is not None
    assert update_values["fa_resolution_status"] == ProcessStatus.PROCESSED
    assert isinstance(update_values["fa_resolution_timestamp"], datetime)
    assert update_values["fa_resolution_latest_error_code"] is None
    assert mock_session_maker.flushed
    assert mock_session_maker.committed


def test_process_and_store_resolution_failure(mock_session_maker, mock_resolve_helper):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [MagicMock(id="test_beneficiary_id", fa=None)]
    beneficiary_map = {"test_beneficiary_id": "test_disbursement_id"}

    process_and_store_resolution("test_batch_control_id", mock_response, beneficiary_map, mock_session_maker)

    update_values = next(
        (item for item in mock_session_maker.updates if "fa_resolution_status" in item),
        None,
    )
    assert update_values is not None
    assert update_values["fa_resolution_status"] == ProcessStatus.PENDING
    assert update_values["fa_resolution_latest_error_code"] is not None
    assert mock_session_maker.flushed
    assert mock_session_maker.committed
