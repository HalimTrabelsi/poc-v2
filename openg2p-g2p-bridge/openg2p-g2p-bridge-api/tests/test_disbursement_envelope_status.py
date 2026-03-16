from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from openg2p_g2p_bridge_api.controllers import DisbursementEnvelopeStatusController
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementStatusException
from openg2p_g2p_bridge_models.models.disbursement_envelope import (
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementEnvelopeStatusPayload,
    DisbursementEnvelopeStatusRequest,
    DisbursementEnvelopeStatusResponse,
)
from openg2p_g2pconnect_common_lib.schemas import (
    RequestHeader,
    StatusEnum,
    SyncResponseHeader,
)


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementEnvelopeStatusService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
async def test_get_disbursement_envelope_status_success(mock_request_validation, mock_service_get_component):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    # Setup mock service
    mock_service_instance = AsyncMock()
    mock_service_get_component.return_value = mock_service_instance

    # Mock service methods
    mock_service_instance.get_disbursement_envelope_status = AsyncMock(
        return_value=DisbursementEnvelopeStatusPayload(
            disbursement_envelope_id="env123",
            benefit_code_id=1,
            benefit_code_mnemonic="BEN123",
            benefit_type="CASH_DIGITAL",
            measurement_unit="kg",
            number_of_beneficiaries_received=100,
            number_of_beneficiaries_declared=100,
            number_of_disbursements_declared=100,
            number_of_disbursements_received=100,
            total_disbursement_quantity_declared=5000.0,
            total_disbursement_quantity_received=5000,
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE,
            funds_available_latest_timestamp=datetime.now(),
            funds_available_latest_error_code=None,
            funds_available_attempts=3,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
            funds_blocked_latest_timestamp=datetime.now(),
            funds_blocked_latest_error_code=None,
            funds_blocked_attempts=2,
            funds_blocked_reference_number="ref123",
            number_of_disbursements_shipped=100,
            number_of_disbursements_reconciled=95,
            number_of_disbursements_reversed=5,
            no_of_warehouses_allocated=1,
            no_of_warehouses_notified=1,
            no_of_agencies_allocated=1,
            no_of_agencies_notified=1,
            no_of_beneficiaries_notified=100,
            no_of_pods_received=None,
            disbursement_batch_control_geos=None,
        )
    )

    request_header = RequestHeader(
        message_id="123",
        message_ts=datetime.now().isoformat(),
        action="",
        sender_id="",
        sender_uri="",
        receiver_id="",
        total_count=1,
        is_msg_encrypted=False,
    )
    request_payload = DisbursementEnvelopeStatusRequest(
        header=request_header,
        message="env123",
    )

    expected_response = DisbursementEnvelopeStatusResponse(
        header=SyncResponseHeader(
            message_id=request_header.message_id,
            message_ts=request_header.message_ts,
            action=request_header.action,
            status=StatusEnum.succ,
            status_reason_message="",
        ),
        message=DisbursementEnvelopeStatusPayload(
            disbursement_envelope_id="env123",
            benefit_code_id=1,
            benefit_code_mnemonic="BEN123",
            benefit_type="CASH_DIGITAL",
            measurement_unit="kg",
            number_of_beneficiaries_received=100,
            number_of_beneficiaries_declared=100,
            number_of_disbursements_declared=100,
            number_of_disbursements_received=100,
            total_disbursement_quantity_declared=5000.0,
            total_disbursement_quantity_received=5000,
            funds_available_with_bank=FundsAvailableWithBankEnum.FUNDS_AVAILABLE,
            funds_available_latest_timestamp=None,
            funds_available_latest_error_code=None,
            funds_available_attempts=3,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
            funds_blocked_latest_timestamp=None,
            funds_blocked_latest_error_code=None,
            funds_blocked_attempts=2,
            funds_blocked_reference_number="ref123",
            number_of_disbursements_shipped=100,
            number_of_disbursements_reconciled=95,
            number_of_disbursements_reversed=5,
            no_of_warehouses_allocated=1,
            no_of_warehouses_notified=1,
            no_of_agencies_allocated=1,
            no_of_agencies_notified=1,
            no_of_beneficiaries_notified=100,
            no_of_pods_received=None,
            disbursement_batch_control_geos=None,
        ),
    )

    mock_service_instance.construct_disbursement_envelope_status_success_response = AsyncMock(
        return_value=expected_response
    )

    # Instantiate controller and make request
    controller = DisbursementEnvelopeStatusController()
    actual_response = await controller.get_disbursement_envelope_status(
        request_payload, is_signature_valid=True
    )
    assert actual_response == expected_response


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementEnvelopeStatusService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
@pytest.mark.parametrize("error_code", list(G2PBridgeErrorCodes))
async def test_get_disbursement_envelope_status_failure(
    mock_request_validation, mock_service_get_component, error_code
):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    # Setup mock service
    mock_service_instance = AsyncMock()
    mock_service_get_component.return_value = mock_service_instance

    # Mock service methods to raise an error
    mock_service_instance.get_disbursement_envelope_status.side_effect = DisbursementStatusException(
        code=error_code, message=f"{error_code} error."
    )

    request_header = RequestHeader(
        message_id="123",
        message_ts=datetime.now().isoformat(),
        action="",
        sender_id="",
        sender_uri="",
        receiver_id="",
        total_count=1,
        is_msg_encrypted=False,
    )
    request_payload = DisbursementEnvelopeStatusRequest(
        header=request_header,
        message="env123",
    )

    error_response = DisbursementEnvelopeStatusResponse(
        header=SyncResponseHeader(
            message_id=request_header.message_id,
            message_ts=request_header.message_ts,
            action=request_header.action,
            status=StatusEnum.rjct,
            status_reason_message=error_code,
        ),
        message=None,
    )

    mock_service_instance.construct_disbursement_envelope_status_error_response = AsyncMock(
        return_value=error_response
    )

    # Instantiate controller and make request
    controller = DisbursementEnvelopeStatusController()
    actual_response = await controller.get_disbursement_envelope_status(
        request_payload, is_signature_valid=True
    )
    assert (
        actual_response == error_response
    ), f"The response did not match the expected error response for {error_code}."
