from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from openg2p_g2p_bridge_api.controllers import DisbursementController
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.models import CancellationStatus
from openg2p_g2p_bridge_models.schemas import (
    DisbursementPayload,
    DisbursementRequest,
    DisbursementResponse,
)
from openg2p_g2pconnect_common_lib.schemas import (
    RequestHeader,
    StatusEnum,
    SyncResponseHeader,
)


def mock_create_disbursements(is_valid, disbursement_request):
    if not is_valid:
        raise DisbursementException(
            code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
            disbursement_payloads=disbursement_request.message,
        )
    return disbursement_request


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
async def test_create_disbursements_success(mock_request_validation, mock_service_get_component):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    mock_service_instance = AsyncMock()
    disbursement_payloads = [
        DisbursementPayload(
            disbursement_id="disb123",
            disbursement_envelope_id="env123",
            beneficiary_id="123AB",
            disbursement_amount=1000,
        )
    ]
    disbursement_request = DisbursementRequest(
        header=RequestHeader(
            message_id="123",
            message_ts=datetime.now().isoformat(),
            action="",
            sender_id="",
            sender_uri="",
            receiver_id="",
            total_count=1,
            is_msg_encrypted=False,
        ),
        message=disbursement_payloads,
    )
    mock_service_instance.create_disbursements = AsyncMock(
        return_value=mock_create_disbursements(True, disbursement_request)
    )
    mock_service_instance.construct_disbursement_success_response = AsyncMock(
        return_value=DisbursementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.succ,
                status_reason_message="",
            ),
            message=disbursement_payloads,
        )
    )

    mock_service_get_component.return_value = mock_service_instance

    controller = DisbursementController()
    request_payload = disbursement_request

    response = await controller.create_disbursements(request_payload, is_signature_valid=True)

    assert response.message == disbursement_payloads


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
async def test_create_disbursements_failure(mock_request_validation, mock_service_get_component):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    mock_service_instance = AsyncMock()
    disbursement_payloads = [
        DisbursementPayload(
            disbursement_id="disb123",
            disbursement_envelope_id="env123",
            beneficiary_id="123AB",
            disbursement_amount=1000,
        )
    ]
    disbursement_request = DisbursementRequest(
        header=RequestHeader(
            message_id="123",
            message_ts=datetime.now().isoformat(),
            action="",
            sender_id="",
            sender_uri="",
            receiver_id="",
            total_count=1,
            is_msg_encrypted=False,
        ),
        message=disbursement_payloads,
    )
    mock_service_instance.create_disbursements = AsyncMock(
        side_effect=lambda req: mock_create_disbursements(False, req)
    )
    mock_service_instance.construct_disbursement_error_response = AsyncMock(
        return_value=DisbursementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.rjct,
                status_reason_message=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
            ),
            message=disbursement_payloads,
        )
    )

    mock_service_get_component.return_value = mock_service_instance

    controller = DisbursementController()
    request_payload = disbursement_request

    response = await controller.create_disbursements(request_payload, is_signature_valid=True)

    assert response.header.status_reason_message == G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD.value


def mock_cancel_disbursements(is_valid, disbursement_request):
    if not is_valid:
        raise DisbursementException(
            code=G2PBridgeErrorCodes.DISBURSEMENT_ALREADY_CANCELED,
            disbursement_payloads=disbursement_request.message,
        )
    for payload in disbursement_request.message:
        payload.cancellation_status = CancellationStatus.CANCELLED
        payload.cancellation_time_stamp = datetime.now()
    return disbursement_request


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
async def test_cancel_disbursements_success(mock_request_validation, mock_service_get_component):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    mock_service_instance = AsyncMock()
    disbursement_payloads = [
        DisbursementPayload(
            disbursement_id="123",
            beneficiary_id="123AB",
            disbursement_amount=1000,
            cancellation_status=None,
        )
    ]
    disbursement_request = DisbursementRequest(
        header=RequestHeader(
            message_id="123",
            message_ts=datetime.now().isoformat(),
            action="",
            sender_id="",
            sender_uri="",
            receiver_id="",
            total_count=1,
            is_msg_encrypted=False,
        ),
        message=disbursement_payloads,
    )
    mock_service_instance.cancel_disbursements = AsyncMock(
        return_value=mock_cancel_disbursements(True, disbursement_request)
    )
    mock_service_instance.construct_disbursement_success_response = AsyncMock(
        return_value=DisbursementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.succ,
                status_reason_message="",
            ),
            message=disbursement_payloads,
        )
    )

    mock_service_get_component.return_value = mock_service_instance

    controller = DisbursementController()
    request_payload = disbursement_request

    response = await controller.cancel_disbursements(request_payload, is_signature_valid=True)

    assert response.header.status == StatusEnum.succ
    assert all(payload.cancellation_status == CancellationStatus.CANCELLED for payload in response.message)


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementService.get_component")
@patch("openg2p_g2p_bridge_api.services.RequestValidation.get_component")
async def test_cancel_disbursements_failure(mock_request_validation, mock_service_get_component):
    mock_request_validation.validate_signature.return_value = None
    mock_request_validation.validate_request.return_value = None

    mock_service_instance = AsyncMock()
    disbursement_payloads = [
        DisbursementPayload(
            disbursement_id="123",
            beneficiary_id="123AB",
            disbursement_amount=1000,
            cancellation_status=None,
        )
    ]
    disbursement_request = DisbursementRequest(
        header=RequestHeader(
            message_id="123",
            message_ts=datetime.now().isoformat(),
            action="",
            sender_id="",
            sender_uri="",
            receiver_id="",
            total_count=1,
            is_msg_encrypted=False,
        ),
        message=disbursement_payloads,
    )
    mock_service_instance.cancel_disbursements = AsyncMock(
        side_effect=lambda req: mock_cancel_disbursements(False, req)
    )
    mock_service_instance.construct_disbursement_error_response = AsyncMock(
        return_value=DisbursementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.rjct,
                status_reason_message=G2PBridgeErrorCodes.DISBURSEMENT_ALREADY_CANCELED,
            ),
            message=disbursement_payloads,
        )
    )

    mock_service_get_component.return_value = mock_service_instance

    controller = DisbursementController()
    request_payload = disbursement_request

    response = await controller.cancel_disbursements(request_payload, is_signature_valid=True)

    assert response.header.status == StatusEnum.rjct
    assert response.header.status_reason_message == G2PBridgeErrorCodes.DISBURSEMENT_ALREADY_CANCELED.value
