"""
Unit tests for DfspController
Tests DFSP Bank, Branch, and Wallet Service Provider API endpoints
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openg2p_fastapi_common.schemas import G2PResponseStatus
from openg2p_spar_bene_portal_api.controllers import DfspController
from openg2p_spar_models.schemas import (
    BankSchema,
    BranchSchema,
    WalletServiceProviderSchema,
)


@pytest.mark.asyncio
@patch(
    "openg2p_spar_bene_portal_api.controllers.dfsp_controller.DfspService.get_component"
)
async def test_fetch_banks(mock_dfsp_service):
    """Test fetch banks endpoint"""
    # Setup mock
    mock_service_instance = MagicMock()
    mock_service_instance.fetch_banks = AsyncMock()
    mock_dfsp_service.return_value = mock_service_instance

    # Create controller
    controller = DfspController()

    # Mock request
    mock_request = MagicMock()
    mock_request.request_header.request_id = "req-123"

    # Mock response
    mock_banks = [
        BankSchema(
            id=1, bank_mnemonic="ICIC", bank_name="ICICI Bank", bank_code="ICIC"
        ),
        BankSchema(id=2, bank_mnemonic="HDFC", bank_name="HDFC Bank", bank_code="HDFC"),
    ]
    mock_response = MagicMock()
    mock_response.response_body.response_payload.banks = mock_banks
    mock_response.response_header.response_status = G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_banks.return_value = mock_response

    # Call endpoint
    result = await controller.fetch_banks(mock_request)

    # Verify
    assert result is not None
    assert result.response_header.response_status == G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_banks.assert_called_once_with(mock_request)


@pytest.mark.asyncio
@patch(
    "openg2p_spar_bene_portal_api.controllers.dfsp_controller.DfspService.get_component"
)
async def test_fetch_branches(mock_dfsp_service):
    """Test fetch branches endpoint"""
    # Setup mock
    mock_service_instance = MagicMock()
    mock_service_instance.fetch_branches = AsyncMock()
    mock_dfsp_service.return_value = mock_service_instance

    # Create controller
    controller = DfspController()

    # Mock request
    mock_request = MagicMock()
    mock_request.request_header.request_id = "req-456"
    mock_request.request_body.request_payload.bank_id = 1

    # Mock response
    mock_branches = [
        BranchSchema(
            id=3,
            branch_mnemonic="ICIC_DEL",
            branch_name="ICICI Delhi",
            branch_code="ICIC_DEL",
            bank_id=1,
        ),
        BranchSchema(
            id=4,
            branch_mnemonic="ICIC_MUM",
            branch_name="ICICI Mumbai",
            branch_code="ICIC_MUM",
            bank_id=1,
        ),
    ]
    mock_response = MagicMock()
    mock_response.response_body.response_payload.branches = mock_branches
    mock_response.response_header.response_status = G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_branches.return_value = mock_response

    # Call endpoint
    result = await controller.fetch_branches(mock_request)

    # Verify
    assert result is not None
    assert result.response_header.response_status == G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_branches.assert_called_once_with(mock_request)


@pytest.mark.asyncio
@patch(
    "openg2p_spar_bene_portal_api.controllers.dfsp_controller.DfspService.get_component"
)
async def test_fetch_wallet_service_providers(mock_dfsp_service):
    """Test fetch wallet service providers endpoint"""
    # Setup mock
    mock_service_instance = MagicMock()
    mock_service_instance.fetch_wallet_service_providers = AsyncMock()
    mock_dfsp_service.return_value = mock_service_instance

    # Create controller
    controller = DfspController()

    # Mock request
    mock_request = MagicMock()
    mock_request.request_header.request_id = "req-789"

    # Mock response
    mock_providers = [
        WalletServiceProviderSchema(
            id=1,
            sp_mnemonic="GMAIL",
            sp_name="Gmail Wallet",
            sp_code="GMAIL",
            wallet_type="EMAIL",
        ),
        WalletServiceProviderSchema(
            id=2,
            sp_mnemonic="AIRTEL",
            sp_name="Airtel Money",
            sp_code="AIRTEL",
            wallet_type="MOBILE",
        ),
    ]
    mock_response = MagicMock()
    mock_response.response_body.response_payload.wallet_service_providers = (
        mock_providers
    )
    mock_response.response_header.response_status = G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_wallet_service_providers.return_value = mock_response

    # Call endpoint
    result = await controller.fetch_wallet_service_providers(mock_request)

    # Verify
    assert result is not None
    assert result.response_header.response_status == G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_wallet_service_providers.assert_called_once_with(
        mock_request
    )


@pytest.mark.asyncio
@patch(
    "openg2p_spar_bene_portal_api.controllers.dfsp_controller.DfspService.get_component"
)
async def test_fetch_branches_with_bank_id_filter(mock_dfsp_service):
    """Test fetch branches with bank_id filter"""
    # Setup mock
    mock_service_instance = MagicMock()
    mock_service_instance.fetch_branches = AsyncMock()
    mock_dfsp_service.return_value = mock_service_instance

    # Create controller
    controller = DfspController()

    # Mock request with bank_id filter
    mock_request = MagicMock()
    mock_request.request_header.request_id = "req-filter"
    mock_request.request_body.request_payload.bank_id = 2

    # Mock response
    mock_branches = [
        BranchSchema(
            id=5,
            branch_mnemonic="HDFC_DEL",
            branch_name="HDFC Delhi",
            branch_code="HDFC_DEL",
            bank_id=2,
        ),
    ]
    mock_response = MagicMock()
    mock_response.response_body.response_payload.branches = mock_branches
    mock_response.response_header.response_status = G2PResponseStatus.SUCCESS
    mock_service_instance.fetch_branches.return_value = mock_response

    # Call endpoint
    result = await controller.fetch_branches(mock_request)

    # Verify
    assert result is not None
    assert len(result.response_body.response_payload.branches) == 1
    assert result.response_body.response_payload.branches[0].bank_id == 2
    mock_service_instance.fetch_branches.assert_called_once_with(mock_request)
