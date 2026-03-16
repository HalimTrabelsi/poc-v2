"""
Unit tests for MapperController
Tests API endpoints for Link, Resolve, Update, and Unlink operations
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openg2p_fastapi_auth_models.schemas import AuthCredentials
from openg2p_spar_bene_portal_api.controllers import MapperController
from openg2p_spar_models.schemas import StatusEnum


@pytest.mark.asyncio
async def test_link_endpoint():
    """Test link API endpoint with ID construction from auth credentials"""
    # Create mock auth credentials
    mock_auth = MagicMock(spec=AuthCredentials)
    mock_auth.iss = "test_issuer"
    mock_auth.sub = "test_subject"
    mock_auth.model_dump.return_value = {"iss": "test_issuer", "sub": "test_subject"}

    # Create mock request with link_request items
    mock_request = MagicMock()
    mock_single_link_request = MagicMock()
    mock_single_link_request.id = "original_id"
    # Create a mock FA object with strategy_id
    mock_fa = MagicMock()
    mock_fa.strategy_id = 1
    mock_single_link_request.fa = mock_fa
    mock_single_link_request.additional_info = None
    mock_request.request_body.request_payload.link_request = [mock_single_link_request]

    # Mock all dependencies before creating controller
    with patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.MapperService"
    ) as mock_mapper_service, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.RequestValidation"
    ) as mock_validation, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.StrategyHelper"
    ) as mock_strategy_helper, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.ResponseHelper"
    ) as mock_response_helper:
        # Setup service mock
        mock_service_instance = MagicMock()
        mock_service_instance.link = AsyncMock(
            return_value=[MagicMock(reference_id="ref123", status=StatusEnum.succ)]
        )
        mock_mapper_service.get_component.return_value = mock_service_instance

        # Setup validation mock
        mock_validation.get_component.return_value.validate_request.return_value = None

        # Setup strategy helper mock
        mock_strategy_helper_instance = MagicMock()
        mock_strategy_helper_instance.construct_id = AsyncMock(
            return_value="constructed_id_123"
        )
        mock_strategy_helper_instance.construct_fa = AsyncMock(
            return_value="constructed_fa_123"
        )
        mock_strategy_helper.return_value.get_component.return_value = (
            mock_strategy_helper_instance
        )

        # Setup response helper mock
        mock_response = MagicMock()
        mock_response_helper.get_component.return_value.construct_link_response.return_value = (
            mock_response
        )

        # Create controller
        controller = MapperController()

        # Call the endpoint
        result = await controller.link(mock_request, mock_auth)

        # Verify ID was replaced with constructed ID
        assert mock_single_link_request.id == "constructed_id_123"
        # Verify FA was constructed
        assert mock_single_link_request.fa == "constructed_fa_123"
        # Verify additional_info contains strategy_id
        assert mock_single_link_request.additional_info[0]["strategy_id"] == 1
        assert result is not None
        mock_service_instance.link.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_endpoint():
    """Test resolve API endpoint with ID construction from auth credentials"""
    # Create mock auth credentials
    mock_auth = MagicMock(spec=AuthCredentials)
    mock_auth.iss = "test_issuer"
    mock_auth.sub = "test_subject"
    mock_auth.model_dump.return_value = {"iss": "test_issuer", "sub": "test_subject"}

    # Create mock request with resolve_request items
    mock_request = MagicMock()
    mock_single_resolve_request = MagicMock()
    mock_single_resolve_request.id = ""  # Empty ID should be replaced
    mock_single_resolve_request.fa = "test_fa"
    mock_request.request_body.request_payload.resolve_request = [
        mock_single_resolve_request
    ]

    # Mock all dependencies before creating controller
    with patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.MapperService"
    ) as mock_mapper_service, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.RequestValidation"
    ) as mock_validation, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.StrategyHelper"
    ) as mock_strategy_helper, patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.ResponseHelper"
    ) as mock_response_helper:
        # Setup service mock
        mock_service_instance = MagicMock()
        mock_service_instance.resolve = AsyncMock(
            return_value=[
                MagicMock(reference_id="ref123", fa="FA456", status=StatusEnum.succ)
            ]
        )
        mock_mapper_service.get_component.return_value = mock_service_instance

        # Setup validation mock
        mock_validation.get_component.return_value.validate_request.return_value = None

        # Setup strategy helper mock
        mock_strategy_helper_instance = MagicMock()
        mock_strategy_helper_instance.construct_id = AsyncMock(
            return_value="constructed_id_456"
        )
        mock_strategy_helper.return_value.get_component.return_value = (
            mock_strategy_helper_instance
        )

        # Setup response helper mock
        mock_response = MagicMock()
        mock_response_helper.get_component.return_value.construct_resolve_response.return_value = (
            mock_response
        )

        # Create controller
        controller = MapperController()

        # Call the endpoint
        result = await controller.resolve(mock_request, mock_auth)

        # Verify ID was replaced with constructed ID
        assert mock_single_resolve_request.id == "constructed_id_456"
        assert result is not None
        mock_service_instance.resolve.assert_called_once()


@pytest.mark.asyncio
async def test_controller_initialization():
    """Test MapperController initialization"""
    with patch(
        "openg2p_spar_bene_portal_api.controllers.mapper_controller.MapperService"
    ) as mock_mapper_service:
        mock_mapper_service.get_component.return_value = MagicMock()
        controller = MapperController()
        assert controller is not None
        assert hasattr(controller, "link")
        assert hasattr(controller, "resolve")
        assert hasattr(controller, "update")
        assert hasattr(controller, "unlink")
