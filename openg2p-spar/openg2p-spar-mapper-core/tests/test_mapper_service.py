"""
Unit tests for MapperService
Tests Link, Resolve, Update, and Unlink operations
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openg2p_spar_mapper_core.services import MapperService
from openg2p_spar_models.schemas import StatusEnum


@pytest.mark.asyncio
async def test_link_success():
    """Test successful link operation"""
    service = MapperService()

    with patch.object(service, "link", new_callable=AsyncMock) as mock_link:
        mock_link.return_value = [
            MagicMock(
                reference_id="ref123", id="ID123", fa="FA456", status=StatusEnum.succ
            )
        ]

        mock_request = MagicMock()
        result = await service.link(mock_request)

        assert result is not None
        assert len(result) == 1
        assert result[0].reference_id == "ref123"
        assert result[0].status == StatusEnum.succ


@pytest.mark.asyncio
async def test_resolve_success():
    """Test successful resolve operation"""
    service = MapperService()

    with patch.object(service, "resolve", new_callable=AsyncMock) as mock_resolve:
        mock_resolve.return_value = [
            MagicMock(
                reference_id="ref123", id="ID123", fa="FA456", status=StatusEnum.succ
            )
        ]

        mock_request = MagicMock()
        result = await service.resolve(mock_request)

        assert result is not None
        assert len(result) == 1
        assert result[0].fa == "FA456"
        assert result[0].status == StatusEnum.succ


@pytest.mark.asyncio
async def test_update_success():
    """Test successful update operation"""
    service = MapperService()

    with patch.object(service, "update", new_callable=AsyncMock) as mock_update:
        mock_update.return_value = [
            MagicMock(
                reference_id="ref123", id="ID123", fa="FA789", status=StatusEnum.succ
            )
        ]

        mock_request = MagicMock()
        result = await service.update(mock_request)

        assert result is not None
        assert len(result) == 1
        assert result[0].fa == "FA789"
        assert result[0].status == StatusEnum.succ


@pytest.mark.asyncio
async def test_unlink_success():
    """Test successful unlink operation"""
    service = MapperService()

    with patch.object(service, "unlink", new_callable=AsyncMock) as mock_unlink:
        mock_unlink.return_value = [
            MagicMock(
                reference_id="ref123", id="ID123", fa="FA456", status=StatusEnum.succ
            )
        ]

        mock_request = MagicMock()
        result = await service.unlink(mock_request)

        assert result is not None
        assert len(result) == 1
        assert result[0].status == StatusEnum.succ


@pytest.mark.asyncio
async def test_link_with_error():
    """Test link operation with error"""
    service = MapperService()

    with patch.object(service, "link", new_callable=AsyncMock) as mock_link:
        mock_link.return_value = [
            MagicMock(
                reference_id="ref123",
                id="ID123",
                fa="FA456",
                status=StatusEnum.rjct,
                status_reason_code="rjct_id_invalid",
            )
        ]

        mock_request = MagicMock()
        result = await service.link(mock_request)

        assert result is not None
        assert result[0].status == StatusEnum.rjct


@pytest.mark.asyncio
async def test_resolve_not_found():
    """Test resolve operation when mapping not found"""
    service = MapperService()

    with patch.object(service, "resolve", new_callable=AsyncMock) as mock_resolve:
        mock_resolve.return_value = [
            MagicMock(
                reference_id="ref123",
                id="NONEXISTENT",
                status=StatusEnum.rjct,
                status_reason_code="rjct_not_found",
            )
        ]

        mock_request = MagicMock()
        result = await service.resolve(mock_request)

        assert result is not None
        assert result[0].status == StatusEnum.rjct


@pytest.mark.asyncio
async def test_multiple_requests():
    """Test handling multiple requests in single operation"""
    service = MapperService()

    with patch.object(service, "link", new_callable=AsyncMock) as mock_link:
        mock_link.return_value = [
            MagicMock(reference_id="ref1", status=StatusEnum.succ),
            MagicMock(reference_id="ref2", status=StatusEnum.succ),
            MagicMock(reference_id="ref3", status=StatusEnum.succ),
        ]

        mock_request = MagicMock()
        result = await service.link(mock_request)

        assert len(result) == 3
        assert all(r.status == StatusEnum.succ for r in result)


@pytest.mark.asyncio
async def test_service_initialization():
    """Test MapperService initialization"""
    service = MapperService()
    assert service is not None
    assert hasattr(service, "link")
    assert hasattr(service, "resolve")
    assert hasattr(service, "update")
    assert hasattr(service, "unlink")
