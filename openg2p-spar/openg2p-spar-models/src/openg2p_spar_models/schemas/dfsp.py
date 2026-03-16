from typing import List, Optional

from openg2p_fastapi_common.schemas import (
    G2PRequest,
    G2PRequestBody,
    G2PResponse,
    G2PResponseBody,
)
from pydantic import BaseModel, ConfigDict


# Bank Schemas
class BankSchema(BaseModel):
    """Pydantic schema for Bank model"""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    bank_mnemonic: str
    bank_name: str
    bank_code: str


# Branch Schemas
class BranchSchema(BaseModel):
    """Pydantic schema for Branch model"""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    branch_mnemonic: str
    branch_name: str
    branch_code: str
    bank_id: int


# WalletServiceProvider Schemas
class WalletServiceProviderSchema(BaseModel):
    """Pydantic schema for WalletServiceProvider model"""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    sp_mnemonic: str
    sp_name: str
    sp_code: str
    wallet_type: str


# DFSP Request/Response Schemas
class BanksRequestPayload(BaseModel):
    """Payload for banks request"""

    pass


class BanksRequestBody(G2PRequestBody):
    """Request body for banks"""

    request_payload: BanksRequestPayload


class BanksRequest(G2PRequest):
    """Request for banks"""

    request_body: BanksRequestBody


class BanksResponsePayload(BaseModel):
    """Response payload for banks"""

    banks: List[BankSchema]


class BanksResponseBody(G2PResponseBody):
    """Response body for banks"""

    response_payload: BanksResponsePayload


class BanksResponse(G2PResponse):
    """Response for banks"""

    response_body: BanksResponseBody


# Branches Request/Response
class BranchesRequestPayload(BaseModel):
    """Payload for branches request"""

    bank_id: Optional[int] = None


class BranchesRequestBody(G2PRequestBody):
    """Request body for branches"""

    request_payload: BranchesRequestPayload


class BranchesRequest(G2PRequest):
    """Request for branches"""

    request_body: BranchesRequestBody


class BranchesResponsePayload(BaseModel):
    """Response payload for branches"""

    branches: List[BranchSchema]


class BranchesResponseBody(G2PResponseBody):
    """Response body for branches"""

    response_payload: BranchesResponsePayload


class BranchesResponse(G2PResponse):
    """Response for branches"""

    response_body: BranchesResponseBody


# Wallet Service Providers Request/Response
class WalletServiceProvidersRequestPayload(BaseModel):
    """Payload for wallet service providers request"""

    pass


class WalletServiceProvidersRequestBody(G2PRequestBody):
    """Request body for wallet service providers"""

    request_payload: WalletServiceProvidersRequestPayload


class WalletServiceProvidersRequest(G2PRequest):
    """Request for wallet service providers"""

    request_body: WalletServiceProvidersRequestBody


class WalletServiceProvidersResponsePayload(BaseModel):
    """Response payload for wallet service providers"""

    wallet_service_providers: List[WalletServiceProviderSchema]


class WalletServiceProvidersResponseBody(G2PResponseBody):
    """Response body for wallet service providers"""

    response_payload: WalletServiceProvidersResponsePayload


class WalletServiceProvidersResponse(G2PResponse):
    """Response for wallet service providers"""

    response_body: WalletServiceProvidersResponseBody
