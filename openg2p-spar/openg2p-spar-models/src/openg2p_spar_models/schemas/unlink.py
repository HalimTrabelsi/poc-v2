from datetime import datetime
from enum import Enum
from typing import List, Optional

from openg2p_fastapi_common.schemas import (
    G2PRequest,
    G2PRequestBody,
    G2PResponse,
    G2PResponseBody,
)
from pydantic import BaseModel, ConfigDict

from .link import StatusEnum


class UnlinkStatusReasonCode(Enum):
    rjct_reference_id_invalid = "rjct.reference_id.invalid"
    rjct_id_invalid = "rjct.id.invalid"
    rjct_fa_invalid = "rjct.fa.invalid"
    rjct_reference_id_duplicate = "rjct.reference_id.duplicate"
    rjct_timestamp_invalid = "rjct.timestamp.invalid"
    rjct_beneficiary_name_invalid = "rjct.beneficiary_name.invalid"


class SingleUnlinkRequest(BaseModel):
    reference_id: str
    timestamp: str
    id: Optional[str] = None
    fa: Optional[str] = None
    name: Optional[str] = None
    phone_number: Optional[str] = None
    additional_info: Optional[List[object]] = None
    locale: Optional[str] = "en"


class UnlinkRequestPayload(BaseModel):
    transaction_id: str
    unlink_request: List[SingleUnlinkRequest]


class UnlinkRequestBody(G2PRequestBody):
    request_payload: UnlinkRequestPayload


class UnlinkRequest(G2PRequest):
    request_body: UnlinkRequestBody


class SingleUnlinkResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    reference_id: str
    timestamp: datetime
    id: Optional[str] = ""
    status: StatusEnum
    status_reason_code: Optional[UnlinkStatusReasonCode] = None
    status_reason_message: Optional[str] = ""
    additional_info: Optional[List[object]] = None
    locale: Optional[str] = "en"


class UnlinkResponsePayload(BaseModel):
    transaction_id: Optional[str] = None
    correlation_id: Optional[str] = ""
    unlink_response: List[SingleUnlinkResponse]


class UnlinkResponseBody(G2PResponseBody):
    response_payload: UnlinkResponsePayload


class UnlinkResponse(G2PResponse):
    response_body: UnlinkResponseBody
