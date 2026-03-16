from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from openg2p_fastapi_common.schemas import (
    G2PRequest,
    G2PRequestBody,
    G2PResponse,
    G2PResponseBody,
)
from pydantic import BaseModel, ConfigDict

from .strategy import FaUnion


class StatusEnum(Enum):
    succ = "succ"
    rjct = "rjct"


class LinkStatusReasonCode(Enum):
    rjct_reference_id_invalid = "rjct.reference_id.invalid"
    rjct_reference_id_duplicate = "rjct.reference_id.duplicate"
    rjct_timestamp_invalid = "rjct.timestamp.invalid"
    rjct_id_invalid = "rjct.id.invalid"
    rjct_fa_invalid = "rjct.fa.invalid"
    rjct_name_invalid = "rjct.name.invalid"
    rjct_mobile_number_invalid = "rjct.mobile_number.invalid"
    rjct_unknown_retry = "rjct.unknown.retry"
    rjct_other_error = "rjct.other.error"


class SingleLinkRequest(BaseModel):
    reference_id: str
    timestamp: str
    id: Optional[str] = None
    fa: FaUnion
    name: Optional[str] = None
    phone_number: Optional[str] = None
    additional_info: Optional[List[Dict[str, Any]]] = None
    locale: Optional[str] = "en"


class LinkRequestPayload(BaseModel):
    transaction_id: str
    link_request: List[SingleLinkRequest]


class LinkRequestBody(G2PRequestBody):
    request_payload: LinkRequestPayload


class LinkRequest(G2PRequest):
    request_body: LinkRequestBody


class SingleLinkResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    reference_id: str
    timestamp: datetime
    fa: Optional[str] = None
    status: StatusEnum
    status_reason_code: Optional[LinkStatusReasonCode] = None
    status_reason_message: Optional[str] = None
    additional_info: Optional[List[object]] = None
    locale: Optional[str] = "en"


class LinkResponsePayload(BaseModel):
    transaction_id: Optional[str] = None
    correlation_id: Optional[str] = None
    link_response: List[SingleLinkResponse]


class LinkResponseBody(G2PResponseBody):
    response_payload: LinkResponsePayload


class LinkResponse(G2PResponse):
    response_body: LinkResponseBody
