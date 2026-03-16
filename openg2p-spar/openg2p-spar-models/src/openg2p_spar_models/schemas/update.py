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

from .link import StatusEnum
from .strategy import FaUnion


class UpdateStatusReasonCode(Enum):
    rjct_reference_id_invalid = "rjct.reference_id.invalid"
    rjct_reference_id_duplicate = "rjct.reference_id.duplicate"
    rjct_timestamp_invalid = "rjct.timestamp.invalid"
    rjct_beneficiary_name_invalid = "rjct.beneficiary_name.invalid"
    rjct_id_invalid = "rjct.id.invalid"
    rjct_fa_invalid = "rjct.fa.invalid"


class SingleUpdateRequest(BaseModel):
    reference_id: str
    timestamp: str
    id: Optional[str] = None
    fa: FaUnion
    name: Optional[str] = None
    phone_number: Optional[str] = None
    additional_info: Optional[List[Dict[str, Any]]] = None
    locale: Optional[str] = "en"


class UpdateRequestPayload(BaseModel):
    transaction_id: str
    update_request: List[SingleUpdateRequest]


class UpdateRequestBody(G2PRequestBody):
    request_payload: UpdateRequestPayload


class UpdateRequest(G2PRequest):
    request_body: UpdateRequestBody


class SingleUpdateResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    reference_id: str
    timestamp: datetime
    id: Optional[str] = ""
    status: StatusEnum
    status_reason_code: Optional[UpdateStatusReasonCode] = None
    status_reason_message: Optional[str] = ""
    additional_info: Optional[List[object]] = None
    locale: Optional[str] = "en"


class UpdateResponsePayload(BaseModel):
    transaction_id: Optional[str] = None
    correlation_id: Optional[str] = ""
    update_response: List[SingleUpdateResponse]


class UpdateResponseBody(G2PResponseBody):
    response_payload: UpdateResponsePayload


class UpdateResponse(G2PResponse):
    response_body: UpdateResponseBody
