import logging
from typing import Union

from openg2p_fastapi_common.service import BaseService
from openg2p_spar_models.schemas import (
    LinkRequest,
    ResolveRequest,
    StatusReasonCodeEnum,
    UnlinkRequest,
    UpdateRequest,
)

from ..exceptions import RequestValidationException

_logger = logging.getLogger("request_validation")


class RequestValidation(BaseService):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        print("RequestValidation initialized")

    def validate_signature(self, is_signature_valid) -> None:
        _logger.info("Validating signature")
        if not is_signature_valid:
            _logger.error("Invalid JWT signature")
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_jwt_invalid,
                message=StatusReasonCodeEnum.rjct_jwt_invalid,
            )

        _logger.info("Signature validated successfully")
        return None

    def validate_request(
        self, request: Union[LinkRequest, UpdateRequest, ResolveRequest, UnlinkRequest]
    ) -> None:
        """
        Unified validation method for all request types.
        Validates the request structure based on the request type.
        """
        # Validate request_body exists
        if not request.request_body or not hasattr(
            request.request_body, "request_payload"
        ):
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Missing request_payload in request.request_body",
            )

        # Determine request type and validate accordingly
        if isinstance(request, LinkRequest):
            self._validate_link_request_payload(request)
        elif isinstance(request, UpdateRequest):
            self._validate_update_request_payload(request)
        elif isinstance(request, ResolveRequest):
            self._validate_resolve_request_payload(request)
        elif isinstance(request, UnlinkRequest):
            self._validate_unlink_request_payload(request)
        else:
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message=f"Unsupported request type: {type(request).__name__}",
            )

        return None

    def _validate_link_request_payload(self, request: LinkRequest) -> None:
        """Validate link request payload structure."""
        if not request.request_body.request_payload or not hasattr(
            request.request_body.request_payload, "link_request"
        ):
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Missing link_request in request.request_body.request_payload",
            )
        if not request.request_body.request_payload.link_request:
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Empty link_request list in request.request_body.request_payload",
            )

    def _validate_update_request_payload(self, request: UpdateRequest) -> None:
        """Validate update request payload structure."""
        if not request.request_body.request_payload or not hasattr(
            request.request_body.request_payload, "update_request"
        ):
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Missing update_request in request.request_body.request_payload",
            )
        if not request.request_body.request_payload.update_request:
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Empty update_request list in request.request_body.request_payload",
            )

    def _validate_resolve_request_payload(self, request: ResolveRequest) -> None:
        """Validate resolve request payload structure."""
        if not request.request_body.request_payload or not hasattr(
            request.request_body.request_payload, "resolve_request"
        ):
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Missing resolve_request in request.request_body.request_payload",
            )
        if not request.request_body.request_payload.resolve_request:
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Empty resolve_request list in request.request_body.request_payload",
            )

    def _validate_unlink_request_payload(self, request: UnlinkRequest) -> None:
        """Validate unlink request payload structure."""
        if not request.request_body.request_payload or not hasattr(
            request.request_body.request_payload, "unlink_request"
        ):
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Missing unlink_request in request.request_body.request_payload",
            )
        if not request.request_body.request_payload.unlink_request:
            raise RequestValidationException(
                code=StatusReasonCodeEnum.rjct_action_not_supported,
                message="Empty unlink_request list in request.request_body.request_payload",
            )
