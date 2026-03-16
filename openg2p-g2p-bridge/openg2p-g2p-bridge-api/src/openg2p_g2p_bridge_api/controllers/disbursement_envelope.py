import logging
from typing import Annotated

from fastapi import Depends
from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import (
    DisbursementEnvelopeException,
    RequestValidationException,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementEnvelopePayload,
    DisbursementEnvelopeRequest,
    DisbursementEnvelopeResponse,
)
from openg2p_g2pconnect_common_lib.jwt_signature_validator import JWTSignatureValidator

from ..config import Settings
from ..services import (
    DisbursementEnvelopeService,
    RequestValidation,
)

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementEnvelopeController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_envelope_service = DisbursementEnvelopeService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Envelope"]

        self.router.add_api_route(
            "/create_disbursement_envelopes",
            self.create_disbursement_envelopes,
            responses={200: {"model": DisbursementEnvelopeResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/cancel_disbursement_envelope",
            self.cancel_disbursement_envelope,
            responses={200: {"model": DisbursementEnvelopeResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/amend_disbursement_envelope",
            self.amend_disbursement_envelope,
            responses={200: {"model": DisbursementEnvelopeResponse}},
            methods=["POST"],
        )

    async def create_disbursement_envelopes(
        self,
        disbursement_envelope_request: DisbursementEnvelopeRequest,
        is_signature_valid: Annotated[bool, Depends(JWTSignatureValidator())],
    ) -> DisbursementEnvelopeResponse:
        _logger.info("Bulk creating disbursement envelopes")
        try:
            _logger.info(f"Request to create disbursement envelopes: {disbursement_envelope_request}")
            RequestValidation.get_component().validate_signature(is_signature_valid)
            RequestValidation.get_component().validate_request(disbursement_envelope_request)
            RequestValidation.get_component().validate_create_disbursement_envelope_request_header(
                disbursement_envelope_request
            )
            disbursement_envelope_payloads: list[DisbursementEnvelopePayload] = (
                await self.disbursement_envelope_service.create_disbursement_envelopes(
                    disbursement_envelope_request
                )
            )
        except RequestValidationException as e:
            _logger.error(f"Error validating request: {e}")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response
        except DisbursementEnvelopeException as e:
            _logger.error("Error creating disbursement envelopes")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response

        disbursement_envelope_response: DisbursementEnvelopeResponse = (
            await self.disbursement_envelope_service.construct_disbursement_envelope_success_response(
                disbursement_envelope_request, disbursement_envelope_payloads
            )
        )
        _logger.info("Disbursement envelopes created successfully")
        return disbursement_envelope_response

    async def cancel_disbursement_envelope(
        self,
        disbursement_envelope_request: DisbursementEnvelopeRequest,
        is_signature_valid: Annotated[bool, Depends(JWTSignatureValidator())],
    ) -> DisbursementEnvelopeResponse:
        _logger.info("Cancelling disbursement envelope")
        try:
            RequestValidation.get_component().validate_signature(is_signature_valid)
            RequestValidation.get_component().validate_request(disbursement_envelope_request)
            RequestValidation.get_component().validate_cancel_disbursement_envelope_request_header(
                disbursement_envelope_request
            )

            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                await self.disbursement_envelope_service.cancel_disbursement_envelope(
                    disbursement_envelope_request
                )
            )
        except RequestValidationException as e:
            _logger.error("Error validating request")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response
        except DisbursementEnvelopeException as e:
            _logger.error("Error cancelling disbursement envelope")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response

        disbursement_envelope_response: DisbursementEnvelopeResponse = (
            await self.disbursement_envelope_service.construct_disbursement_envelope_success_response(
                disbursement_envelope_request, disbursement_envelope_payload
            )
        )
        _logger.info("Disbursement envelope cancelled successfully")
        return disbursement_envelope_response

    async def amend_disbursement_envelope(
        self,
        disbursement_envelope_request: DisbursementEnvelopeRequest,
        is_signature_valid: Annotated[bool, Depends(JWTSignatureValidator())],
    ) -> DisbursementEnvelopeResponse:
        _logger.info("Amending disbursement envelope")
        try:
            RequestValidation.get_component().validate_signature(is_signature_valid)
            RequestValidation.get_component().validate_request(disbursement_envelope_request)
            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                await self.disbursement_envelope_service.amend_disbursement_envelope(
                    disbursement_envelope_request
                )
            )
        except RequestValidationException as e:
            _logger.error("Error validating request")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response
        except DisbursementEnvelopeException as e:
            _logger.error("Error amending disbursement envelope")
            error_response: DisbursementEnvelopeResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_envelope_request, e.code
                )
            )
            return error_response

        disbursement_envelope_response: DisbursementEnvelopeResponse = (
            await self.disbursement_envelope_service.construct_disbursement_envelope_success_response(
                disbursement_envelope_request, disbursement_envelope_payload
            )
        )
        _logger.info("Disbursement envelope amended successfully")
        return disbursement_envelope_response
