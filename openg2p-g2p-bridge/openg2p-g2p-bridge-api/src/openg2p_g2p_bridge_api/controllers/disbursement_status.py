import logging
from typing import Annotated, List

from fastapi import Depends
from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import (
    DisbursementException,
    RequestValidationException,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementBatchControlPayload,
    DisbursementBatchControlRequest,
    DisbursementBatchControlResponse,
    DisbursementStatusPayload,
    DisbursementStatusRequest,
    DisbursementStatusResponse,
)
from openg2p_g2pconnect_common_lib.jwt_signature_validator import JWTSignatureValidator

from ..config import Settings
from ..services import DisbursementStatusService, RequestValidation

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementStatusController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_service = DisbursementStatusService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Status"]

        self.router.add_api_route(
            "/get_disbursement_status",
            self.get_disbursement_status,
            responses={200: {"model": DisbursementStatusResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/get_disbursement_batch_control",
            self.get_disbursement_batch_control,
            responses={200: {"model": DisbursementBatchControlResponse}},
            methods=["POST"],
        )

    async def get_disbursement_status(
        self,
        disbursement_status_request: DisbursementStatusRequest,
        is_signature_valid: Annotated[bool, Depends(JWTSignatureValidator())],
    ) -> DisbursementStatusResponse:
        _logger.info("Retrieving disbursement envelope status")
        try:
            RequestValidation.get_component().validate_signature(is_signature_valid)
            RequestValidation.get_component().validate_request(disbursement_status_request)

            disbursement_status_payloads: List[DisbursementStatusPayload] = (
                await self.disbursement_service.get_disbursement_status_payloads(disbursement_status_request)
            )
            disbursement_status_response: DisbursementStatusResponse = (
                await self.disbursement_service.construct_disbursement_status_success_response(
                    disbursement_status_request, disbursement_status_payloads
                )
            )
            _logger.info("Disbursements cancelled successfully")
            return disbursement_status_response
        except RequestValidationException as e:
            _logger.error("Error validating request")
            error_response: DisbursementStatusResponse = (
                await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                    disbursement_status_request, e.code
                )
            )
            return error_response
        except DisbursementException as e:
            error_response: DisbursementStatusResponse = (
                await self.disbursement_service.construct_disbursement_status_error_response(
                    disbursement_status_request, e.code
                )
            )
            return error_response

    async def get_disbursement_batch_control(
        self,
        disbursement_batch_control_request: DisbursementBatchControlRequest,
        is_signature_valid: Annotated[bool, Depends(JWTSignatureValidator())],
    ) -> DisbursementBatchControlResponse:
        _logger.info("Retrieving disbursement batch status")
        try:
            RequestValidation.get_component().validate_signature(is_signature_valid)
            RequestValidation.get_component().validate_request(disbursement_batch_control_request)
            disbursement_batch_control_payload: DisbursementBatchControlPayload = (
                await self.disbursement_service.get_disbursement_batch_control_payload(
                    disbursement_batch_control_request
                )
            )
            disbursement_batch_control_response: DisbursementBatchControlResponse = (
                await self.disbursement_service.construct_disbursement_batch_control_success_response(
                    disbursement_batch_control_request,
                    disbursement_batch_control_payload,
                )
            )
            return disbursement_batch_control_response
        except RequestValidationException as e:
            _logger.error("Error validating request")
            disbursement_batch_control_response: DisbursementBatchControlResponse = (
                await self.disbursement_service.construct_disbursement_batch_control_error_response(
                    disbursement_batch_control_request, e.code
                )
            )
            return disbursement_batch_control_response
        except Exception as e:
            _logger.error(f"Error retrieving disbursement batch status: {e}")
            disbursement_batch_control_response: DisbursementBatchControlResponse = (
                await self.disbursement_service.construct_disbursement_batch_control_error_response(
                    disbursement_batch_control_request, "internal_error"
                )
            )
            return disbursement_batch_control_response
