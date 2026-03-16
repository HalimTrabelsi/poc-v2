import logging

from fastapi import File, UploadFile
from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.codes import (
    G2PBridgeErrorCodes,
)
from openg2p_g2p_bridge_models.errors.exceptions import (
    AccountStatementException,
    RequestValidationException,
)
from openg2p_g2p_bridge_models.schemas import (
    AccountStatementResponse,
)

from openg2p_g2p_bridge_api.services import AccountStatementService

from ..config import Settings
from ..services import RequestValidation

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class AccountStatementController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["G2P Bridge Account Statement"]
        self.account_statement_service = AccountStatementService.get_component()

        self.router.add_api_route(
            "/upload_mt940",
            self.upload_mt940,
            responses={200: {"model": AccountStatementResponse}},
            methods=["POST"],
        )

    async def upload_mt940(
        self,
        statement_file: UploadFile = File(...),
    ) -> AccountStatementResponse:
        _logger.info("Uploading statement file")
        try:
            RequestValidation.get_component().validate_request(statement_file)
            RequestValidation.get_component().validate_mt940_file(statement_file)
            account_statement_id: str = await self.account_statement_service.upload_mt940(statement_file)
            account_statement_response: AccountStatementResponse = (
                await self.account_statement_service.construct_account_statement_success_response(
                    account_statement_id
                )
            )
        except RequestValidationException:
            _logger.error("Request validation failed")
            account_statement_response: AccountStatementResponse = (
                await self.account_statement_service.construct_account_statement_error_response(
                    G2PBridgeErrorCodes.REQUEST_VALIDATION_ERROR
                )
            )
            return account_statement_response
        except AccountStatementException:
            _logger.error("Error uploading statement file")
            account_statement_response: AccountStatementResponse = (
                await self.account_statement_service.construct_account_statement_error_response(
                    G2PBridgeErrorCodes.STATEMENT_UPLOAD_ERROR
                )
            )
            return account_statement_response
        _logger.info("Statement file uploaded successfully")
        return account_statement_response
