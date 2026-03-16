import logging
from datetime import datetime

from openg2p_fastapi_common.schemas import G2PResponseHeader, G2PResponseStatus
from openg2p_fastapi_common.service import BaseService
from openg2p_spar_models.models import Bank, Branch, WalletServiceProvider
from openg2p_spar_models.schemas import (
    BankSchema,
    BanksRequest,
    BanksResponse,
    BanksResponseBody,
    BanksResponsePayload,
    BranchesRequest,
    BranchesResponse,
    BranchesResponseBody,
    BranchesResponsePayload,
    BranchSchema,
    WalletServiceProviderSchema,
    WalletServiceProvidersRequest,
    WalletServiceProvidersResponse,
    WalletServiceProvidersResponseBody,
    WalletServiceProvidersResponsePayload,
)

_logger = logging.getLogger("spar-mapper")


class DfspService(BaseService):
    """
    Service for managing DFSP (Digital Financial Service Provider) data.

    Handles retrieval of banks, branches, and wallet service providers.
    """

    async def fetch_banks(self, banks_request: BanksRequest) -> BanksResponse:
        """
        Fetch all banks.

        Args:
            banks_request: The banks request

        Returns:
            BanksResponse with list of banks
        """
        try:
            _logger.debug("Fetching all banks")
            banks = await Bank.get_all()
            _logger.debug(f"Found {len(banks)} banks")

            bank_schemas = [BankSchema.model_validate(bank.__dict__) for bank in banks]

            response_payload = BanksResponsePayload(banks=bank_schemas)
            response_body = BanksResponseBody(response_payload=response_payload)

            response_header = G2PResponseHeader(
                request_id=banks_request.request_header.request_id,
                response_status=G2PResponseStatus.SUCCESS,
                response_error_code=None,
                response_error_message=None,
                response_timestamp=datetime.now(),
            )

            return BanksResponse(
                response_header=response_header, response_body=response_body
            )
        except Exception as e:
            _logger.error(f"Error fetching banks: {str(e)}")
            raise

    async def fetch_branches(
        self, branches_request: BranchesRequest
    ) -> BranchesResponse:
        """
        Fetch all branches, optionally filtered by bank_id.

        Args:
            branches_request: The branches request

        Returns:
            BranchesResponse with list of branches
        """
        try:
            bank_id = branches_request.request_body.request_payload.bank_id
            _logger.debug(f"Fetching branches with bank_id filter: {bank_id}")

            if bank_id:
                branches = await Branch.get_by_bank_id(bank_id)
            else:
                branches = await Branch.get_all()

            _logger.debug(f"Found {len(branches)} branches")

            branch_schemas = [
                BranchSchema.model_validate(branch.__dict__) for branch in branches
            ]

            response_payload = BranchesResponsePayload(branches=branch_schemas)
            response_body = BranchesResponseBody(response_payload=response_payload)

            response_header = G2PResponseHeader(
                request_id=branches_request.request_header.request_id,
                response_status=G2PResponseStatus.SUCCESS,
                response_error_code=None,
                response_error_message=None,
                response_timestamp=datetime.now(),
            )

            return BranchesResponse(
                response_header=response_header, response_body=response_body
            )
        except Exception as e:
            _logger.error(f"Error fetching branches: {str(e)}")
            raise

    async def fetch_wallet_service_providers(
        self, wsp_request: WalletServiceProvidersRequest
    ) -> WalletServiceProvidersResponse:
        """
        Fetch all wallet service providers.

        Args:
            wsp_request: The wallet service providers request

        Returns:
            WalletServiceProvidersResponse with list of wallet service providers
        """
        try:
            _logger.debug("Fetching all wallet service providers")
            providers = await WalletServiceProvider.get_all()
            _logger.debug(f"Found {len(providers)} wallet service providers")

            provider_schemas = [
                WalletServiceProviderSchema.model_validate(provider.__dict__)
                for provider in providers
            ]

            response_payload = WalletServiceProvidersResponsePayload(
                wallet_service_providers=provider_schemas
            )
            response_body = WalletServiceProvidersResponseBody(
                response_payload=response_payload
            )

            response_header = G2PResponseHeader(
                request_id=wsp_request.request_header.request_id,
                response_status=G2PResponseStatus.SUCCESS,
                response_error_code=None,
                response_error_message=None,
                response_timestamp=datetime.now(),
            )

            return WalletServiceProvidersResponse(
                response_header=response_header, response_body=response_body
            )
        except Exception as e:
            _logger.error(f"Error fetching wallet service providers: {str(e)}")
            raise
