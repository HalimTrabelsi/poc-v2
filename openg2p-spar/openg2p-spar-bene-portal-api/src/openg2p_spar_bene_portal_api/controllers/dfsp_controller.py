from openg2p_fastapi_common.controller import BaseController
from openg2p_spar_mapper_core.services import DfspService
from openg2p_spar_models.schemas import (
    BanksRequest,
    BanksResponse,
    BranchesRequest,
    BranchesResponse,
    WalletServiceProvidersRequest,
    WalletServiceProvidersResponse,
)


class DfspController(BaseController):
    """
    Controller for Bank, Branch, and WalletServiceProvider endpoints.

    Provides API endpoints for querying bank, branch, and wallet service provider data.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["Bank, Branch & Wallet"]
        self.router.prefix = "/dfsp"
        self.service = DfspService.get_component()

        # Bank endpoints
        self.router.add_api_route(
            "/fetch-banks",
            self.fetch_banks,
            responses={200: {"model": BanksResponse}},
            methods=["POST"],
            summary="Fetch all banks",
            description="Returns all available banks",
        )

        # Branch endpoints
        self.router.add_api_route(
            "/fetch-branches",
            self.fetch_branches,
            responses={200: {"model": BranchesResponse}},
            methods=["POST"],
            summary="Fetch all branches",
            description="Returns all available branches, optionally filtered by bank_id",
        )

        # WalletServiceProvider endpoints
        self.router.add_api_route(
            "/fetch-wallet-service-providers",
            self.fetch_wallet_service_providers,
            responses={200: {"model": WalletServiceProvidersResponse}},
            methods=["POST"],
            summary="Fetch all wallet service providers",
            description="Returns all available wallet service providers",
        )

    async def fetch_banks(self, banks_request: BanksRequest) -> BanksResponse:
        """
        Fetch all banks.

        Args:
            banks_request: The banks request

        Returns:
            BanksResponse with list of banks
        """
        return await self.service.fetch_banks(banks_request)

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
        return await self.service.fetch_branches(branches_request)

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
        return await self.service.fetch_wallet_service_providers(wsp_request)
