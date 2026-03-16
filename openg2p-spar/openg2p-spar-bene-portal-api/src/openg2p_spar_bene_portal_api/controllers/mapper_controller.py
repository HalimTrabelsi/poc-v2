import logging
from typing import Annotated

from fastapi import Depends
from openg2p_fastapi_auth.auth import AuthFactory
from openg2p_fastapi_auth_models.schemas import AuthCredentials
from openg2p_fastapi_common.controller import BaseController
from openg2p_spar_mapper_core.exceptions import (
    LinkValidationException,
    RequestValidationException,
    ResolveValidationException,
    UnlinkValidationException,
    UpdateValidationException,
)
from openg2p_spar_mapper_core.helpers import ResponseHelper, StrategyHelper
from openg2p_spar_mapper_core.services import MapperService, RequestValidation
from openg2p_spar_models.schemas import (
    STRATEGY_ID_KEY,
    LinkRequest,
    LinkResponse,
    ResolveRequest,
    ResolveResponse,
    UnlinkRequest,
    UnlinkResponse,
    UpdateRequest,
    UpdateResponse,
)

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class MapperController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["SPAR Bene Portal"]
        self.router.prefix = "/mapper"
        self.service = MapperService.get_component()

        self.router.add_api_route(
            "/link",
            self.link,
            responses={200: {"model": LinkResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/resolve",
            self.resolve,
            responses={200: {"model": ResolveResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/unlink",
            self.unlink,
            responses={200: {"model": UnlinkResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/update",
            self.update,
            responses={200: {"model": UpdateResponse}},
            methods=["POST"],
        )

    async def link(
        self,
        link_request: LinkRequest,
        auth_credentials: Annotated[AuthCredentials, Depends(AuthFactory)],
    ) -> LinkResponse:
        """
        Link ID to Financial Address
        """
        try:
            # Validate request structure
            RequestValidation.get_component().validate_request(link_request)

            # Construct ID from auth credentials
            constructed_id = (
                await StrategyHelper().get_component().construct_id(auth_credentials)
            )

            # Replace ID with constructed ID from auth for each request
            # Also construct FA and add additional_info for each request
            for (
                single_link_request
            ) in link_request.request_body.request_payload.link_request:
                single_link_request.id = constructed_id

                # Construct FA from the request FA object
                if single_link_request.fa:
                    # Store strategy_id before constructing FA
                    strategy_id = single_link_request.fa.strategy_id

                    # Construct FA using StrategyHelper (converts FA object to string)
                    _logger.info(
                        f"Constructing FA string from FA object... {single_link_request.fa}"
                    )
                    constructed_fa_string = (
                        await StrategyHelper()
                        .get_component()
                        .construct_fa(single_link_request.fa)
                    )
                    # Replace FA object with constructed FA string for storage
                    single_link_request.fa = constructed_fa_string

                    # Ensure additional_info contains strategy_id
                    if not single_link_request.additional_info:
                        single_link_request.additional_info = []

                    if (
                        not single_link_request.additional_info
                        or STRATEGY_ID_KEY not in single_link_request.additional_info[0]
                    ):
                        if not single_link_request.additional_info:
                            single_link_request.additional_info = [{}]
                        single_link_request.additional_info[0][
                            STRATEGY_ID_KEY
                        ] = strategy_id

            # Process link request
            single_link_responses = await self.service.link(link_request)
            _logger.debug(f"Single Link Responses: {single_link_responses}")
            # Construct response
            link_response: LinkResponse = (
                ResponseHelper.get_component().construct_link_response(
                    link_request, single_link_responses
                )
            )
            return link_response

        except RequestValidationException as e:
            return ResponseHelper.get_component().construct_link_error_response(
                link_request, e, "rjct.request.validation", str(e)
            )
        except LinkValidationException as e:
            _logger.info(
                f"Link validation error: {e.validation_error_type} - {e.message}"
            )
            return ResponseHelper.get_component().construct_link_error_response(
                link_request, e, e.validation_error_type, e.message
            )
        except Exception as e:
            _logger.error(f"Internal server error during link operation: {str(e)}")
            return ResponseHelper.get_component().construct_link_error_response(
                link_request, e, "rjct.internal.error", "Internal server error"
            )

    async def resolve(
        self,
        resolve_request: ResolveRequest,
        auth_credentials: Annotated[AuthCredentials, Depends(AuthFactory)],
    ) -> ResolveResponse:
        """
        Resolve ID to Financial Address
        """
        try:
            # Validate request structure
            RequestValidation.get_component().validate_request(resolve_request)
            # Construct ID from auth credentials
            constructed_id = (
                await StrategyHelper().get_component().construct_id(auth_credentials)
            )

            # Replace ID with constructed ID from auth for each request
            for (
                single_resolve_request
            ) in resolve_request.request_body.request_payload.resolve_request:
                if not single_resolve_request.id:
                    single_resolve_request.id = constructed_id

            # Process resolve request
            print("Processing resolve request...")
            single_resolve_responses = await self.service.resolve(resolve_request)
            _logger.info(f"Single Resolve Responses: {single_resolve_responses}")
            # Construct response
            resolve_response: ResolveResponse = (
                await ResponseHelper.get_component().construct_resolve_response(
                    resolve_request, single_resolve_responses
                )
            )
            return resolve_response

        except RequestValidationException as e:
            return ResponseHelper.get_component().construct_resolve_error_response(
                resolve_request, e, "rjct.request.validation", str(e)
            )
        except ResolveValidationException as e:
            return ResponseHelper.get_component().construct_resolve_error_response(
                resolve_request, e, e.validation_error_type, e.message
            )
        except Exception as e:
            _logger.error(f"Internal server error during resolve operation: {str(e)}")
            return ResponseHelper.get_component().construct_resolve_error_response(
                resolve_request, e, "rjct.internal.error", "Internal server error"
            )

    async def unlink(
        self,
        unlink_request: UnlinkRequest,
        auth_credentials: Annotated[AuthCredentials, Depends(AuthFactory)],
    ) -> UnlinkResponse:
        """
        Unlink ID from Financial Address
        """
        try:
            # Validate request structure
            RequestValidation.get_component().validate_request(unlink_request)

            # Construct ID from auth credentials
            constructed_id = (
                await StrategyHelper().get_component().construct_id(auth_credentials)
            )

            # Replace ID with constructed ID from auth for each request
            for (
                single_unlink_request
            ) in unlink_request.request_body.request_payload.unlink_request:
                single_unlink_request.id = constructed_id

            # Process unlink request
            single_unlink_responses = await self.service.unlink(unlink_request)

            # Construct response
            unlink_response: UnlinkResponse = (
                ResponseHelper.get_component().construct_unlink_response(
                    unlink_request, single_unlink_responses
                )
            )
            return unlink_response

        except RequestValidationException as e:
            return ResponseHelper.get_component().construct_unlink_error_response(
                unlink_request, e, "rjct.request.validation", str(e)
            )
        except UnlinkValidationException as e:
            return ResponseHelper.get_component().construct_unlink_error_response(
                unlink_request, e, e.validation_error_type, e.message
            )
        except Exception as e:
            _logger.error(f"Internal server error during unlink operation: {str(e)}")
            return ResponseHelper.get_component().construct_unlink_error_response(
                unlink_request, e, "rjct.internal.error", "Internal server error"
            )

    async def update(
        self,
        update_request: UpdateRequest,
        auth_credentials: Annotated[AuthCredentials, Depends(AuthFactory)],
    ) -> UpdateResponse:
        """
        Update ID to Financial Address mapping
        """
        try:
            # Validate request structure
            RequestValidation.get_component().validate_request(update_request)

            # Construct ID from auth credentials
            constructed_id = (
                await StrategyHelper().get_component().construct_id(auth_credentials)
            )

            # Replace ID with constructed ID from auth for each request
            # Also construct FA and add additional_info for each request
            for (
                single_update_request
            ) in update_request.request_body.request_payload.update_request:
                single_update_request.id = constructed_id

                # Construct FA from the request FA object
                if single_update_request.fa:
                    # Store strategy_id before constructing FA
                    strategy_id = single_update_request.fa.strategy_id

                    # Construct FA using StrategyHelper (converts FA object to string)
                    constructed_fa_string = (
                        await StrategyHelper()
                        .get_component()
                        .construct_fa(single_update_request.fa)
                    )
                    # Replace FA object with constructed FA string for storage
                    single_update_request.fa = constructed_fa_string

                    # Ensure additional_info contains strategy_id
                    if not single_update_request.additional_info:
                        single_update_request.additional_info = []

                    if (
                        not single_update_request.additional_info
                        or STRATEGY_ID_KEY
                        not in single_update_request.additional_info[0]
                    ):
                        if not single_update_request.additional_info:
                            single_update_request.additional_info = [{}]
                        single_update_request.additional_info[0][
                            STRATEGY_ID_KEY
                        ] = strategy_id

            # Process update request
            single_update_responses = await self.service.update(update_request)

            # Construct response
            update_response: UpdateResponse = (
                ResponseHelper.get_component().construct_update_response(
                    update_request, single_update_responses
                )
            )
            return update_response

        except RequestValidationException as e:
            return ResponseHelper.get_component().construct_update_error_response(
                update_request, e, "rjct.request.validation", str(e)
            )
        except UpdateValidationException as e:
            return ResponseHelper.get_component().construct_update_error_response(
                update_request, e, e.validation_error_type, e.message
            )
        except Exception as e:
            _logger.error(f"Internal server error during update operation: {str(e)}")
            return ResponseHelper.get_component().construct_update_error_response(
                update_request, e, "rjct.internal.error", "Internal server error"
            )
