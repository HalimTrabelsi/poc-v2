from datetime import datetime

from openg2p_fastapi_common.schemas import (
    G2PResponseHeader,
    G2PResponseStatus,
)
from openg2p_fastapi_common.service import BaseService
from openg2p_spar_models.schemas import (
    LinkRequest,
    LinkResponse,
    LinkResponseBody,
    LinkResponsePayload,
    ResolveRequest,
    ResolveResponse,
    ResolveResponseBody,
    ResolveResponsePayload,
    SingleLinkResponse,
    SingleResolveResponse,
    SingleUnlinkResponse,
    SingleUpdateResponse,
    UnlinkRequest,
    UnlinkResponse,
    UnlinkResponseBody,
    UnlinkResponsePayload,
    UpdateRequest,
    UpdateResponse,
    UpdateResponseBody,
    UpdateResponsePayload,
)

from .strategy_helper import StrategyHelper


class ResponseHelper(BaseService):
    @staticmethod
    def construct_link_response(
        link_request: LinkRequest,
        single_link_responses: list[SingleLinkResponse],
    ) -> LinkResponse:
        link_request_payload = link_request.request_body.request_payload
        link_response_payload = LinkResponsePayload(
            transaction_id=link_request_payload.transaction_id,
            correlation_id=None,
            link_response=single_link_responses,
        )
        link_response_body = LinkResponseBody(response_payload=link_response_payload)

        # Create response header
        response_header = G2PResponseHeader(
            request_id=link_request.request_header.request_id,
            response_status=G2PResponseStatus.SUCCESS,
            response_error_code=None,
            response_error_message=None,
            response_timestamp=datetime.now(),
        )

        return LinkResponse(
            response_header=response_header, response_body=link_response_body
        )

    @staticmethod
    def construct_update_response(
        update_request: UpdateRequest,
        single_update_responses: list[SingleUpdateResponse],
    ) -> UpdateResponse:
        update_request_payload = update_request.request_body.request_payload
        update_response_payload = UpdateResponsePayload(
            transaction_id=update_request_payload.transaction_id,
            correlation_id=None,
            update_response=single_update_responses,
        )
        update_response_body = UpdateResponseBody(
            response_payload=update_response_payload
        )

        # Create response header
        response_header = G2PResponseHeader(
            request_id=update_request.request_header.request_id,
            response_status=G2PResponseStatus.SUCCESS,
            response_error_code=None,
            response_error_message=None,
            response_timestamp=datetime.now(),
        )

        return UpdateResponse(
            response_header=response_header, response_body=update_response_body
        )

    @staticmethod
    async def construct_resolve_response(
        resolve_request: ResolveRequest,
        single_resolve_responses: list[SingleResolveResponse],
    ) -> ResolveResponse:
        resolve_request_payload = resolve_request.request_body.request_payload

        # Deconstruct FA for each response
        deconstructed_responses = []
        for response in single_resolve_responses:
            deconstructed_fa = None
            if response.fa and response.additional_info:
                deconstructed_fa = (
                    await StrategyHelper()
                    .get_component()
                    .deconstruct_fa(response.fa, response.additional_info)
                )
            # Create a new response with deconstructed FA
            deconstructed_response = SingleResolveResponse(
                reference_id=response.reference_id,
                timestamp=response.timestamp,
                fa=deconstructed_fa,
                id=response.id,
                account_provider_info=response.account_provider_info,
                status=response.status,
                status_reason_code=response.status_reason_code,
                status_reason_message=response.status_reason_message,
                additional_info=response.additional_info,
                locale=response.locale,
            )
            deconstructed_responses.append(deconstructed_response)

        resolve_response_payload = ResolveResponsePayload(
            transaction_id=resolve_request_payload.transaction_id,
            correlation_id=None,
            resolve_response=deconstructed_responses,
        )
        resolve_response_body = ResolveResponseBody(
            response_payload=resolve_response_payload
        )

        # Create response header
        response_header = G2PResponseHeader(
            request_id=resolve_request.request_header.request_id,
            response_status=G2PResponseStatus.SUCCESS,
            response_error_code=None,
            response_error_message=None,
            response_timestamp=datetime.now(),
        )

        return ResolveResponse(
            response_header=response_header, response_body=resolve_response_body
        )

    @staticmethod
    def construct_unlink_response(
        unlink_request: UnlinkRequest,
        single_unlink_responses: list[SingleUnlinkResponse],
    ) -> UnlinkResponse:
        unlink_request_payload = unlink_request.request_body.request_payload
        unlink_response_payload = UnlinkResponsePayload(
            transaction_id=unlink_request_payload.transaction_id,
            correlation_id=None,
            unlink_response=single_unlink_responses,
        )
        unlink_response_body = UnlinkResponseBody(
            response_payload=unlink_response_payload
        )

        # Create response header
        response_header = G2PResponseHeader(
            request_id=unlink_request.request_header.request_id,
            response_status=G2PResponseStatus.SUCCESS,
            response_error_code=None,
            response_error_message=None,
            response_timestamp=datetime.now(),
        )

        return UnlinkResponse(
            response_header=response_header, response_body=unlink_response_body
        )

    @staticmethod
    def construct_link_error_response(
        link_request: LinkRequest,
        exception: Exception,
        error_code: str = None,
        error_message: str = None,
    ) -> LinkResponse:
        """
        Construct a LinkResponse error response
        """
        # Use provided error details or extract from exception
        final_error_code = error_code or getattr(
            exception, "validation_error_type", "rjct.internal.error"
        )
        final_error_message = error_message or getattr(
            exception, "message", str(exception)
        )

        # Create response header with error status
        response_header = G2PResponseHeader(
            request_id=link_request.request_header.request_id,
            response_status=G2PResponseStatus.ERROR,
            response_error_code=final_error_code,
            response_error_message=final_error_message,
            response_timestamp=datetime.now(),
        )

        # Create response body with empty list
        link_response_payload = LinkResponsePayload(
            transaction_id=link_request.request_body.request_payload.transaction_id,
            correlation_id=None,
            link_response=[],
        )
        link_response_body = LinkResponseBody(response_payload=link_response_payload)

        return LinkResponse(
            response_header=response_header, response_body=link_response_body
        )

    @staticmethod
    def construct_resolve_error_response(
        resolve_request: ResolveRequest,
        exception: Exception,
        error_code: str = None,
        error_message: str = None,
    ) -> ResolveResponse:
        """
        Construct a ResolveResponse error response
        """
        # Use provided error details or extract from exception
        final_error_code = error_code or getattr(
            exception, "validation_error_type", "rjct.internal.error"
        )
        final_error_message = error_message or getattr(
            exception, "message", str(exception)
        )

        # Create response header with error status
        response_header = G2PResponseHeader(
            request_id=resolve_request.request_header.request_id,
            response_status=G2PResponseStatus.ERROR,
            response_error_code=final_error_code,
            response_error_message=final_error_message,
            response_timestamp=datetime.now(),
        )

        # Create response body with empty list
        resolve_response_payload = ResolveResponsePayload(
            transaction_id=resolve_request.request_body.request_payload.transaction_id,
            correlation_id=None,
            resolve_response=[],
        )
        resolve_response_body = ResolveResponseBody(
            response_payload=resolve_response_payload
        )

        return ResolveResponse(
            response_header=response_header, response_body=resolve_response_body
        )

    @staticmethod
    def construct_update_error_response(
        update_request: UpdateRequest,
        exception: Exception,
        error_code: str = None,
        error_message: str = None,
    ) -> UpdateResponse:
        """
        Construct an UpdateResponse error response
        """
        # Use provided error details or extract from exception
        final_error_code = error_code or getattr(
            exception, "validation_error_type", "rjct.internal.error"
        )
        final_error_message = error_message or getattr(
            exception, "message", str(exception)
        )

        # Create response header with error status
        response_header = G2PResponseHeader(
            request_id=update_request.request_header.request_id,
            response_status=G2PResponseStatus.ERROR,
            response_error_code=final_error_code,
            response_error_message=final_error_message,
            response_timestamp=datetime.now(),
        )

        # Create response body with empty list
        update_response_payload = UpdateResponsePayload(
            transaction_id=update_request.request_body.request_payload.transaction_id,
            correlation_id=None,
            update_response=[],
        )
        update_response_body = UpdateResponseBody(
            response_payload=update_response_payload
        )

        return UpdateResponse(
            response_header=response_header, response_body=update_response_body
        )

    @staticmethod
    def construct_unlink_error_response(
        unlink_request: UnlinkRequest,
        exception: Exception,
        error_code: str = None,
        error_message: str = None,
    ) -> UnlinkResponse:
        """
        Construct an UnlinkResponse error response
        """
        # Use provided error details or extract from exception
        final_error_code = error_code or getattr(
            exception, "validation_error_type", "rjct.internal.error"
        )
        final_error_message = error_message or getattr(
            exception, "message", str(exception)
        )

        # Create response header with error status
        response_header = G2PResponseHeader(
            request_id=unlink_request.request_header.request_id,
            response_status=G2PResponseStatus.ERROR,
            response_error_code=final_error_code,
            response_error_message=final_error_message,
            response_timestamp=datetime.now(),
        )

        # Create response body with empty list
        unlink_response_payload = UnlinkResponsePayload(
            transaction_id=unlink_request.request_body.request_payload.transaction_id,
            correlation_id=None,
            unlink_response=[],
        )
        unlink_response_body = UnlinkResponseBody(
            response_payload=unlink_response_payload
        )

        return UnlinkResponse(
            response_header=response_header, response_body=unlink_response_body
        )
