import logging
from datetime import datetime

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_spar_models.models import IdFaMapping
from openg2p_spar_models.schemas import (
    LinkRequest,
    ResolveRequest,
    ResolveScope,
    ResolveStatusReasonCode,
    SingleLinkRequest,
    SingleLinkResponse,
    SingleResolveRequest,
    SingleResolveResponse,
    SingleUnlinkRequest,
    SingleUnlinkResponse,
    SingleUpdateRequest,
    SingleUpdateResponse,
    StatusEnum,
    UnlinkRequest,
    UpdateRequest,
)
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from ..exceptions import (
    LinkValidationException,
    ResolveValidationException,
    UnlinkValidationException,
    UpdateValidationException,
)
from .id_fa_mapping_validations import IdFaMappingValidations

_logger = logging.getLogger("spar-mapper")


class MapperService(BaseService):
    async def link(self, link_request: LinkRequest):
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            link_request_payload = link_request.request_body.request_payload
            mappings_to_add = []
            single_link_responses: list[SingleLinkResponse] = []

            for single_link_request in link_request_payload.link_request:
                try:
                    await IdFaMappingValidations.get_component().validate_link_request(
                        connection=session, single_link_request=single_link_request
                    )
                    mappings_to_add.append(
                        self.construct_id_fa_mapping(single_link_request)
                    )
                    single_link_responses.append(
                        self.construct_single_link_response_for_success(
                            single_link_request
                        )
                    )
                except LinkValidationException as e:
                    single_link_responses.append(
                        self.construct_single_link_response_for_failure(
                            single_link_request, e
                        )
                    )
            session.add_all(mappings_to_add)
            await session.commit()
            return single_link_responses

    def construct_id_fa_mapping(self, single_link_request: SingleLinkRequest):
        return IdFaMapping(
            id_value=single_link_request.id,
            fa_value=single_link_request.fa,
            name=single_link_request.name,
            phone=single_link_request.phone_number,
            additional_info=single_link_request.additional_info,
            active=True,
        )

    def construct_single_link_response_for_success(
        self, single_link_request: SingleLinkRequest
    ):
        return SingleLinkResponse(
            reference_id=single_link_request.reference_id,
            timestamp=datetime.now(),
            fa=single_link_request.fa,
            status=StatusEnum.succ,
            status_reason_code=None,
            status_reason_message=None,
            additional_info=None,
            locale=single_link_request.locale,
        )

    def construct_single_link_response_for_failure(
        self, single_link_request: SingleLinkRequest, error
    ):
        return SingleLinkResponse(
            reference_id=single_link_request.reference_id,
            timestamp=datetime.now(),
            fa=single_link_request.fa,
            status=StatusEnum.rjct,
            status_reason_code=error.validation_error_type,
            status_reason_message=error.message,
            additional_info=None,
            locale=single_link_request.locale,
        )

    async def update(self, update_request: UpdateRequest):
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            update_request_payload = update_request.request_body.request_payload
            single_update_responses: list[SingleUpdateResponse] = []
            for single_update_request in update_request_payload.update_request:
                try:
                    await IdFaMappingValidations.get_component().validate_update_request(
                        connection=session, single_update_request=single_update_request
                    )
                    single_update_responses.append(
                        self.construct_single_update_response_for_success(
                            single_update_request
                        )
                    )
                    await self.update_mapping(session, single_update_request)
                except UpdateValidationException as e:
                    single_update_responses.append(
                        self.construct_single_update_response_for_failure(
                            single_update_request, e
                        )
                    )
            await session.commit()
            return single_update_responses

    async def update_mapping(self, session, single_update_request: SingleUpdateRequest):
        result = await session.execute(
            select(IdFaMapping).where(IdFaMapping.id_value == single_update_request.id)
        )
        result = result.scalar()
        if result:
            if single_update_request.fa:
                result.fa_value = single_update_request.fa
            if single_update_request.name:
                result.name = single_update_request.name
            if single_update_request.phone_number:
                result.phone = single_update_request.phone_number
            if single_update_request.additional_info:
                result.additional_info = single_update_request.additional_info
        await session.commit()

    def construct_single_update_response_for_success(
        self, single_update_request: SingleUpdateRequest
    ):
        return SingleUpdateResponse(
            id=single_update_request.id,
            reference_id=single_update_request.reference_id,
            timestamp=datetime.now(),
            status=StatusEnum.succ,
            status_reason_code=None,
            status_reason_message=None,
            additional_info=None,
            locale=single_update_request.locale,
        )

    def construct_single_update_response_for_failure(
        self, single_update_request: SingleUpdateRequest, error
    ):
        return SingleUpdateResponse(
            reference_id=single_update_request.reference_id,
            timestamp=datetime.now(),
            id=single_update_request.id,
            status=StatusEnum.rjct,
            status_reason_code=error.validation_error_type,
            status_reason_message=error.message,
            additional_info=None,
            locale=single_update_request.locale,
        )

    async def resolve(self, resolve_request: ResolveRequest):
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            resolve_request_payload = resolve_request.request_body.request_payload
            id_values = [
                req.id for req in resolve_request_payload.resolve_request if req.id
            ]
            validated_requests = []
            single_resolve_responses = []
            for single_resolve_request in resolve_request_payload.resolve_request:
                try:
                    await IdFaMappingValidations.get_component().validate_resolve_request(
                        single_resolve_request=single_resolve_request
                    )
                    validated_request = single_resolve_request
                    validated_requests.append(validated_request)
                except ResolveValidationException as e:
                    single_resolve_responses.append(
                        self.construct_single_resolve_response_for_failure(
                            single_resolve_request, e
                        )
                    )

            if validated_requests:
                stmt, results = await self.construct_bulk_query(session, id_values)
                result_dict = {result.id_value: result for result in results}
                for validated_request in validated_requests:
                    result = result_dict.get(validated_request.id)
                    if result:
                        single_resolve_response = self.construct_single_resolve(
                            validated_request, result
                        )
                        single_resolve_responses.append(single_resolve_response)
                    else:
                        resolve_validation_exception = ResolveValidationException(
                            message="ID doesn't exist, please link first",
                            status=StatusEnum.succ,
                            validation_error_type=ResolveStatusReasonCode.succ_fa_not_linked_to_id,
                        )
                        single_resolve_responses.append(
                            self.construct_single_resolve_response_for_failure(
                                validated_request, resolve_validation_exception
                            )
                        )
            await session.commit()
        return single_resolve_responses

    def construct_single_resolve(
        self, single_resolve_request: SingleResolveRequest, result
    ) -> SingleResolveResponse:
        single_response = self.construct_single_resolve_response_for_success(
            single_resolve_request
        )
        if result:
            if single_resolve_request.scope == ResolveScope.details:
                single_response.status = StatusEnum.succ
                single_response.additional_info = result.additional_info
                single_response.fa = result.fa_value
                single_response.id = result.id_value
                single_response.status_reason_code = (
                    ResolveStatusReasonCode.succ_id_active
                )
            else:
                single_response.status = StatusEnum.succ
                single_response.status_reason_code = (
                    ResolveStatusReasonCode.succ_id_active
                )
        else:
            single_response.status = StatusEnum.succ
            single_response.status_reason_code = (
                ResolveStatusReasonCode.succ_id_not_found
            )
            single_response.status_reason_message = (
                "Mapping not found against given ID."
            )
        return single_response

    async def construct_bulk_query(self, session, id_values):
        stmt = select(IdFaMapping).where(IdFaMapping.id_value.in_(id_values))
        result = await session.execute(stmt)
        result = result.scalars().all()
        return stmt, result

    def construct_single_resolve_response_for_success(
        self, single_resolve_request: SingleResolveRequest
    ):
        return SingleResolveResponse(
            id=single_resolve_request.id,
            reference_id=single_resolve_request.reference_id,
            timestamp=datetime.now(),
            fa=None,
            status=StatusEnum.succ,
            status_reason_code=None,
            status_reason_message=None,
            additional_info=single_resolve_request.additional_info,
            locale=single_resolve_request.locale,
        )

    def construct_single_resolve_response_for_failure(
        self, single_resolve_request: SingleResolveRequest, error
    ):
        return SingleResolveResponse(
            reference_id=single_resolve_request.reference_id,
            timestamp=datetime.now(),
            fa=None,
            status=error.status,
            status_reason_code=error.validation_error_type,
            status_reason_message=error.message,
            additional_info=None,
            locale=single_resolve_request.locale,
        )

    async def unlink(self, unlink_request: UnlinkRequest):
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            unlink_request_payload = unlink_request.request_body.request_payload
            single_unlink_responses: list[SingleUnlinkResponse] = []
            for single_unlink_request in unlink_request_payload.unlink_request:
                try:
                    await IdFaMappingValidations.get_component().validate_unlink_request(
                        connection=session, single_unlink_request=single_unlink_request
                    )
                    await session.execute(
                        delete(IdFaMapping).where(
                            IdFaMapping.id_value == single_unlink_request.id
                        )
                    )
                    single_unlink_responses.append(
                        self.construct_single_unlink_response_for_success(
                            single_unlink_request
                        )
                    )
                except UnlinkValidationException as e:
                    single_unlink_responses.append(
                        self.construct_single_unlink_response_for_failure(
                            single_unlink_request, e
                        )
                    )
            await session.commit()
            return single_unlink_responses

    def construct_single_unlink_response_for_success(
        self, single_unlink_request: SingleUnlinkRequest
    ):
        return SingleUnlinkResponse(
            reference_id=single_unlink_request.reference_id,
            timestamp=datetime.now(),
            fa=single_unlink_request.fa,
            status=StatusEnum.succ,
            status_reason_code=None,
            status_reason_message=None,
            additional_info=None,
            locale=single_unlink_request.locale,
        )

    def construct_single_unlink_response_for_failure(
        self, single_unlink_request: SingleUnlinkRequest, error
    ):
        return SingleUnlinkResponse(
            reference_id=single_unlink_request.reference_id,
            timestamp=datetime.now(),
            fa=single_unlink_request.fa,
            status=StatusEnum.rjct,
            status_reason_code=error.validation_error_type,
            status_reason_message=error.message,
            additional_info=None,
            locale=single_unlink_request.locale,
        )
