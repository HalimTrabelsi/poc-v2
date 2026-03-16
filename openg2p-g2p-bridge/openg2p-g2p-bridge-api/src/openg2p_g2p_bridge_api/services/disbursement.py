import asyncio
import logging
import random
from datetime import datetime
from typing import List

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    CancellationStatus,
    Disbursement,
    DisbursementBatchControl,
    DisbursementCancellationStatus,
    DisbursementEnvelope,
    EnvelopeControl,
)
from openg2p_g2p_bridge_models.models.common_enums import ProcessStatus
from openg2p_g2p_bridge_models.schemas import (
    DisbursementPayload,
    DisbursementRequest,
    DisbursementResponse,
)
from openg2p_g2pconnect_common_lib.schemas import (
    StatusEnum,
    SyncResponseHeader,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementService(BaseService):
    async def create_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> List[DisbursementPayload]:
        _logger.info("Creating Disbursements")
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            try:
                await self.validate_disbursement_envelope(
                    session=session,
                    disbursement_payloads=disbursement_request.message,
                )
            except DisbursementException as e:
                _logger.error(f"Error validating disbursement envelope: {str(e)}")
                raise e
            is_error_free = await self.validate_disbursement_request(
                disbursement_payloads=disbursement_request.message
            )

            if not is_error_free:
                _logger.error("Error validating disbursement request")
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.message,
                )
            try:
                disbursement_envelope = (
                    (
                        await session.execute(
                            select(DisbursementEnvelope).where(
                                DisbursementEnvelope.id
                                == str(disbursement_request.message[0].disbursement_envelope_id)
                            )
                        )
                    )
                    .scalars()
                    .first()
                )

                disbursement_batch_control: DisbursementBatchControl = (
                    await self.construct_disbursement_batch_control(
                        disbursement_request.disbursement_batch_control_id,
                        disbursement_envelope=disbursement_envelope,
                    )
                )

                disbursements: List[Disbursement] = await self.construct_disbursements(
                    disbursement_payloads=disbursement_request.message,
                    disbursement_batch_control_id=disbursement_batch_control.id,
                )
                _logger.info(f"***Length of disbursements before updating: {len(disbursements)}***")
                # Lock the envelope batch status row for update (nowait)
                envelope_control = await self.update_envelope_control(disbursements, session)
                session.add(disbursement_batch_control)
                session.add_all(disbursements)
                session.add(envelope_control)

                # No need to create a separate bank disbursement status; this is now handled by DisbursementBatchControl
                await session.commit()
                _logger.info("Disbursements Created Successfully!")
                return disbursement_request.message
            except Exception as e:
                _logger.error(f"Disbursement creation failed: {str(e)}")
                session.rollback()
                raise e

    async def update_envelope_control(self, disbursements, session):
        _logger.info("Updating Envelope Control")
        max_retries = 5
        last_exc = None

        while max_retries:
            try:
                result = await session.execute(
                    select(EnvelopeControl)
                    .where(
                        EnvelopeControl.disbursement_envelope_id
                        == str(disbursements[0].disbursement_envelope_id)
                    )
                    .with_for_update(nowait=True)
                )
                envelope_control = result.scalars().first()
                await asyncio.sleep(2)
                break

            except Exception as e:
                last_exc = e
                wait = random.randint(2, 5)
                _logger.warning(
                    f"Lock attempt failed updating envelope control: {e}. "
                    f"{max_retries} retries left, sleeping {wait}s…"
                )
                await asyncio.sleep(wait)
                max_retries -= 1

        else:
            _logger.error("Unable to acquire lock on EnvelopeControl after retries")
            raise last_exc
        _logger.info(f"***Length of disbursements inside: {len(disbursements)}***")
        envelope_control.number_of_disbursements_received += len(disbursements)
        envelope_control.total_disbursement_quantity_received += sum(
            d.disbursement_quantity for d in disbursements
        )
        _logger.info("Envelope Control Updated!")
        return envelope_control

    async def construct_disbursements(
        self,
        disbursement_payloads: List[DisbursementPayload],
        disbursement_batch_control_id: str = None,
    ) -> List[Disbursement]:
        _logger.info("Constructing Disbursements")
        disbursements: List[Disbursement] = []
        for disbursement_payload in disbursement_payloads:
            disbursement = Disbursement(
                id=disbursement_payload.disbursement_id,
                disbursement_envelope_id=str(disbursement_payload.disbursement_envelope_id),
                beneficiary_id=disbursement_payload.beneficiary_id,
                beneficiary_name=disbursement_payload.beneficiary_name,
                disbursement_quantity=disbursement_payload.disbursement_quantity,
                compute_elements=disbursement_payload.compute_elements,
                narrative=disbursement_payload.narrative,
                disbursement_cycle_id=disbursement_payload.disbursement_cycle_id,
                disbursement_batch_control_id=disbursement_batch_control_id,
            )
            disbursements.append(disbursement)
            _logger.info(f"Compute elements for this disbursements: {disbursement.compute_elements}")
        _logger.info("Disbursements Constructed!")
        return disbursements

    async def construct_disbursement_batch_control(
        self,
        disbursement_batch_control_id: str,
        disbursement_envelope: DisbursementEnvelope,
    ):
        _logger.info("Constructing Disbursement Batch Control")

        id = disbursement_batch_control_id
        # Determine statuses based on benefit_type
        if disbursement_envelope.benefit_type == BenefitType.CASH_DIGITAL.value:
            fa_resolution_status = ProcessStatus.PENDING.value
            sponsor_bank_dispatch_status = ProcessStatus.NOT_APPLICABLE.value
            geo_resolutuon_status = ProcessStatus.NOT_APPLICABLE.value
            warehouse_allocation_status = ProcessStatus.NOT_APPLICABLE.value
            agency_allocation_status = ProcessStatus.NOT_APPLICABLE.value

        elif disbursement_envelope.benefit_type == BenefitType.CASH_PHYSICAL.value:
            fa_resolution_status = ProcessStatus.NOT_APPLICABLE.value
            sponsor_bank_dispatch_status = ProcessStatus.NOT_APPLICABLE.value
            geo_resolutuon_status = ProcessStatus.PENDING.value
            warehouse_allocation_status = ProcessStatus.NOT_APPLICABLE.value
            agency_allocation_status = ProcessStatus.NOT_APPLICABLE.value

        else:
            fa_resolution_status = ProcessStatus.NOT_APPLICABLE.value
            sponsor_bank_dispatch_status = ProcessStatus.NOT_APPLICABLE.value
            geo_resolutuon_status = ProcessStatus.PENDING.value
            warehouse_allocation_status = ProcessStatus.NOT_APPLICABLE.value
            agency_allocation_status = ProcessStatus.NOT_APPLICABLE.value

        disbursement_batch_control = DisbursementBatchControl(
            id=id,
            disbursement_cycle_id=disbursement_envelope.disbursement_cycle_id,
            disbursement_envelope_id=disbursement_envelope.id,
            fa_resolution_status=fa_resolution_status,
            sponsor_bank_dispatch_status=sponsor_bank_dispatch_status,
            geo_resolution_status=geo_resolutuon_status,
            warehouse_allocation_status=warehouse_allocation_status,
            agency_allocation_status=agency_allocation_status,
            # The following fields are set to None or 0 by default
            fa_resolution_timestamp=None,
            fa_resolution_latest_error_code=None,
            fa_resolution_attempts=0,
            sponsor_bank_dispatch_timestamp=None,
            sponsor_bank_dispatch_latest_error_code=None,
            sponsor_bank_dispatch_attempts=0,
            geo_resolution_timestamp=None,
            geo_resolution_latest_error_code=None,
            geo_resolution_attempts=0,
            warehouse_allocation_timestamp=None,
            warehouse_allocation_latest_error_code=None,
            warehouse_allocation_attempts=0,
            agency_allocation_timestamp=None,
            agency_allocation_latest_error_code=None,
        )
        _logger.info("Disbursement Batch Control Constructed!")
        return disbursement_batch_control

    async def validate_disbursement_request(self, disbursement_payloads: List[DisbursementPayload]):
        _logger.info("Validating Disbursement Request")
        absolutely_no_error = True

        for disbursement_payload in disbursement_payloads:
            disbursement_payload.response_error_codes = []
            if disbursement_payload.disbursement_envelope_id is None:
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ENVELOPE_ID
                )
                _logger.debug(
                    f"Invalid Disbursement Envelope ID: {disbursement_payload.disbursement_envelope_id}"
                )
            if disbursement_payload.disbursement_quantity < 0:
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_QUANTITY
                )
            if disbursement_payload.beneficiary_id is None or disbursement_payload.beneficiary_id == "":
                disbursement_payload.response_error_codes.append(G2PBridgeErrorCodes.INVALID_BENEFICIARY_ID)
            if disbursement_payload.narrative is None or disbursement_payload.narrative == "":
                disbursement_payload.response_error_codes.append(G2PBridgeErrorCodes.INVALID_NARRATIVE)

            if len(disbursement_payload.response_error_codes) > 0:
                absolutely_no_error = False
        _logger.info("Disbursement request validated!")
        return absolutely_no_error

    async def validate_disbursement_envelope(self, session, disbursement_payloads: List[DisbursementPayload]):
        _logger.info("Validating Disbursement Envelope")
        disbursement_envelope_id = disbursement_payloads[0].disbursement_envelope_id
        if not all(
            disbursement_payload.disbursement_envelope_id == disbursement_envelope_id
            for disbursement_payload in disbursement_payloads
        ):
            raise DisbursementException(
                G2PBridgeErrorCodes.MULTIPLE_ENVELOPES_FOUND,
                disbursement_payloads,
            )
        disbursement_envelope = (
            (
                await session.execute(
                    select(DisbursementEnvelope).where(
                        DisbursementEnvelope.id == str(disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )
        if not disbursement_envelope:
            _logger.error("Disbursement Envelope Not Found!")
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_NOT_FOUND,
                disbursement_payloads,
            )

        if disbursement_envelope.cancellation_status == CancellationStatus.CANCELLED:
            _logger.error("Disbursement Envelope Already Canceled!")
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_ALREADY_CANCELED,
                disbursement_payloads,
            )

        envelope_control = (
            (
                await session.execute(
                    select(EnvelopeControl).where(
                        EnvelopeControl.disbursement_envelope_id == str(disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )

        no_of_disbursements_after_this_request = (
            len(disbursement_payloads) + envelope_control.number_of_disbursements_received
        )
        total_disbursement_quantity_after_this_request = (
            sum(
                [disbursement_payload.disbursement_quantity for disbursement_payload in disbursement_payloads]
            )
            + envelope_control.total_disbursement_quantity_received
        )

        if no_of_disbursements_after_this_request > disbursement_envelope.number_of_disbursements:
            _logger.error("Number of Disbursements Exceeds Declared!")
            raise DisbursementException(
                G2PBridgeErrorCodes.NO_OF_DISBURSEMENTS_EXCEEDS_DECLARED,
                disbursement_payloads,
            )

        if total_disbursement_quantity_after_this_request > disbursement_envelope.total_disbursement_quantity:
            raise DisbursementException(
                G2PBridgeErrorCodes.TOTAL_DISBURSEMENT_QUANTITY_EXCEEDS_DECLARED,
                disbursement_payloads,
            )
        _logger.info("Disbursement envelope validated!")
        return True

    async def construct_disbursement_error_response(
        self,
        disbursement_request: DisbursementRequest,
        code: G2PBridgeErrorCodes,
        disbursement_payloads: List[DisbursementPayload],
    ) -> DisbursementResponse:
        _logger.info("Constructing Disbursement Error Response")
        disbursement_response: DisbursementResponse = DisbursementResponse(
            header=SyncResponseHeader(
                message_id=disbursement_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_request.header.action,
                status=StatusEnum.rjct,
                status_reason_message=code.value,
            ),
            message=disbursement_payloads,
        )
        _logger.info("Disbursement Error Response Constructed!")
        return disbursement_response

    async def construct_disbursement_success_response(
        self,
        disbursement_request: DisbursementRequest,
        disbursement_payloads: List[DisbursementPayload],
    ) -> DisbursementResponse:
        _logger.info("Constructing Disbursement Success Response")
        disbursement_response: DisbursementResponse = DisbursementResponse(
            header=SyncResponseHeader(
                message_id=disbursement_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_request.header.action,
                status=StatusEnum.succ,
            ),
            message=disbursement_payloads,
        )
        _logger.info("Disbursement Success Response Constructed!")
        return disbursement_response

    async def cancel_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> List[DisbursementPayload]:
        _logger.info("Cancelling Disbursements")
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            is_payload_valid = await self.validate_request_payload(
                disbursement_payloads=disbursement_request.message
            )

            if not is_payload_valid:
                _logger.error("Error validating disbursement request")
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.message,
                )

            # Fetch and lock disbursements for update (nowait)
            disbursements_in_db: List[Disbursement] = await self.fetch_disbursements_from_db(
                disbursement_request, session
            )
            if not disbursements_in_db:
                _logger.error("Disbursements not found in DB")
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID,
                    disbursement_payloads=disbursement_request.message,
                )

            try:
                await self.check_for_single_envelope(disbursements_in_db, disbursement_request.message)
            except DisbursementException as e:
                _logger.error(f"Error checking for single envelope: {str(e)}")
                raise e

            try:
                await self.validate_envelope_for_disbursement_cancellation(
                    disbursements_in_db=disbursements_in_db,
                    disbursement_payloads=disbursement_request.message,
                    session=session,
                )
            except DisbursementException as e:
                _logger.error(f"Error validating envelope for disbursement cancellation: {str(e)}")
                raise e

            invalid_disbursements_exist = await self.check_for_invalid_disbursements(
                disbursement_request, disbursements_in_db
            )
            if invalid_disbursements_exist:
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.message,
                )

            for disbursement in disbursements_in_db:
                disbursement.cancellation_status = DisbursementCancellationStatus.CANCELLED
                disbursement.cancellation_time_stamp = datetime.now()

            # Lock the envelope batch status row for update (nowait)
            envelope_control = (
                (
                    await session.execute(
                        select(EnvelopeControl)
                        .where(
                            EnvelopeControl.disbursement_envelope_id
                            == str(disbursements_in_db[0].disbursement_envelope_id)
                        )
                        .with_for_update(nowait=True)
                    )
                )
                .scalars()
                .first()
            )
            envelope_control.number_of_disbursements_received -= len(disbursements_in_db)
            envelope_control.total_disbursement_quantity_received -= sum(
                [disbursement.disbursement_quantity for disbursement in disbursements_in_db]
            )

            session.add_all(disbursements_in_db)
            session.add(envelope_control)
            await session.commit()
            _logger.info("Disbursements Cancelled Successfully!")
            return disbursement_request.message

    async def check_for_single_envelope(self, disbursements_in_db, disbursement_payloads):
        _logger.info("Checking for Single Envelope")
        disbursement_envelope_ids = {
            disbursement.disbursement_envelope_id for disbursement in disbursements_in_db
        }
        if len(disbursement_envelope_ids) > 1:
            _logger.error("Multiple Envelopes Found!")
            raise DisbursementException(
                G2PBridgeErrorCodes.MULTIPLE_ENVELOPES_FOUND,
                disbursement_payloads,
            )
        _logger.info("Single Envelope Found!")
        return True

    async def check_for_invalid_disbursements(self, disbursement_request, disbursements_in_db) -> bool:
        _logger.info("Checking for Invalid Disbursements")
        invalid_disbursements_exist = False
        for disbursement_payload in disbursement_request.message:
            if disbursement_payload.disbursement_id not in [
                disbursement.disbursement_id for disbursement in disbursements_in_db
            ]:
                invalid_disbursements_exist = True
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID.value
                )
            if disbursement_payload.disbursement_id in [
                disbursement.disbursement_id
                for disbursement in disbursements_in_db
                if disbursement.cancellation_status == DisbursementCancellationStatus.CANCELLED
            ]:
                invalid_disbursements_exist = True
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.DISBURSEMENT_ALREADY_CANCELED.value
                )
        _logger.info("Invalid Disbursements Checked!")
        return invalid_disbursements_exist

    async def fetch_disbursements_from_db(self, disbursement_request, session) -> List[Disbursement]:
        _logger.info("Fetching Disbursements from DB")
        max_retries = 5
        last_exc = None

        while max_retries:
            try:
                result = await session.execute(
                    select(Disbursement)
                    .where(
                        Disbursement.disbursement_id.in_(
                            [str(p.disbursement_id) for p in disbursement_request.message]
                        )
                    )
                    .with_for_update(nowait=True)
                )
                disbursements_in_db = result.scalars().all()
                break

            except OperationalError as e:
                last_exc = e
                wait = random.randint(8, 15)
                _logger.warning(
                    f"Lock attempt failed fetching disbursements: {e}. "
                    f"{max_retries} retries left, sleeping {wait}s…"
                )
                await asyncio.sleep(wait)
                max_retries -= 1

        else:
            _logger.error("Unable to acquire lock on Disbursement rows after retries")
            raise last_exc

        _logger.info("Disbursements Fetched from DB!")
        return disbursements_in_db

    async def validate_envelope_for_disbursement_cancellation(
        self,
        disbursements_in_db,
        disbursement_payloads: List[DisbursementPayload],
        session,
    ):
        _logger.info("Validating Envelope for Disbursement Cancellation")
        max_retries = 5
        last_exc = None

        while max_retries:
            try:
                result = await session.execute(
                    select(DisbursementEnvelope)
                    .where(DisbursementEnvelope.id == str(disbursements_in_db[0].disbursement_envelope_id))
                    .with_for_update(nowait=True)
                )
                disbursement_envelope = result.scalars().first()
                break

            except OperationalError as e:
                last_exc = e
                wait = random.randint(8, 15)
                _logger.warning(
                    f"Lock attempt failed on DisbursementEnvelope: {e}. "
                    f"{max_retries} retries left, sleeping {wait}s…"
                )
                await asyncio.sleep(wait)
                max_retries -= 1

        else:
            _logger.error("Unable to lock DisbursementEnvelope after retries")
            raise last_exc

        if not disbursement_envelope:
            _logger.error("Disbursement Envelope Not Found!")
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_NOT_FOUND,
                disbursement_payloads,
            )

        if disbursement_envelope.cancellation_status == CancellationStatus.CANCELLED:
            _logger.error("Disbursement Envelope Already Canceled!")
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_ALREADY_CANCELED,
                disbursement_payloads,
            )

        if disbursement_envelope.disbursement_schedule_date <= datetime.now().date():
            _logger.error("Disbursement Envelope Schedule Date Reached!")
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_SCHEDULE_DATE_REACHED,
                disbursement_payloads,
            )

        # we don't need a lock for this read
        envelope_control = (
            (
                await session.execute(
                    select(EnvelopeControl).where(
                        EnvelopeControl.disbursement_envelope_id
                        == str(disbursements_in_db[0].disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )

        no_of_after = envelope_control.number_of_disbursements_received - len(disbursements_in_db)
        total_amt_after = envelope_control.total_disbursement_quantity_received - sum(
            d.disbursement_quantity for d in disbursements_in_db
        )

        if no_of_after < 0:
            _logger.error("Number of Disbursements Less Than Zero!")
            raise DisbursementException(
                G2PBridgeErrorCodes.NO_OF_DISBURSEMENTS_LESS_THAN_ZERO,
                disbursement_payloads,
            )

        if total_amt_after < 0:
            _logger.error("Total Disbursement Quantity Less Than Zero!")
            raise DisbursementException(
                G2PBridgeErrorCodes.TOTAL_DISBURSEMENT_QUANTITY_LESS_THAN_ZERO,
                disbursement_payloads,
            )

        _logger.info("Envelope Validated for Disbursement Cancellation!")
        return True
