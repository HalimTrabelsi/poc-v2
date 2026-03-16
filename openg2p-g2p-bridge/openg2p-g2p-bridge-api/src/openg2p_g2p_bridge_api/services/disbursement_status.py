import logging
from datetime import datetime
from typing import List

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementStatusException
from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementEnvelope,
    DisbursementErrorRecon,
    DisbursementRecon,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementBatchControlGeoPayload,
    DisbursementBatchControlPayload,
    DisbursementBatchControlRequest,
    DisbursementBatchControlResponse,
    DisbursementErrorReconPayload,
    DisbursementReconPayload,
    DisbursementReconRecords,
    DisbursementStatusPayload,
    DisbursementStatusRequest,
    DisbursementStatusResponse,
)
from openg2p_g2pconnect_common_lib.schemas import (
    StatusEnum,
    SyncResponseHeader,
)
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementStatusService(BaseService):
    async def get_disbursement_status_payloads(
        self, disbursement_status_request: DisbursementStatusRequest
    ) -> List[DisbursementStatusPayload]:
        _logger.info("Getting disbursement status payloads")
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            try:
                disbursement_status_payloads = []
                for disbursement_id in disbursement_status_request.message:
                    disbursement_recon_records = await self.get_disbursement_recon_records(
                        session, disbursement_id
                    )
                    disbursement_status_payload = DisbursementStatusPayload(
                        disbursement_id=disbursement_id,
                        disbursement_recon_records=disbursement_recon_records,
                    )
                    disbursement_status_payloads.append(disbursement_status_payload)
                _logger.info("Disbursement status payloads retrieved successfully")
                return disbursement_status_payloads
            except DisbursementStatusException as e:
                _logger.error("Error in getting disbursement status")
                raise e

    async def get_disbursement_recon_records(self, session, disbursement_id: str) -> DisbursementReconRecords:
        _logger.info(f"Getting disbursement recon records for disbursement ID: {disbursement_id}")
        disbursement_recon_payloads = []
        disbursement_error_recon_payloads = []

        disbursement_recon_payloads_from_db = (
            (
                await session.execute(
                    select(DisbursementRecon).where(DisbursementRecon.disbursement_id == disbursement_id)
                )
            )
            .scalars()
            .all()
        )

        for disbursement_recon_payload in disbursement_recon_payloads_from_db:
            disbursement_recon_payloads.append(
                DisbursementReconPayload(
                    bank_disbursement_batch_id=disbursement_recon_payload.bank_disbursement_batch_id,
                    disbursement_id=disbursement_recon_payload.disbursement_id,
                    disbursement_envelope_id=disbursement_recon_payload.disbursement_envelope_id,
                    beneficiary_name_from_bank=disbursement_recon_payload.beneficiary_name_from_bank,
                    remittance_reference_number=disbursement_recon_payload.remittance_reference_number,
                    remittance_statement_id=disbursement_recon_payload.remittance_statement_id,
                    remittance_statement_number=disbursement_recon_payload.remittance_statement_number,
                    remittance_statement_sequence=disbursement_recon_payload.remittance_statement_sequence,
                    remittance_entry_sequence=disbursement_recon_payload.remittance_entry_sequence,
                    remittance_entry_date=disbursement_recon_payload.remittance_entry_date,
                    remittance_value_date=disbursement_recon_payload.remittance_value_date,
                    reversal_found=disbursement_recon_payload.reversal_found,
                    reversal_statement_id=disbursement_recon_payload.reversal_statement_id,
                    reversal_statement_number=disbursement_recon_payload.reversal_statement_number,
                    reversal_statement_sequence=disbursement_recon_payload.reversal_statement_sequence,
                    reversal_entry_sequence=disbursement_recon_payload.reversal_entry_sequence,
                    reversal_entry_date=disbursement_recon_payload.reversal_entry_date,
                    reversal_value_date=disbursement_recon_payload.reversal_value_date,
                    reversal_reason=disbursement_recon_payload.reversal_reason,
                )
            )

        disbursement_error_recon_payloads_from_db = (
            (
                await session.execute(
                    select(DisbursementErrorRecon).where(
                        DisbursementErrorRecon.disbursement_id == disbursement_id
                    )
                )
            )
            .scalars()
            .all()
        )

        for disbursement_error_recon_payload in disbursement_error_recon_payloads_from_db:
            disbursement_error_recon_payloads.append(
                DisbursementErrorReconPayload(
                    statement_id=disbursement_error_recon_payload.statement_id,
                    statement_number=disbursement_error_recon_payload.statement_number,
                    statement_sequence=disbursement_error_recon_payload.statement_sequence,
                    entry_sequence=disbursement_error_recon_payload.entry_sequence,
                    entry_date=disbursement_error_recon_payload.entry_date,
                    value_date=disbursement_error_recon_payload.value_date,
                    error_reason=disbursement_error_recon_payload.error_reason,
                    disbursement_id=disbursement_error_recon_payload.disbursement_id,
                    bank_reference_number=disbursement_error_recon_payload.bank_reference_number,
                )
            )

        disbursement_recon_records = DisbursementReconRecords(
            disbursement_recon_payloads=disbursement_recon_payloads,
            disbursement_error_recon_payloads=disbursement_error_recon_payloads,
        )

        _logger.info(f"Disbursement recon records retrieved for disbursement ID: {disbursement_id}")
        return disbursement_recon_records

    async def get_disbursement_batch_control_payload(
        self, disbursement_batch_control_request: DisbursementBatchControlRequest
    ) -> DisbursementBatchControlPayload:
        _logger.info("Getting disbursement batch control payload")
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            disbursement_batch_control_payload = None
            disbursement_batch_control = (
                (
                    await session.execute(
                        select(DisbursementBatchControl).where(
                            DisbursementBatchControl.id == disbursement_batch_control_request.message
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_batch_control:
                _logger.warning("Disbursement batch control not found")
                return
            disbursement_batch_control_geos = (
                (
                    await session.execute(
                        select(DisbursementBatchControlGeo).where(
                            DisbursementBatchControlGeo.disbursement_batch_control_id
                            == disbursement_batch_control.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            disbursement_envelope = (
                (
                    await session.execute(
                        select(DisbursementEnvelope).where(
                            DisbursementEnvelope.id == disbursement_batch_control.disbursement_envelope_id
                        )
                    )
                )
                .scalars()
                .first()
            )

            _logger.info(f"Disbursement Envelope: {disbursement_envelope}")
            disbursement_batch_control_geo_payloads = [
                DisbursementBatchControlGeoPayload(
                    disbursement_batch_control_geo_id=disbursement_batch_control_geo.id,
                    disbursement_cycle_id=disbursement_batch_control_geo.disbursement_cycle_id,
                    disbursement_envelope_id=disbursement_batch_control_geo.disbursement_envelope_id,
                    disbursement_batch_control_id=disbursement_batch_control_geo.disbursement_batch_control_id,
                    administrative_zone_id_large=disbursement_batch_control_geo.administrative_zone_id_large,
                    administrative_zone_mnemonic_large=disbursement_batch_control_geo.administrative_zone_mnemonic_large,
                    administrative_zone_id_small=disbursement_batch_control_geo.administrative_zone_id_small,
                    administrative_zone_mnemonic_small=disbursement_batch_control_geo.administrative_zone_mnemonic_small,
                    no_of_beneficiaries=disbursement_batch_control_geo.no_of_beneficiaries,
                    total_quantity=disbursement_batch_control_geo.total_quantity,
                    warehouse_id=disbursement_batch_control_geo.warehouse_id,
                    warehouse_mnemonic=disbursement_batch_control_geo.warehouse_mnemonic,
                    warehouse_additional_attributes=disbursement_batch_control_geo.warehouse_additional_attributes,
                    agency_id=disbursement_batch_control_geo.agency_id,
                    agency_mnemonic=disbursement_batch_control_geo.agency_mnemonic,
                    agency_additional_attributes=disbursement_batch_control_geo.agency_additional_attributes,
                    warehouse_notification_status=str(
                        disbursement_batch_control_geo.warehouse_notification_status
                    ),
                    agency_notification_status=str(disbursement_batch_control_geo.agency_notification_status),
                )
                for disbursement_batch_control_geo in disbursement_batch_control_geos
            ]
            disbursement_batch_control_payload = DisbursementBatchControlPayload(
                disbursement_batch_control_id=disbursement_batch_control.id,
                benefit_code_id=disbursement_envelope.benefit_code_id,
                benefit_code_mnemonic=disbursement_envelope.benefit_code_mnemonic,
                benefit_type=disbursement_envelope.benefit_type,
                measurement_unit=disbursement_envelope.measurement_unit,
                disbursement_cycle_id=disbursement_batch_control.disbursement_cycle_id,
                disbursement_cycle_code_mnemonic=disbursement_envelope.cycle_code_mnemonic,
                disbursement_envelope_id=disbursement_batch_control.disbursement_envelope_id,
                fa_resolution_status=str(disbursement_batch_control.fa_resolution_status),
                fa_resolution_timestamp=disbursement_batch_control.fa_resolution_timestamp,
                fa_resolution_latest_error_code=disbursement_batch_control.fa_resolution_latest_error_code,
                fa_resolution_attempts=disbursement_batch_control.fa_resolution_attempts,
                sponsor_bank_dispatch_status=str(disbursement_batch_control.sponsor_bank_dispatch_status),
                sponsor_bank_dispatch_timestamp=disbursement_batch_control.sponsor_bank_dispatch_timestamp,
                sponsor_bank_dispatch_latest_error_code=disbursement_batch_control.sponsor_bank_dispatch_latest_error_code,
                sponsor_bank_dispatch_attempts=disbursement_batch_control.sponsor_bank_dispatch_attempts,
                geo_resolution_status=str(disbursement_batch_control.geo_resolution_status),
                geo_resolution_timestamp=disbursement_batch_control.geo_resolution_timestamp,
                geo_resolution_latest_error_code=disbursement_batch_control.geo_resolution_latest_error_code,
                geo_resolution_attempts=disbursement_batch_control.geo_resolution_attempts,
                warehouse_allocation_status=str(disbursement_batch_control.warehouse_allocation_status),
                warehouse_allocation_timestamp=disbursement_batch_control.warehouse_allocation_timestamp,
                warehouse_allocation_latest_error_code=disbursement_batch_control.warehouse_allocation_latest_error_code,
                warehouse_allocation_attempts=disbursement_batch_control.warehouse_allocation_attempts,
                agency_allocation_status=str(disbursement_batch_control.agency_allocation_status),
                agency_allocation_timestamp=disbursement_batch_control.agency_allocation_timestamp,
                agency_allocation_latest_error_code=disbursement_batch_control.agency_allocation_latest_error_code,
                agency_allocation_attempts=disbursement_batch_control.agency_allocation_attempts,
                disbursement_batch_control_geos=disbursement_batch_control_geo_payloads,
            )

        _logger.info("Disbursement batch control payload retrieved successfully")
        return disbursement_batch_control_payload

    async def construct_disbursement_status_error_response(
        self,
        disbursement_status_request: DisbursementStatusRequest,
        code: str,
    ) -> DisbursementStatusResponse:
        _logger.info("Constructing disbursement status error response")
        response = DisbursementStatusResponse(
            header=SyncResponseHeader(
                message_id=disbursement_status_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_status_request.header.action,
                status=StatusEnum.rjct,
                status_reason_message=code,
            ),
            message={},
        )

        _logger.info("Disbursement status error response constructed")
        return response

    async def construct_disbursement_status_success_response(
        self,
        disbursement_status_request: DisbursementStatusRequest,
        disbursement_status_payloads: List[DisbursementStatusPayload],
    ) -> DisbursementStatusResponse:
        _logger.info("Constructing disbursement status success response")
        response = DisbursementStatusResponse(
            header=SyncResponseHeader(
                message_id=disbursement_status_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_status_request.header.action,
                status=StatusEnum.succ,
            ),
            message=disbursement_status_payloads,
        )
        _logger.info("Disbursement status success response constructed")
        return response

    async def construct_disbursement_batch_control_success_response(
        self,
        disbursement_batch_control_request: DisbursementBatchControlRequest,
        disbursement_batch_control_payload: DisbursementBatchControlPayload,
    ) -> DisbursementBatchControlResponse:
        _logger.info("Constructing disbursement batch control success response")
        response = DisbursementBatchControlResponse(
            header=SyncResponseHeader(
                message_id=disbursement_batch_control_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_batch_control_request.header.action,
                status=StatusEnum.succ,
            ),
            message=disbursement_batch_control_payload,
        )
        _logger.info("Disbursement batch control success response constructed")
        return response

    async def construct_disbursement_batch_control_error_response(
        self,
        disbursement_batch_control_request: DisbursementBatchControlRequest,
        code: str,
    ) -> DisbursementBatchControlResponse:
        _logger.info("Constructing disbursement batch control error response")
        response = DisbursementBatchControlResponse(
            header=SyncResponseHeader(
                message_id=disbursement_batch_control_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_batch_control_request.header.action,
                status=StatusEnum.rjct,
                status_reason_message=code,
            ),
            message={},
        )
        _logger.info("Disbursement batch control error response constructed")
        return response
