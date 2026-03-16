import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from openg2p_g2p_bridge_agency_allocator.factory.agency_allocator_factory import (
    AgencyAllocatorFactory,
)
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementResolutionGeoAddress,
    ProcessStatus,
)
from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_logger = logging.getLogger("agency_allocation_worker")
_engine = get_engine()
_config = Settings.get_config()
session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
# Remove session_maker_pbms and pbms_session


@celery_app.task(name="agency_allocation_worker")
def agency_allocation_worker(disbursement_batch_control_id: str) -> None:
    _logger.info(f"Starting agency allocation for batch: {disbursement_batch_control_id}")
    with session_maker() as session:
        try:
            # Fetch the batch control record
            disbursement_batch_control: Optional[DisbursementBatchControl] = (
                (
                    session.execute(
                        select(DisbursementBatchControl).where(
                            DisbursementBatchControl.id == disbursement_batch_control_id
                        )
                    )
                )
                .scalars()
                .first()
            )

            # Fetch all related geo records
            disbursement_batch_control_geos: List[DisbursementBatchControlGeo] = (
                (
                    session.execute(
                        select(DisbursementBatchControlGeo).where(
                            DisbursementBatchControlGeo.disbursement_batch_control_id
                            == disbursement_batch_control_id
                        )
                    )
                )
                .scalars()
                .all()
            )

            # Fetch the related envelope for benefit_code and program
            disbursement_envelope = (
                session.execute(
                    select(DisbursementEnvelope).where(
                        DisbursementEnvelope.id == disbursement_batch_control.disbursement_envelope_id
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_envelope:
                _logger.error(
                    f"No envelope found for id {disbursement_batch_control.disbursement_envelope_id}"
                )
                raise Exception(
                    f"No envelope found for id {disbursement_batch_control.disbursement_envelope_id}"
                )

            # Prepare small_geo_list
            small_geo_list = [
                {
                    "batch_control_geo_id": disbursement_batch_control_geo.id,
                    "administrative_zone_id_small": disbursement_batch_control_geo.administrative_zone_id_small,
                    "administrative_zone_mnemonic_small": disbursement_batch_control_geo.administrative_zone_mnemonic_small,
                }
                for disbursement_batch_control_geo in disbursement_batch_control_geos
            ]
            benefit_code_id = disbursement_envelope.benefit_code_id
            program_id = disbursement_envelope.benefit_program_id

            agency_allocator = AgencyAllocatorFactory.get_component().get_agency_allocator()
            allocation_results: List[Dict[str, Any]] = agency_allocator.allocate_agency(
                small_geo_list, benefit_code_id, program_id
            )

            warehouse_notification_status = ProcessStatus.NOT_APPLICABLE.value
            agency_notification_status = ProcessStatus.PENDING.value
            if (
                disbursement_envelope.benefit_type == BenefitType.SERVICE.value
                or disbursement_envelope.benefit_type == BenefitType.COMMODITY.value
                or disbursement_envelope.benefit_type == BenefitType.COMBINATION.value
            ):
                # For services or commodities, we do not need to update warehouse_notification_status
                warehouse_notification_status = ProcessStatus.PENDING.value

            for disbursement_batch_control_geo, allocation in zip(
                disbursement_batch_control_geos, allocation_results
            ):
                # Bulk update DisbursementBatchControlGeo
                session.execute(
                    update(DisbursementBatchControlGeo)
                    .where(DisbursementBatchControlGeo.id == disbursement_batch_control_geo.id)
                    .values(
                        agency_id=allocation["agency_id"],
                        agency_mnemonic=allocation["agency_mnemonic"],
                        agency_additional_attributes=allocation.get("agency_additional_attributes", {}),
                        warehouse_notification_status=(
                            warehouse_notification_status
                            if not _config.suppress_notifications
                            else ProcessStatus.PROCESSED.value
                        ),
                        agency_notification_status=(
                            agency_notification_status
                            if not _config.suppress_notifications
                            else ProcessStatus.PROCESSED.value
                        ),
                    )
                )

                # Bulk update DisbursementResolutionGeoAddress
                session.execute(
                    update(DisbursementResolutionGeoAddress)
                    .where(
                        DisbursementResolutionGeoAddress.disbursement_batch_control_geo_id
                        == disbursement_batch_control_geo.id,
                    )
                    .values(
                        agency_id=allocation["agency_id"],
                        agency_mnemonic=allocation["agency_mnemonic"],
                        beneficiary_notification_status=(
                            ProcessStatus.PENDING.value
                            if not _config.suppress_notifications
                            else ProcessStatus.PROCESSED.value
                        ),
                    )
                )

                # Update DisbursementBatchControlGeoAttributes
                session.execute(
                    update(DisbursementBatchControlGeoAttributes)
                    .where(DisbursementBatchControlGeoAttributes.id == disbursement_batch_control_geo.id)
                    .values(
                        agency_name=allocation.get("agency_name", None),
                        agency_admin_name=allocation.get("agency_admin_name", None),
                        agency_admin_email=allocation.get("agency_admin_email", None),
                        agency_admin_phone=allocation.get("agency_admin_phone", None),
                    )
                )

            # Update batch control status
            disbursement_batch_control.agency_allocation_status = ProcessStatus.PROCESSED.value

            if disbursement_envelope.benefit_type == BenefitType.CASH_PHYSICAL.value:
                disbursement_batch_control.sponsor_bank_dispatch_status = ProcessStatus.PENDING.value

            disbursement_batch_control.agency_allocation_attempts += 1
            disbursement_batch_control.agency_allocation_latest_error_code = None
            disbursement_batch_control.agency_allocation_timestamp = datetime.now()

            session.commit()
            _logger.info(
                f"Agency allocation completed successfully for batch: {disbursement_batch_control_id}"
            )
        except Exception as e:
            session.rollback()
            _logger.error(f"Agency allocation failed: {e}")
            disbursement_batch_control.agency_allocation_latest_error_code = str(e)
            disbursement_batch_control.agency_allocation_attempts += 1
            if (
                disbursement_batch_control.agency_allocation_attempts
                >= _config.agency_allocation_max_attempts
            ):
                disbursement_batch_control.agency_allocation_status = ProcessStatus.ERROR.value
            else:
                disbursement_batch_control.agency_allocation_status = ProcessStatus.PENDING.value
            session.commit()
