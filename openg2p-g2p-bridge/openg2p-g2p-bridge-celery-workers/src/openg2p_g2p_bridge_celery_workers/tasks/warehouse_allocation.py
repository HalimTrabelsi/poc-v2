import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementResolutionGeoAddress,
    ProcessStatus,
)
from openg2p_g2p_bridge_warehouse_allocator.factory import WarehouseAllocatorFactory
from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_logger = logging.getLogger("warehouse_allocation_worker")
_engine = get_engine()
_config = Settings.get_config()


@celery_app.task(name="warehouse_allocation_worker")
def warehouse_allocation_worker(disbursement_batch_control_id: str) -> None:
    _logger.info(f"Starting warehouse allocation for batch: {disbursement_batch_control_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
    # Remove session_maker_pbms and pbms_session
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
            if not disbursement_batch_control:
                _logger.error(f"No batch control found for id {disbursement_batch_control_id}")
                raise Exception(f"No batch control found for id {disbursement_batch_control_id}")

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

            # Prepare large_geo_list
            large_geo_list = [
                {
                    "batch_control_geo_id": disbursement_batch_control_geo.id,
                    "administrative_zone_id_large": disbursement_batch_control_geo.administrative_zone_id_large,
                    "administrative_zone_mnemonic_large": disbursement_batch_control_geo.administrative_zone_mnemonic_large,
                }
                for disbursement_batch_control_geo in disbursement_batch_control_geos
            ]
            benefit_code_id = disbursement_envelope.benefit_code_id
            program_id = disbursement_envelope.benefit_program_id

            warehouse_allocator = WarehouseAllocatorFactory.get_component().get_warehouse_allocator()
            allocation_results: List[Dict[str, Any]] = warehouse_allocator.allocate_warehouse(
                large_geo_list, benefit_code_id, program_id
            )

            for disbursement_batch_control_geo, allocation in zip(
                disbursement_batch_control_geos, allocation_results
            ):
                # Bulk update DisbursementBatchControlGeo
                session.execute(
                    update(DisbursementBatchControlGeo)
                    .where(DisbursementBatchControlGeo.id == disbursement_batch_control_geo.id)
                    .values(
                        warehouse_id=allocation["warehouse_id"],
                        warehouse_mnemonic=allocation["warehouse_mnemonic"],
                        warehouse_additional_attributes=allocation.get("warehouse_additional_attributes", {}),
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
                        warehouse_id=allocation["warehouse_id"],
                        warehouse_mnemonic=allocation["warehouse_mnemonic"],
                    )
                )

                # Update DisbursementBatchControlGeoAttributes
                session.execute(
                    update(DisbursementBatchControlGeoAttributes)
                    .where(DisbursementBatchControlGeoAttributes.id == disbursement_batch_control_geo.id)
                    .values(
                        warehouse_name=allocation.get("warehouse_name", None),
                        warehouse_admin_name=allocation.get("warehouse_admin_name", None),
                        warehouse_admin_email=allocation.get("warehouse_admin_email", None),
                        warehouse_admin_phone=allocation.get("warehouse_admin_phone", None),
                    )
                )

            # Update batch control status
            disbursement_batch_control.warehouse_allocation_status = ProcessStatus.PROCESSED.value
            disbursement_batch_control.warehouse_allocation_latest_error_code = None
            disbursement_batch_control.warehouse_allocation_attempts += 1
            disbursement_batch_control.warehouse_allocation_timestamp = datetime.now()
            disbursement_batch_control.agency_allocation_status = ProcessStatus.PENDING.value
            session.commit()
            _logger.info(
                f"Warehouse allocation completed successfully for batch: {disbursement_batch_control_id}"
            )
        except Exception as e:
            session.rollback()
            _logger.error(f"Warehouse allocation failed: {e}")
            # Update error code and attempts
            if disbursement_batch_control:
                disbursement_batch_control.warehouse_allocation_latest_error_code = str(e)
                disbursement_batch_control.warehouse_allocation_attempts += 1
                if (
                    disbursement_batch_control.warehouse_allocation_attempts
                    >= _config.warehouse_allocation_max_attempts
                ):
                    disbursement_batch_control.warehouse_allocation_status = ProcessStatus.ERROR.value
                else:
                    disbursement_batch_control.warehouse_allocation_status = ProcessStatus.PENDING.value
                session.commit()
