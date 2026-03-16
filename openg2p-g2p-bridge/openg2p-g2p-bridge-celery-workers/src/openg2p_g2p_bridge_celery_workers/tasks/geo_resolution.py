import logging
from datetime import datetime
from typing import Dict, List, Optional

from fastnanoid import generate
from openg2p_g2p_bridge_geo_resolver.factory import GeoResolutionFactory
from openg2p_g2p_bridge_geo_resolver.interface import GeoResolver
from openg2p_g2p_bridge_models.models import (
    BenefitType,
    Disbursement,
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementResolutionGeoAddress,
    ProcessStatus,
)
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="geo_resolution_worker")
def geo_resolution_worker(disbursement_batch_control_id: str):
    _logger.info(f"Starting geo resolution for batch: {disbursement_batch_control_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
    with session_maker() as session:
        try:
            disbursement_batch_control: Optional[DisbursementBatchControl] = session.execute(
                select(DisbursementBatchControl).where(
                    DisbursementBatchControl.id == disbursement_batch_control_id
                )
            ).scalar_one_or_none()

            if not disbursement_batch_control:
                _logger.error(f"No DisbursementBatchControl found for id {disbursement_batch_control_id}")
                raise ValueError(f"No DisbursementBatchControl found for id {disbursement_batch_control_id}")

            disbursement_envelope = session.execute(
                select(DisbursementEnvelope).where(
                    DisbursementEnvelope.id == disbursement_batch_control.disbursement_envelope_id
                )
            ).scalar_one_or_none()

            if not disbursement_envelope:
                _logger.error(
                    f"Disbursement envelope ID is missing for batch {disbursement_batch_control_id}"
                )
                raise ValueError(
                    f"Disbursement envelope ID is missing for batch {disbursement_batch_control_id}"
                )

            disbursements: List[Disbursement] = (
                session.execute(
                    select(Disbursement).where(
                        Disbursement.disbursement_batch_control_id == disbursement_batch_control_id
                    )
                )
                .scalars()
                .all()
            )

            if not disbursements:
                _logger.warning(f"No disbursements found for batch {disbursement_batch_control_id}")
                raise ValueError(f"No disbursements found for batch {disbursement_batch_control_id}")

            batch_beneficiary_list = [
                {
                    "disbursement_id": d.id,
                    "beneficiary_id": d.beneficiary_id,
                }
                for d in disbursements
            ]

            geo_resolver: GeoResolver = GeoResolutionFactory.get_component().get_geo_resolver(
                disbursement_envelope.target_registry
            )

            resolved_data: List[Dict[str, str]]
            resolved_data = geo_resolver.resolve_geo(batch_beneficiary_list)

            if not resolved_data:
                _logger.error(f"Geo resolution failed for batch {disbursement_batch_control_id}")
                raise ValueError("Geo resolution returned no data")

            # Create a map of disbursement_id to disbursement_quantity for quick lookup
            disbursement_quantities = {d.id: d.disbursement_quantity for d in disbursements}

            # Optimized: Aggregate both total_quantity and no_of_beneficiaries in one pass
            batch_control_geo_map = {}
            for geo_resolution_item in resolved_data:
                key = (
                    geo_resolution_item["administrative_zone_id_large"],
                    geo_resolution_item["administrative_zone_id_small"],
                )
                if key not in batch_control_geo_map:
                    batch_control_geo_map[key] = {
                        "total_quantity": 0,
                        "no_of_beneficiaries": 0,
                        "administrative_zone_mnemonic_large": geo_resolution_item[
                            "administrative_zone_mnemonic_large"
                        ],
                        "administrative_zone_mnemonic_small": geo_resolution_item[
                            "administrative_zone_mnemonic_small"
                        ],
                    }
                quantity = disbursement_quantities.get(geo_resolution_item["disbursement_id"], 0)
                batch_control_geo_map[key]["total_quantity"] += quantity
                batch_control_geo_map[key]["no_of_beneficiaries"] += 1

            disbursement_batch_control_geos = []
            disbursement_batch_control_geo_attributes_list = []
            batch_control_geo_id_map = {}
            for (admin_large_id, admin_small_id), data in batch_control_geo_map.items():
                disbursement_batch_control_geo_id = str(generate(size=16))
                disbursement_batch_control_geo = DisbursementBatchControlGeo(
                    id=disbursement_batch_control_geo_id,
                    disbursement_cycle_id=disbursement_batch_control.disbursement_cycle_id,
                    disbursement_envelope_id=disbursement_batch_control.disbursement_envelope_id,
                    disbursement_batch_control_id=disbursement_batch_control.id,
                    administrative_zone_id_large=admin_large_id,
                    administrative_zone_mnemonic_large=data["administrative_zone_mnemonic_large"],
                    administrative_zone_id_small=admin_small_id,
                    administrative_zone_mnemonic_small=data["administrative_zone_mnemonic_small"],
                    no_of_beneficiaries=data["no_of_beneficiaries"],
                    total_quantity=data["total_quantity"],
                    warehouse_notification_status=ProcessStatus.NOT_APPLICABLE.value,
                    agency_notification_status=ProcessStatus.NOT_APPLICABLE.value,
                )
                disbursement_batch_control_geos.append(disbursement_batch_control_geo)
                batch_control_geo_id_map[(admin_large_id, admin_small_id)] = disbursement_batch_control_geo_id

                disbursement_batch_control_geo_attributes: DisbursementBatchControlGeoAttributes = (
                    DisbursementBatchControlGeoAttributes(
                        id=disbursement_batch_control_geo_id,
                        disbursement_batch_control_id=disbursement_batch_control.id,
                    )
                )
                disbursement_batch_control_geo_attributes_list.append(
                    disbursement_batch_control_geo_attributes
                )

            session.add_all(disbursement_batch_control_geos)
            session.add_all(disbursement_batch_control_geo_attributes_list)
            session.flush()  # Ensure IDs are available

            # Now create DisbursementResolutionGeoAddress with the correct disbursement_batch_control_geo_id
            disbursement_resolution_geo_addresses = []
            for geo_resolution_item in resolved_data:
                key = (
                    geo_resolution_item["administrative_zone_id_large"],
                    geo_resolution_item["administrative_zone_id_small"],
                )
                disbursement_batch_control_geo_id = batch_control_geo_id_map.get(key)
                disbursement_resolution_geo_address = DisbursementResolutionGeoAddress(
                    id=geo_resolution_item.get("disbursement_id", None),
                    disbursement_id=geo_resolution_item.get("disbursement_id", None),
                    disbursement_cycle_id=disbursement_batch_control.disbursement_cycle_id,
                    disbursement_envelope_id=disbursement_batch_control.disbursement_envelope_id,
                    disbursement_batch_control_id=disbursement_batch_control.id,
                    disbursement_batch_control_geo_id=disbursement_batch_control_geo_id,
                    beneficiary_id=geo_resolution_item.get("beneficiary_id", None),
                    administrative_zone_id_large=geo_resolution_item.get(
                        "administrative_zone_id_large", None
                    ),
                    administrative_zone_mnemonic_large=geo_resolution_item.get(
                        "administrative_zone_mnemonic_large", None
                    ),
                    administrative_zone_id_small=geo_resolution_item.get(
                        "administrative_zone_id_small", None
                    ),
                    administrative_zone_mnemonic_small=geo_resolution_item.get(
                        "administrative_zone_mnemonic_small", None
                    ),
                    beneficiary_name=geo_resolution_item.get("beneficiary_name", None),
                    beneficiary_email=geo_resolution_item.get("beneficiary_email", None),
                    beneficiary_phone=geo_resolution_item.get("beneficiary_phone", None),
                )
                disbursement_resolution_geo_addresses.append(disbursement_resolution_geo_address)

            session.add_all(disbursement_resolution_geo_addresses)

            # Update the DisbursementBatchControl status

            disbursement_batch_control.geo_resolution_status = ProcessStatus.PROCESSED.value

            if disbursement_envelope.benefit_type == BenefitType.CASH_PHYSICAL.value:
                disbursement_batch_control.agency_allocation_status = ProcessStatus.PENDING.value
            else:
                # For non-cash physical benefits, set warehouse allocation status to PENDING and
                # Warehouse allocation will make Agency allocation as PENDING
                # This worker is not applicable to DIGITAL CASH
                disbursement_batch_control.warehouse_allocation_status = ProcessStatus.PENDING.value
            disbursement_batch_control.geo_resolution_timestamp = datetime.now()
            disbursement_batch_control.geo_resolution_latest_error_code = None
            disbursement_batch_control.geo_resolution_attempts = (
                disbursement_batch_control.geo_resolution_attempts or 0
            ) + 1

            session.commit()
            _logger.info(f"Successfully completed geo resolution for batch: {disbursement_batch_control_id}")

        except Exception as e:
            session.rollback()
            _logger.error(
                f"Error in geo resolution for batch {disbursement_batch_control_id}: {e}",
                exc_info=True,
            )
            disbursement_batch_control = (
                session.query(DisbursementBatchControl).filter_by(id=disbursement_batch_control_id).first()
            )
            if disbursement_batch_control:
                disbursement_batch_control.geo_resolution_latest_error_code = str(e)
                disbursement_batch_control.geo_resolution_attempts = (
                    disbursement_batch_control.geo_resolution_attempts or 0
                ) + 1
                if disbursement_batch_control.geo_resolution_attempts >= _config.geo_resolution_max_attempts:
                    disbursement_batch_control.geo_resolution_status = ProcessStatus.ERROR.value
                else:
                    disbursement_batch_control.geo_resolution_status = ProcessStatus.PENDING.value
                session.commit()
