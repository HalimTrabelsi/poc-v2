import datetime
import logging
import uuid
from typing import Optional

from openg2p_g2p_bridge_models.models import (
    Disbursement,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementResolutionGeoAddress,
    NotificationLog,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import (
    AgencyNotificationPayload,
    BeneficiaryEntitlement,
)
from openg2p_g2p_bridge_notification_connectors.factory import NotificationFactory
from openg2p_g2p_bridge_notification_connectors.models import (
    NotificationResponse,
    NotificationResponseStatus,
    NotificationType,
    Recipient,
)
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine

_config = Settings.get_config()
_engine = get_engine()

_logger = logging.getLogger("agency_notification_worker")


@celery_app.task(name="agency_notification_worker")
def agency_notification_worker(disbursement_batch_control_geo_id: str) -> None:
    _logger.info(f"Starting agency notification for geo: {disbursement_batch_control_geo_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
    with session_maker() as session:
        disbursement_batch_control_geo: Optional[DisbursementBatchControlGeo] = None
        try:
            # Fetch the batch control geo record
            disbursement_batch_control_geo = (
                (
                    session.execute(
                        select(DisbursementBatchControlGeo).where(
                            DisbursementBatchControlGeo.id == disbursement_batch_control_geo_id
                        )
                    )
                )
                .scalars()
                .first()
            )

            # Fetch the related DisbursementEnvelope
            disbursement_envelope = (
                (
                    session.execute(
                        select(DisbursementEnvelope).where(
                            DisbursementEnvelope.id == disbursement_batch_control_geo.disbursement_envelope_id
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_envelope:
                _logger.error(
                    f"No DisbursementEnvelope found for id {disbursement_batch_control_geo.disbursement_envelope_id}"
                )
                raise ValueError(
                    f"No DisbursementEnvelope found for id {disbursement_batch_control_geo.disbursement_envelope_id}"
                )

            # Fetch all DisbursementResolutionGeoAddress records for this agency/zone
            disbursement_resolution_geo_addresses = (
                session.execute(
                    select(DisbursementResolutionGeoAddress).where(
                        DisbursementResolutionGeoAddress.disbursement_batch_control_geo_id
                        == disbursement_batch_control_geo.id
                    )
                )
                .scalars()
                .all()
            )

            # Fetch Disbursement records for these beneficiaries
            beneficiary_ids = [
                disbursement_resolution_geo_address.beneficiary_id
                for disbursement_resolution_geo_address in disbursement_resolution_geo_addresses
            ]
            disbursements = (
                session.execute(select(Disbursement).where(Disbursement.beneficiary_id.in_(beneficiary_ids)))
                .scalars()
                .all()
            )
            disbursement_map = {(d.beneficiary_id, d.id): d for d in disbursements}

            beneficiary_entitlements = []
            for disbursement_resolution_geo_address in disbursement_resolution_geo_addresses:
                # Try to find the matching Disbursement by beneficiary_id and disbursement_id if available
                disbursement = disbursement_map.get(
                    (
                        disbursement_resolution_geo_address.beneficiary_id,
                        getattr(disbursement_resolution_geo_address, "disbursement_id", None),
                    )
                )
                beneficiary_entitlements.append(
                    BeneficiaryEntitlement(
                        beneficiary_id=disbursement_resolution_geo_address.beneficiary_id,
                        beneficiary_name=(
                            getattr(disbursement, "beneficiary_name", None) if disbursement else None
                        ),
                        total_quantity=(
                            getattr(disbursement, "disbursement_quantity", None) if disbursement else None
                        ),
                    )
                )

            disbursement_batch_control_geo_attributes = (
                session.execute(
                    select(DisbursementBatchControlGeoAttributes).where(
                        DisbursementBatchControlGeoAttributes.id == disbursement_batch_control_geo.id
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_batch_control_geo_attributes:
                _logger.error(
                    f"No DisbursementBatchControlGeoAttributes found for id {disbursement_batch_control_geo.id}"
                )
                raise ValueError(
                    f"No DisbursementBatchControlGeoAttributes found for id {disbursement_batch_control_geo.id}"
                )

            # Build notification payload
            notification_payload = construct_agency_notification_payload(
                disbursement_batch_control_geo,
                disbursement_envelope,
                beneficiary_entitlements,
                disbursement_batch_control_geo_attributes,
            )

            # Generate notification_id
            notification_id = str(uuid.uuid4())

            # Send to notification microservice
            notifier = NotificationFactory.get_component().get_notifier()
            recipient = Recipient(
                recipient_id=disbursement_batch_control_geo.agency_id,
                recipient_name=disbursement_batch_control_geo_attributes.agency_admin_name,
                recipient_email=disbursement_batch_control_geo_attributes.agency_admin_email,
                recipient_phone=disbursement_batch_control_geo_attributes.agency_admin_phone,
            )

            notification_response: NotificationResponse = notifier.send_notification(
                notification_id=notification_id,
                payload=notification_payload.model_dump(),
                notification_type=NotificationType.AGENCY_NOTIFICATION.value,
                recipient=recipient,
            )
            # Create NotificationLog entry
            notification_log = NotificationLog(
                id=str(uuid.uuid4()),
                notification_type=NotificationType.AGENCY_NOTIFICATION.value,
                recipient=disbursement_batch_control_geo.agency_mnemonic,
                payload=str(notification_payload.model_dump()),
                sent_at=datetime.datetime.now(),
            )

            if notification_response.status == NotificationResponseStatus.FAILURE:
                raise Exception(notification_response.error_message or "Notification failed")

            notification_log.response = notification_response.response
            notification_log.processed_at = datetime.datetime.now()
            disbursement_batch_control_geo.agency_notification_status = ProcessStatus.PROCESSED.value
            session.add(notification_log)
            session.commit()
            _logger.info(
                f"Agency notification completed successfully for geo: {disbursement_batch_control_geo_id}"
            )

        except Exception as e:
            session.rollback()
            _logger.error(f"Agency notification failed: {e}")

            disbursement_batch_control_geo.agency_notification_attempts += 1
            disbursement_batch_control_geo.agency_notification_latest_error_code = str(e)
            if (
                disbursement_batch_control_geo.agency_notification_attempts
                > _config.agency_notification_max_attempts
            ):
                disbursement_batch_control_geo.agency_notification_status = ProcessStatus.ERROR.value
            else:
                disbursement_batch_control_geo.agency_notification_status = ProcessStatus.PENDING.value
            session.commit()


def construct_agency_notification_payload(
    disbursement_batch_control_geo,
    disbursement_envelope,
    beneficiary_entitlements,
    disbursement_batch_control_geo_attributes,
):
    _logger.info("Constructing agency notification payload")
    notification_payload = AgencyNotificationPayload(
        program_mnemonic=getattr(disbursement_envelope, "benefit_program_mnemonic", None),
        program_description=getattr(disbursement_envelope, "benefit_program_description", None),
        target_registry=getattr(disbursement_envelope, "target_registry", None),
        disbursement_cycle_mnemonic=getattr(disbursement_batch_control_geo, "disbursement_cycle_id", None),
        disbursement_date=str(getattr(disbursement_envelope, "disbursement_schedule_date", None)),
        benefit_code_id=getattr(disbursement_envelope, "benefit_code_id", None),
        benefit_code_mnemonic=getattr(disbursement_envelope, "benefit_code_mnemonic", None),
        benefit_code_description=getattr(disbursement_envelope, "benefit_code_description", None),
        benefit_type=getattr(disbursement_envelope, "benefit_type", None),
        measurement_unit=getattr(disbursement_envelope, "measurement_unit", None),
        benefit_description=getattr(disbursement_envelope, "benefit_code_description", None),
        warehouse_id=getattr(disbursement_batch_control_geo, "warehouse_id", None),
        warehouse_mnemonic=getattr(disbursement_batch_control_geo, "warehouse_mnemonic", None),
        warehouse_name=getattr(disbursement_batch_control_geo_attributes, "warehouse_name", None),
        agency_id=getattr(disbursement_batch_control_geo, "agency_id", None),
        agency_mnemonic=getattr(disbursement_batch_control_geo, "agency_mnemonic", None),
        agency_name=getattr(disbursement_batch_control_geo_attributes, "agency_name", None),
        total_quantity=getattr(disbursement_batch_control_geo, "total_quantity", None),
        no_of_bebeficiaries=getattr(disbursement_batch_control_geo, "no_of_beneficiaries", None),
        administrative_zone_id_large=getattr(
            disbursement_batch_control_geo, "administrative_zone_id_large", None
        ),
        administrative_zone_mnemonic_large=getattr(
            disbursement_batch_control_geo,
            "administrative_zone_mnemonic_large",
            None,
        ),
        administrative_zone_id_small=getattr(
            disbursement_batch_control_geo, "administrative_zone_id_small", None
        ),
        administrative_zone_mnemonic_small=getattr(
            disbursement_batch_control_geo,
            "administrative_zone_mnemonic_small",
            None,
        ),
        beneficiary_entitlements=beneficiary_entitlements,
    )

    _logger.info("Agency notification payload constructed successfully")
    return notification_payload
