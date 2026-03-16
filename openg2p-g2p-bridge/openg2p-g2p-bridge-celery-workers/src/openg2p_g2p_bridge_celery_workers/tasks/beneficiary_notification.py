import datetime
import logging
import uuid
from typing import Optional

from openg2p_g2p_bridge_models.models import (
    Disbursement,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementResolutionGeoAddress,
    NotificationLog,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import (
    BeneficiaryNotificationPayload,
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

_logger = logging.getLogger("beneficiary_notification_worker")
_config = Settings.get_config()
_engine = get_engine()


@celery_app.task(name="beneficiary_notification_worker")
def beneficiary_notification_worker(disbursement_id: str) -> None:
    _logger.info(f"Starting beneficiary notification for disbursement: {disbursement_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
    with session_maker() as session:
        try:
            # Fetch the geo address record
            disbursement_resolution_geo_address: Optional[DisbursementResolutionGeoAddress] = (
                (
                    session.execute(
                        select(DisbursementResolutionGeoAddress).where(
                            DisbursementResolutionGeoAddress.disbursement_id == disbursement_id
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_resolution_geo_address:
                _logger.error(f"No geo address found for disbursement_id {disbursement_id}")
                raise Exception(f"No geo address found for disbursement_id {disbursement_id}")
            # Fetch the envelope for payload details
            disbursement_envelope: Optional[DisbursementEnvelope] = (
                (
                    session.execute(
                        select(DisbursementEnvelope).where(
                            DisbursementEnvelope.id
                            == disbursement_resolution_geo_address.disbursement_envelope_id
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_envelope:
                _logger.error(
                    f"No envelope found for id {disbursement_resolution_geo_address.disbursement_envelope_id}"
                )
                raise Exception(
                    f"No envelope found for id {disbursement_resolution_geo_address.disbursement_envelope_id}"
                )
            # Fetch the Disbursement for beneficiary_name and disbursement_quantity
            disbursement: Optional[Disbursement] = (
                (session.execute(select(Disbursement).where(Disbursement.id == disbursement_id)))
                .scalars()
                .first()
            )

            disbursement_batch_control_geo_attributes = (
                session.execute(
                    select(DisbursementBatchControlGeoAttributes).where(
                        DisbursementBatchControlGeoAttributes.id
                        == disbursement_resolution_geo_address.disbursement_batch_control_geo_id
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_batch_control_geo_attributes:
                _logger.error(
                    f"No DisbursementBatchControlGeoAttributes found for id {disbursement_resolution_geo_address.disbursement_batch_control_geo_id}"
                )
                raise ValueError(
                    f"No DisbursementBatchControlGeoAttributes found for id {disbursement_resolution_geo_address.disbursement_batch_control_geo_id}"
                )

            # Build notification payload
            notification_payload = construct_beneficiary_notification_payload(
                disbursement_resolution_geo_address,
                disbursement_envelope,
                disbursement,
                disbursement_batch_control_geo_attributes,
            )
            # Generate notification_id
            notification_id = str(uuid.uuid4())

            # Send to notification microservice
            notifier = NotificationFactory.get_component().get_notifier()
            recipient = Recipient(
                recipient_id=disbursement_resolution_geo_address.beneficiary_id,
                recipient_name=disbursement_resolution_geo_address.beneficiary_name,
                recipient_email=disbursement_resolution_geo_address.beneficiary_email,
                recipient_phone=disbursement_resolution_geo_address.beneficiary_phone,
            )
            notification_response: NotificationResponse = notifier.send_notification(
                notification_id=notification_id,
                payload=notification_payload.model_dump(),
                notification_type=NotificationType.BENEFICIARY_NOTIFICATION.value,
                recipient=recipient,
            )

            # Create NotificationLog entry (PENDING)
            notification_log = NotificationLog(
                id=notification_id,
                notification_type=NotificationType.BENEFICIARY_NOTIFICATION.value,
                recipient=disbursement_resolution_geo_address.beneficiary_id,
                payload=str(notification_payload.model_dump()),
                sent_at=datetime.datetime.now(),
            )
            if notification_response.status == NotificationResponseStatus.FAILURE:
                raise Exception(notification_response.error_message or "Notification failed")

            notification_log.response = notification_response.response
            notification_log.processed_at = datetime.datetime.now()
            disbursement_resolution_geo_address.beneficiary_notification_status = (
                ProcessStatus.PROCESSED.value
            )

            session.add(notification_log)
            session.commit()
            _logger.info(
                f"Beneficiary notification completed successfully for disbursement: {disbursement_id}"
            )

        except Exception as e:
            session.rollback()
            _logger.error(f"Beneficiary notification failed: {e}")

            disbursement_resolution_geo_address.beneficiary_notification_attempts += 1
            disbursement_resolution_geo_address.beneficiary_notification_latest_error_code = str(e)

            if (
                disbursement_resolution_geo_address.beneficiary_notification_attempts
                >= _config.beneficiary_notification_max_attempts
            ):
                disbursement_resolution_geo_address.beneficiary_notification_status = (
                    ProcessStatus.ERROR.value
                )
            else:
                disbursement_resolution_geo_address.beneficiary_notification_status = (
                    ProcessStatus.PENDING.value
                )
            session.commit()


def construct_beneficiary_notification_payload(
    disbursement_resolution_geo_address,
    disbursement_envelope,
    disbursement,
    disbursement_batch_control_geo_attributes,
):
    _logger.info("Constructing beneficiary notification payload")
    notification_payload = BeneficiaryNotificationPayload(
        beneficiary_id=disbursement_resolution_geo_address.beneficiary_id,
        beneficiary_name=getattr(disbursement, "beneficiary_name", None),
        program_mnemonic=getattr(disbursement_envelope, "benefit_program_mnemonic", None),
        program_description=getattr(disbursement_envelope, "benefit_program_description", None),
        target_registry=getattr(disbursement_envelope, "target_registry", None),
        disbursement_cycle_mnemonic=getattr(disbursement_envelope, "cycle_code_mnemonic", None),
        disbursement_date=str(getattr(disbursement_envelope, "disbursement_schedule_date", None)),
        benefit_code_id=getattr(disbursement_envelope, "benefit_code_id", None),
        benefit_code_mnemonic=getattr(disbursement_envelope, "benefit_code_mnemonic", None),
        benefit_type=getattr(disbursement_envelope, "benefit_type", None),
        measurement_unit=getattr(disbursement_envelope, "measurement_unit", None),
        benefit_description=getattr(disbursement_envelope, "benefit_code_description", None),
        warehouse_id=getattr(disbursement_resolution_geo_address, "warehouse_id", None),
        warehouse_mnemonic=getattr(disbursement_resolution_geo_address, "warehouse_mnemonic", None),
        warehouse_name=getattr(disbursement_batch_control_geo_attributes, "warehouse_name", None),
        agency_id=getattr(disbursement_resolution_geo_address, "agency_id", None),
        agency_mnemonic=getattr(disbursement_resolution_geo_address, "agency_mnemonic", None),
        agency_name=getattr(disbursement_batch_control_geo_attributes, "agency_name", None),
        total_quantity=getattr(disbursement, "disbursement_quantity", None),
        administrative_zone_id_large=getattr(
            disbursement_resolution_geo_address, "administrative_zone_id_large", None
        ),
        administrative_zone_mnemonic_large=getattr(
            disbursement_resolution_geo_address,
            "administrative_zone_mnemonic_large",
            None,
        ),
        administrative_zone_id_small=getattr(
            disbursement_resolution_geo_address, "administrative_zone_id_small", None
        ),
        administrative_zone_mnemonic_small=getattr(
            disbursement_resolution_geo_address,
            "administrative_zone_mnemonic_small",
            None,
        ),
    )

    _logger.info("Beneficiary notification payload constructed successfully")
    return notification_payload
