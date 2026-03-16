import datetime
import logging
import uuid

from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    NotificationLog,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import (
    WarehouseNotificationPayload,
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
_logger = logging.getLogger("warehouse_notification_worker")


@celery_app.task(name="warehouse_notification_worker")
def warehouse_notification_worker(disbursement_batch_control_geo_id: str) -> None:
    _logger.info(f"Starting warehouse notification for geo: {disbursement_batch_control_geo_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)
    with session_maker() as session:
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
            if not disbursement_batch_control_geo:
                _logger.error(f"No batch control geo found for id {disbursement_batch_control_geo_id}")
                return

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
            notification_payload = construct_warehouse_notification_payload(
                disbursement_batch_control_geo,
                disbursement_envelope,
                disbursement_batch_control_geo_attributes,
            )
            # Generate notification_id
            notification_id = str(uuid.uuid4())

            # Send notification
            notifier = NotificationFactory.get_component().get_notifier()
            recipient = Recipient(
                recipient_id=disbursement_batch_control_geo.warehouse_id,
                recipient_name=disbursement_batch_control_geo_attributes.warehouse_admin_name,
                recipient_email=disbursement_batch_control_geo_attributes.warehouse_admin_email,
                recipient_phone=disbursement_batch_control_geo_attributes.warehouse_admin_phone,
            )

            notification_response: NotificationResponse = notifier.send_notification(
                notification_id=notification_id,
                payload=notification_payload.model_dump(),
                notification_type=NotificationType.WAREHOUSE_NOTIFICATION.value,
                recipient=recipient,
            )

            # Create NotificationLog entry (PENDING)
            notification_log = NotificationLog(
                id=notification_id,
                notification_type=NotificationType.WAREHOUSE_NOTIFICATION.value,
                recipient=disbursement_batch_control_geo.warehouse_mnemonic,
                payload=str(notification_payload.model_dump()),
                sent_at=datetime.datetime.now(),
            )

            if notification_response.status == NotificationResponseStatus.FAILURE:
                raise Exception(notification_response.error_message or "Notification failed")

            notification_log.response = notification_response.response
            notification_log.processed_at = datetime.datetime.now()
            disbursement_batch_control_geo.warehouse_notification_status = ProcessStatus.PROCESSED.value
            session.add(notification_log)

            session.commit()
            _logger.info(
                f"Warehouse notification completed successfully for geo: {disbursement_batch_control_geo_id}"
            )

        except Exception as e:
            session.rollback()
            _logger.error(f"Warehouse notification failed: {e}")

            if disbursement_batch_control_geo:
                disbursement_batch_control_geo.warehouse_notification_attempts += 1
                disbursement_batch_control_geo.warehouse_notification_latest_error_code = str(e)
            if (
                disbursement_batch_control_geo.warehouse_notification_attempts
                > _config.warehouse_notification_max_attempts
            ):
                disbursement_batch_control_geo.warehouse_notification_status = ProcessStatus.ERROR.value
            else:
                disbursement_batch_control_geo.warehouse_notification_status = ProcessStatus.PENDING.value
            session.commit()


def construct_warehouse_notification_payload(
    disbursement_batch_control_geo,
    disbursement_envelope,
    disbursement_batch_control_geo_attributes,
):
    _logger.info("Constructing warehouse notification payload")
    notification_payload = WarehouseNotificationPayload(
        program_mnemonic=getattr(disbursement_envelope, "benefit_program_mnemonic", None),
        program_description=getattr(disbursement_envelope, "benefit_program_description", None),
        target_registry=getattr(disbursement_envelope, "target_registry", None),
        disbursement_cycle_mnemonic=getattr(disbursement_batch_control_geo, "disbursement_cycle_id", None),
        disbursement_date=str(getattr(disbursement_envelope, "disbursement_schedule_date", None)),
        benefit_code_id=getattr(disbursement_envelope, "benefit_code_id", None),
        benefit_code_mnemonic=getattr(disbursement_envelope, "benefit_code_mnemonic", None),
        benefit_type=getattr(disbursement_envelope, "benefit_type", None),
        measurement_unit=getattr(disbursement_envelope, "measurement_unit", None),
        benefit_code_description=getattr(disbursement_envelope, "benefit_code_description", None),
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
    )

    _logger.info("Warehouse notification payload constructed successfully")
    return notification_payload
