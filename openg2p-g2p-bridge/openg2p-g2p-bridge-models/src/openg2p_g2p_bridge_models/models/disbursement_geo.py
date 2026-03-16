from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import BaseORMModelWithId
from .common_enums import ProcessStatus


class DisbursementBatchControlGeo(BaseORMModelWithId):
    __tablename__ = "disbursement_batch_control_geo"

    disbursement_cycle_id: Mapped[str] = mapped_column(String, index=True)
    disbursement_envelope_id: Mapped[str] = mapped_column(String, index=True)
    disbursement_batch_control_id: Mapped[str] = mapped_column(String, index=True)
    administrative_zone_id_large: Mapped[str] = mapped_column(String)
    administrative_zone_mnemonic_large: Mapped[str] = mapped_column(String)
    administrative_zone_id_small: Mapped[str] = mapped_column(String)
    administrative_zone_mnemonic_small: Mapped[str] = mapped_column(String)
    no_of_beneficiaries: Mapped[int] = mapped_column(Integer, default=0)
    total_quantity: Mapped[float] = mapped_column(Float)
    warehouse_id: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_mnemonic: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_additional_attributes: Mapped[str] = mapped_column(String, nullable=True)
    agency_id: Mapped[str] = mapped_column(String, nullable=True)
    agency_mnemonic: Mapped[str] = mapped_column(String, nullable=True)
    agency_additional_attributes: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_notification_status: Mapped[str] = mapped_column(String)
    warehouse_notification_attempts: Mapped[int] = mapped_column(Integer, default=0)
    warehouse_notification_latest_error_code: Mapped[str] = mapped_column(String, nullable=True, default=None)
    agency_notification_status: Mapped[ProcessStatus] = mapped_column(String)
    agency_notification_attempts: Mapped[int] = mapped_column(Integer, default=0)
    agency_notification_latest_error_code: Mapped[str] = mapped_column(String, nullable=True, default=None)
    __table_args__ = (
        # Unique index on (disbursement_batch_control_id, administrative_zone_id_large, administrative_zone_small)
        {"sqlite_autoincrement": True},
    )


class DisbursementBatchControlGeoAttributes(BaseORMModelWithId):
    __tablename__ = "disbursement_batch_control_geo_attributes"

    disbursement_batch_control_id: Mapped[str] = mapped_column(String, index=True)
    warehouse_name: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_admin_name: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_admin_email: Mapped[str] = mapped_column(String, nullable=True)
    warehouse_admin_phone: Mapped[str] = mapped_column(String, nullable=True)
    agency_name: Mapped[str] = mapped_column(String, nullable=True)
    agency_admin_name: Mapped[str] = mapped_column(String, nullable=True)
    agency_admin_email: Mapped[str] = mapped_column(String, nullable=True)
    agency_admin_phone: Mapped[str] = mapped_column(String, nullable=True)


class DisbursementResolutionGeoAddress(BaseORMModelWithId):
    __tablename__ = "disbursement_resolution_geo_address"
    disbursement_id: Mapped[str] = mapped_column(String, unique=True)
    disbursement_cycle_id: Mapped[str] = mapped_column(String, index=True)
    disbursement_envelope_id: Mapped[str] = mapped_column(String, index=True)
    disbursement_batch_control_id: Mapped[str] = mapped_column(String, index=True)
    disbursement_batch_control_geo_id: Mapped[str] = mapped_column(String, index=True)
    beneficiary_id: Mapped[str] = mapped_column(String, index=True)
    administrative_zone_id_large: Mapped[str] = mapped_column(String)
    administrative_zone_mnemonic_large: Mapped[str] = mapped_column(String)
    administrative_zone_id_small: Mapped[str] = mapped_column(String)
    administrative_zone_mnemonic_small: Mapped[str] = mapped_column(String)
    warehouse_id: Mapped[str] = mapped_column(String, index=True, nullable=True)
    warehouse_mnemonic: Mapped[str] = mapped_column(String, nullable=True)
    agency_id: Mapped[str] = mapped_column(String, index=True, nullable=True)
    agency_mnemonic: Mapped[str] = mapped_column(String, nullable=True)
    beneficiary_name: Mapped[str] = mapped_column(String, nullable=True)
    beneficiary_phone: Mapped[str] = mapped_column(String, nullable=True)
    beneficiary_email: Mapped[str] = mapped_column(String, nullable=True)
    beneficiary_notification_status: Mapped[str] = mapped_column(
        String, default=ProcessStatus.NOT_APPLICABLE.value
    )
    beneficiary_notification_attempts: Mapped[int] = mapped_column(Integer, default=0)
    beneficiary_notification_latest_error_code: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
