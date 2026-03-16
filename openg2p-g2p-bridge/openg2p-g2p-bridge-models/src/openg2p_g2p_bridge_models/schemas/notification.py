import datetime
from typing import List, Optional

from pydantic import BaseModel


class NotificationPayload(BaseModel):
    """Base class for all notification payloads."""

    program_mnemonic: Optional[str] = None
    program_description: Optional[str] = None


class WarehouseNotificationPayload(NotificationPayload):
    target_registry: Optional[str] = None
    disbursement_cycle_mnemonic: Optional[str] = None
    disbursement_date: Optional[datetime.datetime] = None
    benefit_code_id: Optional[int] = None
    benefit_code_mnemonic: Optional[str] = None
    benefit_code_description: Optional[str] = None
    benefit_type: Optional[str] = None
    measurement_unit: Optional[str] = None
    benefit_description: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_mnemonic: Optional[str] = None
    warehouse_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_mnemonic: Optional[str] = None
    agency_name: Optional[str] = None
    agency_description: Optional[str] = None
    total_quantity: Optional[float] = None
    no_of_bebeficiaries: Optional[int] = None
    administrative_zone_id_large: Optional[str] = None
    administrative_zone_mnemonic_large: Optional[str] = None
    administrative_zone_id_small: Optional[str] = None
    administrative_zone_mnemonic_small: Optional[str] = None


class BeneficiaryEntitlement(BaseModel):
    beneficiary_id: Optional[str] = None
    beneficiary_name: Optional[str] = None
    total_quantity: Optional[float] = None


class AgencyNotificationPayload(NotificationPayload):
    target_registry: Optional[str] = None
    disbursement_cycle_mnemonic: Optional[str] = None
    disbursement_date: Optional[datetime.datetime] = None
    benefit_code_id: Optional[int] = None
    benefit_code_mnemonic: Optional[str] = None
    benefit_code_description: Optional[str] = None
    benefit_type: Optional[str] = None
    measurement_unit: Optional[str] = None
    benefit_description: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_mnemonic: Optional[str] = None
    warehouse_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_mnemonic: Optional[str] = None
    agency_name: Optional[str] = None
    total_quantity: Optional[float] = None
    no_of_bebeficiaries: Optional[int] = None
    administrative_zone_id_large: Optional[str] = None
    administrative_zone_mnemonic_large: Optional[str] = None
    administrative_zone_id_small: Optional[str] = None
    administrative_zone_mnemonic_small: Optional[str] = None
    beneficiary_entitlements: Optional[List[BeneficiaryEntitlement]] = None


class BeneficiaryNotificationPayload(NotificationPayload):
    beneficiary_id: Optional[str] = None
    beneficiary_name: Optional[str] = None
    total_quantity: Optional[float] = None
    program_mnemonic: Optional[str] = None
    program_description: Optional[str] = None
    target_registry: Optional[str] = None
    disbursement_cycle_mnemonic: Optional[str] = None
    disbursement_date: Optional[datetime.datetime] = None
    benefit_code_id: Optional[int] = None
    benefit_code_mnemonic: Optional[str] = None
    benefit_code_description: Optional[str] = None
    benefit_type: Optional[str] = None
    measurement_unit: Optional[str] = None
    benefit_description: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_mnemonic: Optional[str] = None
    warehouse_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_mnemonic: Optional[str] = None
    agency_name: Optional[str] = None
    total_quantity: Optional[float] = None
    administrative_zone_id_large: Optional[str] = None
    administrative_zone_mnemonic_large: Optional[str] = None
    administrative_zone_id_small: Optional[str] = None
    administrative_zone_mnemonic_small: Optional[str] = None


class NotificationRequest(BaseModel):
    notification_type: str
    recipient: str
    recipient_type: str
    notification_payload: NotificationPayload
    disbursement_control_geo_id: Optional[str] = None
    agency_mnemonic: Optional[str] = None
    beneficiary_id: Optional[str] = None
    disbursement_id: Optional[str] = None
    notification_request_id: Optional[str] = None
