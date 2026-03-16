import datetime
from typing import List, Optional

from openg2p_g2pconnect_common_lib.schemas import Request, SyncResponse
from pydantic import BaseModel

from ..errors.codes import G2PBridgeErrorCodes
from ..models import (
    BenefitType,
    CancellationStatus,
    DisbursementFrequency,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)


class DisbursementEnvelopePayload(BaseModel):
    id: Optional[str] = None
    benefit_program_id: Optional[int] = None
    benefit_program_mnemonic: Optional[str] = None
    benefit_program_description: Optional[str] = None
    target_registry: Optional[str] = None
    benefit_code_id: Optional[int] = None
    benefit_code_mnemonic: Optional[str] = None
    benefit_code_description: Optional[str] = None
    benefit_type: Optional[BenefitType] = None
    disbursement_cycle_id: Optional[int] = None
    disbursement_frequency: Optional[DisbursementFrequency] = None
    cycle_code_mnemonic: Optional[str] = None
    number_of_beneficiaries: Optional[int] = None
    number_of_disbursements: Optional[int] = None
    total_disbursement_quantity: Optional[float] = None
    measurement_unit: Optional[str] = None
    disbursement_schedule_date: Optional[datetime.date] = None
    receipt_time_stamp: Optional[datetime.datetime] = None
    cancellation_status: Optional[CancellationStatus] = None
    cancellation_timestamp: Optional[datetime.datetime] = None


class DisbursementEnvelopeRequest(Request):
    message: List[DisbursementEnvelopePayload]


class DisbursementEnvelopeResponse(SyncResponse):
    message: Optional[List[DisbursementEnvelopePayload]] = None


class DisbursementStatusRequest(Request):
    message: List[str]


class DisbursementReconPayload(BaseModel):
    bank_disbursement_batch_id: str
    disbursement_id: str
    disbursement_envelope_id: Optional[str] = None
    beneficiary_name_from_bank: Optional[str] = None

    remittance_reference_number: Optional[str] = None
    remittance_statement_id: Optional[str] = None
    remittance_statement_number: Optional[str] = None
    remittance_statement_sequence: Optional[str] = None
    remittance_entry_sequence: Optional[str] = None
    remittance_entry_date: Optional[datetime.datetime] = None
    remittance_value_date: Optional[datetime.datetime] = None

    reversal_found: Optional[bool] = None
    reversal_statement_id: Optional[str] = None
    reversal_statement_number: Optional[str] = None
    reversal_statement_sequence: Optional[str] = None
    reversal_entry_sequence: Optional[str] = None
    reversal_entry_date: Optional[datetime.datetime] = None
    reversal_value_date: Optional[datetime.datetime] = None
    reversal_reason: Optional[str] = None


class DisbursementErrorReconPayload(BaseModel):
    statement_id: Optional[str] = None
    statement_number: Optional[str] = None
    statement_sequence: Optional[str] = None
    entry_sequence: Optional[str] = None
    entry_date: Optional[datetime.datetime] = None
    value_date: Optional[datetime.datetime] = None
    error_reason: Optional[G2PBridgeErrorCodes] = None
    disbursement_id: str
    bank_reference_number: Optional[str] = None


class DisbursementReconRecords(BaseModel):
    disbursement_recon_payloads: Optional[List[DisbursementReconPayload]] = None
    disbursement_error_recon_payloads: Optional[List[DisbursementErrorReconPayload]] = None


class DisbursementStatusPayload(BaseModel):
    disbursement_id: str
    disbursement_recon_records: Optional[DisbursementReconRecords] = None


class DisbursementStatusResponse(SyncResponse):
    message: Optional[List[DisbursementStatusPayload]] = None


class DisbursementEnvelopeStatusRequest(Request):
    message: str


class DisbursementBatchControlGeoPayload(BaseModel):
    disbursement_batch_control_geo_id: str
    disbursement_cycle_id: str
    disbursement_envelope_id: str
    disbursement_batch_control_id: str
    administrative_zone_id_large: Optional[str] = None
    administrative_zone_mnemonic_large: Optional[str] = None
    administrative_zone_id_small: Optional[str] = None
    administrative_zone_mnemonic_small: Optional[str] = None
    no_of_beneficiaries: Optional[int] = None
    total_quantity: Optional[float] = None
    warehouse_id: Optional[str] = None
    warehouse_mnemonic: Optional[str] = None
    warehouse_additional_attributes: Optional[str] = None
    agency_id: Optional[str] = None
    agency_mnemonic: Optional[str] = None
    agency_additional_attributes: Optional[str] = None
    warehouse_notification_status: Optional[str] = None
    agency_notification_status: Optional[str] = None


class DisbursementEnvelopeStatusPayload(BaseModel):
    disbursement_envelope_id: str
    benefit_code_id: Optional[int] = None
    benefit_code_mnemonic: Optional[str] = None
    benefit_type: Optional[str] = None
    measurement_unit: Optional[str] = None
    number_of_beneficiaries_received: Optional[int] = None
    number_of_beneficiaries_declared: Optional[int] = None
    number_of_disbursements_declared: Optional[int] = None
    number_of_disbursements_received: int
    total_disbursement_quantity_declared: float = None
    total_disbursement_quantity_received: int

    funds_available_with_bank: Optional[FundsAvailableWithBankEnum] = None
    funds_available_latest_timestamp: Optional[datetime.datetime] = None
    funds_available_latest_error_code: Optional[str] = None
    funds_available_attempts: Optional[int] = None

    funds_blocked_with_bank: Optional[FundsBlockedWithBankEnum] = None
    funds_blocked_latest_timestamp: Optional[datetime.datetime] = None
    funds_blocked_latest_error_code: Optional[str] = None
    funds_blocked_attempts: int
    funds_blocked_reference_number: Optional[str] = None

    number_of_disbursements_shipped: Optional[int] = None
    number_of_disbursements_reconciled: Optional[int] = None
    number_of_disbursements_reversed: Optional[int] = None

    no_of_warehouses_allocated: Optional[int] = None
    no_of_warehouses_notified: Optional[int] = None
    no_of_agencies_allocated: Optional[int] = None
    no_of_agencies_notified: Optional[int] = None
    no_of_beneficiaries_notified: Optional[int] = None
    no_of_pods_received: Optional[int] = None
    disbursement_batch_control_geos: Optional[List[DisbursementBatchControlGeoPayload]] = None


class DisbursementEnvelopeStatusResponse(SyncResponse):
    message: Optional[DisbursementEnvelopeStatusPayload] = None
