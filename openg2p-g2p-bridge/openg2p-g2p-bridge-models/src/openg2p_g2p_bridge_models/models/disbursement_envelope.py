from datetime import datetime
from enum import Enum

from sqlalchemy import Date, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import BaseORMModelWithId


class FundsAvailableWithBankEnum(Enum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    PENDING_CHECK = "PENDING_CHECK"
    CHECK_IN_PROGRESS = "CHECK_IN_PROGRESS"
    FUNDS_AVAILABLE = "FUNDS_AVAILABLE"
    FUNDS_NOT_AVAILABLE = "FUNDS_NOT_AVAILABLE"
    ERROR = "ERROR"


class FundsBlockedWithBankEnum(Enum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    PENDING_CHECK = "PENDING_CHECK"
    CHECK_IN_PROGRESS = "CHECK_IN_PROGRESS"
    FUNDS_BLOCK_SUCCESS = "FUNDS_BLOCK_SUCCESS"
    FUNDS_BLOCK_FAILURE = "FUNDS_BLOCK_FAILURE"
    ERROR = "ERROR"


class DisbursementFrequency(Enum):
    Daily = "Daily"
    Weekly = "Weekly"
    Fortnightly = "Fortnightly"
    Monthly = "Monthly"
    BiMonthly = "BiMonthly"
    Quarterly = "Quarterly"
    SemiAnnually = "SemiAnnually"
    Annually = "Annually"
    OnDemand = "OnDemand"


class CancellationStatus(Enum):
    NOT_CANCELLED = "Not_Cancelled"
    CANCELLED = "Cancelled"


class BenefitType(Enum):
    COMMODITY = "COMMODITY"
    SERVICE = "SERVICE"
    CASH_DIGITAL = "CASH_DIGITAL"
    CASH_PHYSICAL = "CASH_PHYSICAL"
    COMBINATION = "COMBINATION"


class DisbursementEnvelope(BaseORMModelWithId):
    __tablename__ = "disbursement_envelopes"
    benefit_program_id: Mapped[int] = mapped_column(Integer)
    benefit_program_mnemonic: Mapped[str] = mapped_column(String)
    benefit_program_description: Mapped[str] = mapped_column(String, nullable=True)
    target_registry: Mapped[str] = mapped_column(String, nullable=True)
    benefit_code_id: Mapped[int] = mapped_column(Integer)
    benefit_code_mnemonic: Mapped[str] = mapped_column(String)
    benefit_code_description: Mapped[str] = mapped_column(String, nullable=True)
    benefit_type: Mapped[BenefitType] = mapped_column(String)
    disbursement_cycle_id: Mapped[int] = mapped_column(Integer)
    disbursement_frequency: Mapped[DisbursementFrequency] = mapped_column(String)
    cycle_code_mnemonic: Mapped[str] = mapped_column(String)
    number_of_beneficiaries: Mapped[int] = mapped_column(Integer)
    number_of_disbursements: Mapped[int] = mapped_column(Integer)
    total_disbursement_quantity: Mapped[float] = mapped_column(Integer)
    measurement_unit: Mapped[str] = mapped_column(String)
    disbursement_schedule_date: Mapped[datetime.date] = mapped_column(Date())
    receipt_time_stamp: Mapped[datetime] = mapped_column(DateTime(), default=datetime.now())
    cancellation_status: Mapped[CancellationStatus] = mapped_column(
        String, default=CancellationStatus.NOT_CANCELLED.value
    )
    cancellation_timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=True, default=None)


class EnvelopeControl(BaseORMModelWithId):
    __tablename__ = "envelope_control"
    disbursement_envelope_id: Mapped[str] = mapped_column(String, unique=True)
    number_of_disbursements_received: Mapped[int] = mapped_column(Integer, default=0)
    total_disbursement_quantity_received: Mapped[int] = mapped_column(Integer, default=0)


class EnvelopeBatchStatusForCash(BaseORMModelWithId):
    __tablename__ = "envelope_batch_status_for_cash"
    disbursement_envelope_id: Mapped[str] = mapped_column(String, unique=True)
    funds_available_with_bank: Mapped[FundsAvailableWithBankEnum] = mapped_column(String)
    funds_available_latest_timestamp: Mapped[datetime] = mapped_column(
        DateTime(), default=None, nullable=True
    )
    funds_available_latest_error_code: Mapped[str] = mapped_column(String, nullable=True)
    funds_available_attempts: Mapped[int] = mapped_column(Integer, default=0)
    funds_blocked_with_bank: Mapped[FundsBlockedWithBankEnum] = mapped_column(String)
    funds_blocked_latest_timestamp: Mapped[datetime] = mapped_column(DateTime(), default=None, nullable=True)
    funds_blocked_latest_error_code: Mapped[str] = mapped_column(String, nullable=True)
    funds_blocked_attempts: Mapped[int] = mapped_column(Integer, default=0)
    funds_blocked_reference_number: Mapped[str] = mapped_column(String, nullable=True)
    number_of_disbursements_shipped: Mapped[int] = mapped_column(Integer, default=0)
    number_of_disbursements_reconciled: Mapped[int] = mapped_column(Integer, default=0)
    number_of_disbursements_reversed: Mapped[int] = mapped_column(Integer, default=0)
