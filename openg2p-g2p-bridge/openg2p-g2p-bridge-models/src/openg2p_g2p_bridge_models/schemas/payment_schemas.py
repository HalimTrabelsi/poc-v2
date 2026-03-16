from typing import Optional

from pydantic import BaseModel


class SponsorBankConfiguration(BaseModel):
    program_account_number: str
    program_account_type: Optional[str] = None
    program_account_branch_code: str
    sponsor_bank_code: str


class AgencyDetailForPayment(BaseModel):
    agency_name: str
    agency_account_number: str
    agency_account_type: Optional[str] = None
    agency_account_branch_code: str
    agency_account_bank_code: str
