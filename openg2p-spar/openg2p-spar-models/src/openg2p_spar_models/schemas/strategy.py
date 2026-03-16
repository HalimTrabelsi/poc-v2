from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict

# Constant for strategy ID key in additional_info
STRATEGY_ID_KEY = "strategy_id"


class StrategyTypeEnum(str, Enum):
    """Enum for Strategy types"""

    ID = "ID"
    FA = "FA"


class ProviderTypeEnum(str, Enum):
    """Enum for FA provider types"""

    BANK = "BANK"
    EMAIL_WALLET = "EMAIL_WALLET"
    MOBILE_WALLET = "MOBILE_WALLET"


class StrategySchema(BaseModel):
    """
    Pydantic schema for Strategy model.

    Used for API responses and data validation.
    """

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    description: Optional[str] = None
    strategy_type: StrategyTypeEnum
    construct_strategy: str
    deconstruct_strategy: str


class StrategyCreateSchema(BaseModel):
    """Schema for creating a new Strategy"""

    description: Optional[str] = None
    strategy_type: StrategyTypeEnum
    construct_strategy: str
    deconstruct_strategy: str


class StrategyUpdateSchema(BaseModel):
    """Schema for updating a Strategy"""

    description: Optional[str] = None
    strategy_type: Optional[StrategyTypeEnum] = None
    construct_strategy: Optional[str] = None
    deconstruct_strategy: Optional[str] = None


class KeyValuePair(BaseModel):
    """
    Schema for key-value pairs used in ID/FA construction and deconstruction.

    Used by StrategyHelper to pass data for constructing and deconstructing
    ID and FA values using regex patterns and format strings.
    """

    key: str
    value: str


class Fa(BaseModel):
    """
    Base schema for Financial Account (FA) data.

    Contains the FA value and associated strategy_id for construction/deconstruction.
    """

    model_config = ConfigDict(from_attributes=True)

    strategy_id: int
    fa_type: ProviderTypeEnum

    def dict(self, **kwargs):
        """Return dictionary representation of the model"""
        return super().model_dump(**kwargs)


class BankAccountFa(Fa):
    """
    Schema for Bank Account FA.

    Used for bank account financial addresses.
    """

    bank_name: str
    bank_code: str
    branch_name: str
    branch_code: str
    account_number: str


class MobileWalletFa(Fa):
    """
    Schema for Mobile Wallet FA.

    Used for mobile wallet financial addresses.
    """

    wallet_provider_name: str
    wallet_provider_code: str
    mobile_number: str


class EmailWalletFa(Fa):
    """
    Schema for Email Wallet FA.

    Used for email wallet financial addresses.
    """

    wallet_provider_name: str
    wallet_provider_code: str
    email_address: str


# Union type for all FA types
FaUnion = Union[BankAccountFa, MobileWalletFa, EmailWalletFa]
