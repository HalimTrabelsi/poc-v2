from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column


class Bank(BaseORMModelWithTimes):
    """
    Bank model for storing bank information.

    Represents a bank with its mnemonic, name, and code.

    Attributes:
        bank_mnemonic: Unique mnemonic identifier for the bank (e.g., "ICIC", "HDFC")
        bank_name: Display name of the bank
        bank_code: Bank code (e.g., IFSC code prefix)
    """

    __tablename__ = "banks"

    bank_mnemonic: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True
    )
    bank_name: Mapped[str] = mapped_column(String(255), nullable=False)
    bank_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)


class Branch(BaseORMModelWithTimes):
    """
    Branch model for storing bank branch information.

    Represents a branch of a bank with its mnemonic, name, code, and parent bank.

    Attributes:
        branch_mnemonic: Unique mnemonic identifier for the branch (e.g., "BRANCH_001")
        branch_name: Display name of the branch
        branch_code: Branch code (e.g., IFSC code)
        bank_id: Foreign key to the parent Bank
    """

    __tablename__ = "branches"

    branch_mnemonic: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True
    )
    branch_name: Mapped[str] = mapped_column(String(255), nullable=False)
    branch_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    bank_id: Mapped[int] = mapped_column(Integer(), nullable=False, index=True)

    @classmethod
    async def get_by_bank_id(cls, bank_id: int):
        """
        Get all branches for a specific bank.

        Args:
            bank_id: The bank ID to filter by

        Returns:
            List of Branch objects for the specified bank
        """
        from openg2p_fastapi_common.context import dbengine
        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import AsyncSession

        session = AsyncSession(dbengine.get())
        try:
            stmt = (
                select(cls)
                .where(cls.bank_id == bank_id)
                .order_by(cls.branch_name.asc())
            )
            result = await session.execute(stmt)
            return list(result.scalars())
        finally:
            await session.aclose()


class WalletServiceProvider(BaseORMModelWithTimes):
    """
    WalletServiceProvider model for storing wallet service provider information.

    Represents a wallet service provider (e.g., email wallet, mobile wallet) with its
    mnemonic, name, code, and wallet type.

    Attributes:
        sp_mnemonic: Unique mnemonic identifier for the service provider (e.g., "PAYPAL", "AIRTEL")
        sp_name: Display name of the service provider
        sp_code: Service provider code
        wallet_type: Type of wallet (EMAIL_WALLET, MOBILE_WALLET)
    """

    __tablename__ = "wallet_service_providers"

    sp_mnemonic: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True
    )
    sp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    sp_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    wallet_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
