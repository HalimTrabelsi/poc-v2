import logging
import uuid
from datetime import datetime

from fastapi import UploadFile
from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.models import AccountStatement, AccountStatementLob
from openg2p_g2p_bridge_models.schemas import AccountStatementResponse
from openg2p_g2pconnect_common_lib.schemas import StatusEnum, SyncResponseHeader
from sqlalchemy.ext.asyncio import async_sessionmaker

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class AccountStatementService(BaseService):
    async def upload_mt940(self, statement_file: UploadFile) -> str:
        _logger.info("Uploading statement file")
        try:
            statement_file = await statement_file.read()
        except Exception as e:
            _logger.error(f"Error reading file: {str(e)}")
            raise e

        statement_id = str(uuid.uuid4())
        statement_date = datetime.now()

        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            statement = AccountStatement(
                statement_id=statement_id,
                statement_date=statement_date,
            )
            session.add(statement)

            statement_lob = AccountStatementLob(
                statement_id=statement_id,
                statement_lob=str(statement_file.decode("utf-8")),
            )
            session.add(statement_lob)

            await session.commit()
        _logger.info("Statement file uploaded successfully")
        return statement_id

    async def construct_account_statement_success_response(
        self, statement_id: str
    ) -> AccountStatementResponse:
        _logger.info("Constructing account statement success response")
        return AccountStatementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.succ,
            ),
            message=statement_id,
        )

    async def construct_account_statement_error_response(
        self, code: G2PBridgeErrorCodes
    ) -> AccountStatementResponse:
        _logger.error("Constructing account statement error response")
        return AccountStatementResponse(
            header=SyncResponseHeader(
                message_id="",
                message_ts=datetime.now().isoformat(),
                action="",
                status=StatusEnum.rjct,
            ),
            message=code.value,
        )
