from typing import Optional

from openg2p_g2pconnect_common_lib.schemas import SyncResponse


class AccountStatementResponse(SyncResponse):
    statement_id: Optional[str] = None
    response_error_code: Optional[str] = None
