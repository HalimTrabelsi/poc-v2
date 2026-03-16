from typing import List, Optional

from ..schemas import DisbursementPayload
from .codes import G2PBridgeErrorCodes


class RequestValidationException(Exception):
    def __init__(self, code: G2PBridgeErrorCodes, message: Optional[str] = None):
        self.code: G2PBridgeErrorCodes = code
        self.message: Optional[str] = message
        super().__init__(self.message)


class DisbursementEnvelopeException(Exception):
    def __init__(self, code: G2PBridgeErrorCodes, message: Optional[str] = None):
        self.code: G2PBridgeErrorCodes = code
        self.message: Optional[str] = message
        super().__init__(self.message)


class DisbursementException(Exception):
    def __init__(
        self,
        code: G2PBridgeErrorCodes,
        disbursement_payloads: List[DisbursementPayload],
        message: Optional[str] = None,
    ):
        self.code: G2PBridgeErrorCodes = code
        self.message: Optional[str] = message
        self.disbursement_payloads: List[DisbursementPayload] = disbursement_payloads
        super().__init__(code, self.message)


class AccountStatementException(Exception):
    def __init__(self, code: G2PBridgeErrorCodes, message: Optional[str] = None):
        self.code: G2PBridgeErrorCodes = code
        self.message: Optional[str] = message
        super().__init__(self.message)


class DisbursementStatusException(Exception):
    def __init__(
        self,
        code: G2PBridgeErrorCodes,
        message: Optional[str] = None,
    ):
        self.code: G2PBridgeErrorCodes = code
        self.message: Optional[str] = message
        super().__init__(code, self.message)
