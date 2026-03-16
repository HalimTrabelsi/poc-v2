from openg2p_spar_models.schemas import (
    LinkStatusReasonCode,
    ResolveStatusReasonCode,
    StatusEnum,
    UnlinkStatusReasonCode,
    UpdateStatusReasonCode,
)


class LinkValidationException(Exception):
    def __init__(
        self, message, status: StatusEnum, validation_error_type: LinkStatusReasonCode
    ):
        self.message = message
        super().__init__(self.message)
        self.status: StatusEnum = status
        self.validation_error_type: LinkStatusReasonCode = validation_error_type


class UpdateValidationException(Exception):
    def __init__(
        self, message, status: StatusEnum, validation_error_type: UpdateStatusReasonCode
    ):
        self.message = message
        super().__init__(self.message)
        self.status: StatusEnum = status
        self.validation_error_type: UpdateStatusReasonCode = validation_error_type


class ResolveValidationException(Exception):
    def __init__(
        self,
        message,
        status: StatusEnum,
        validation_error_type: ResolveStatusReasonCode,
    ):
        self.message = message
        super().__init__(self.message)
        self.status: StatusEnum = status
        self.validation_error_type: ResolveStatusReasonCode = validation_error_type


class UnlinkValidationException(Exception):
    def __init__(
        self, message, status: StatusEnum, validation_error_type: UnlinkStatusReasonCode
    ):
        self.message = message
        super().__init__(self.message)
        self.status: StatusEnum = status
        self.validation_error_type: UnlinkStatusReasonCode = validation_error_type


class RequestValidationException(Exception):
    # TODO : Add code
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(self.message)
