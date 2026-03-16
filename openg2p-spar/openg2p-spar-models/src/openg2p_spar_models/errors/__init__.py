from .error_codes import SPARMapperErrorCodes


class SPARMapperException(Exception):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code
        self.message = message
