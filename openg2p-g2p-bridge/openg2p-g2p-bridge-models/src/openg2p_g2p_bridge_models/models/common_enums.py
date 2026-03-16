import enum


class ProcessStatus(enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"
    NOT_APPLICABLE = "NOT_APPLICABLE"
