import logging
import magic
from fastapi import UploadFile
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.exceptions import RequestValidationException
from openg2p_g2pconnect_common_lib.schemas import SyncResponseStatusReasonCodeEnum

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class RequestValidation(BaseService):
    def validate_signature(self, is_signature_valid) -> None:
        _logger.info("Validating signature")
        if not is_signature_valid:
            _logger.error("Invalid JWT signature")
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_jwt_invalid,
                message=SyncResponseStatusReasonCodeEnum.rjct_jwt_invalid,
            )

        _logger.info("Signature validated successfully")
        return None

    def validate_create_disbursement_envelope_request_header(self, request) -> None:
        _logger.info("Validating create disbursement envelope request header")
        if request.header.action != "create_disbursement_envelopes":
            _logger.error(f"Unsupported action: {request.header.action}")
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
            )
        _logger.info("Create disbursement envelope request header validated successfully")
        return None

    def validate_cancel_disbursement_envelope_request_header(self, request) -> None:
        _logger.info("Validating cancel disbursement envelope request header")
        if request.header.action != "cancel_disbursement_envelope":
            _logger.error(f"Unsupported action: {request.header.action}")
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
            )
        _logger.info("Cancel disbursement envelope request header validated successfully")
        return None

    def validate_request(self, request) -> None:
        _logger.info("Validating request")
        _logger.info("Request validated successfully")
        return None

    def validate_mt940_file(self, request: UploadFile) -> None:
        _logger.info("Validating MT940 file")
        # --- size check (unchanged) ---
        request.file.seek(0, 2)
        file_size = request.file.tell()
        request.file.seek(0)
        if file_size > _config.max_upload_file_size:
            _logger.error(
                f"File size {file_size} exceeds maximum allowed size {_config.max_upload_file_size}"
            )
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_file_size_exceeded,
                message=SyncResponseStatusReasonCodeEnum.rjct_file_size_exceeded,
            )

        # --- header MIME check (optional) ---
        if request.content_type not in _config.supported_file_types:
            _logger.error(f"File type {request.content_type} is not supported")
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_file_type_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_file_type_not_supported,
            )

        # read a small chunk to detect the real MIME type
        sample = request.file.read(1024)
        request.file.seek(0)
        detector = magic.Magic(mime=True)
        real_mime = detector.from_buffer(sample)
        if real_mime not in _config.supported_file_types:
            _logger.error(f"Detected file type {real_mime} is not supported")
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_file_type_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_file_type_not_supported,
            )

        _logger.info("MT940 file validated successfully")
        return None
