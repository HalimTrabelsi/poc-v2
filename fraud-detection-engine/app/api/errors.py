"""Custom exception classes and FastAPI exception handlers."""
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class BeneficiaryNotFoundError(Exception):
    """Raised when a beneficiary_id cannot be found in OpenG2P."""

    def __init__(self, beneficiary_id: str) -> None:
        self.beneficiary_id = beneficiary_id
        super().__init__(f"Beneficiary not found: {beneficiary_id}")


class ModelNotReadyError(Exception):
    """Raised when the ML model artifacts have not been loaded successfully."""

    def __init__(self, detail: str = "ML models are not ready") -> None:
        super().__init__(detail)


class RuleEngineError(Exception):
    """Raised when the rule engine encounters an unrecoverable error."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)


class FeatureExtractionError(Exception):
    """Raised when feature extraction from the database fails."""

    def __init__(self, beneficiary_id: str, cause: str) -> None:
        self.beneficiary_id = beneficiary_id
        super().__init__(f"Feature extraction failed for {beneficiary_id}: {cause}")


def register_error_handlers(app: FastAPI) -> None:
    """Attach exception handlers to a FastAPI application instance."""

    @app.exception_handler(BeneficiaryNotFoundError)
    async def beneficiary_not_found_handler(
        request: Request, exc: BeneficiaryNotFoundError
    ) -> JSONResponse:
        logger.warning("Beneficiary not found: %s", exc.beneficiary_id)
        return JSONResponse(
            status_code=404,
            content={"error": "beneficiary_not_found", "detail": str(exc)},
        )

    @app.exception_handler(ModelNotReadyError)
    async def model_not_ready_handler(
        request: Request, exc: ModelNotReadyError
    ) -> JSONResponse:
        logger.error("Model not ready: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"error": "model_not_ready", "detail": str(exc)},
        )

    @app.exception_handler(RuleEngineError)
    async def rule_engine_error_handler(
        request: Request, exc: RuleEngineError
    ) -> JSONResponse:
        logger.error("Rule engine error: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"error": "rule_engine_error", "detail": str(exc)},
        )

    @app.exception_handler(FeatureExtractionError)
    async def feature_extraction_error_handler(
        request: Request, exc: FeatureExtractionError
    ) -> JSONResponse:
        logger.error("Feature extraction error: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"error": "feature_extraction_error", "detail": str(exc)},
        )

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"error": "internal_server_error", "detail": "An unexpected error occurred"},
        )
