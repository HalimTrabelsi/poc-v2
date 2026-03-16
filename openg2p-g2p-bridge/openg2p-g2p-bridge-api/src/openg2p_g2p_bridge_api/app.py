# ruff: noqa: E402
import asyncio
import logging

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_fastapi_common.utils.crypto import KeymanagerCryptoHelper
from openg2p_g2p_bridge_models.models import (
    AccountStatement,
    AccountStatementLob,
    Disbursement,
    DisbursementBatchControl,
    DisbursementBatchControlGeo,
    DisbursementBatchControlGeoAttributes,
    DisbursementEnvelope,
    DisbursementErrorRecon,
    DisbursementRecon,
    DisbursementResolutionFinancialAddress,
    DisbursementResolutionGeoAddress,
    EnvelopeBatchStatusForCash,
    EnvelopeControl,
    NotificationLog,
)
from openg2p_g2pconnect_common_lib.jwt_validation_helper import JWTValidationHelper

from .controllers import (
    AccountStatementController,
    DisbursementController,
    DisbursementEnvelopeController,
    DisbursementEnvelopeStatusController,
    DisbursementStatusController,
)
from .services import (
    AccountStatementService,
    DisbursementEnvelopeService,
    DisbursementEnvelopeStatusService,
    DisbursementService,
    DisbursementStatusService,
    RequestValidation,
)

_logger = logging.getLogger(_config.logging_default_logger_name)


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize()
        RequestValidation()
        DisbursementEnvelopeService()
        DisbursementService()
        AccountStatementService()
        DisbursementStatusService()
        DisbursementEnvelopeStatusService()
        KeymanagerCryptoHelper()
        JWTValidationHelper()
        DisbursementEnvelopeController().post_init()
        DisbursementController().post_init()
        AccountStatementController().post_init()
        DisbursementStatusController().post_init()
        DisbursementEnvelopeStatusController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)

        async def migrate():
            _logger.info("Migrating database")
            await AccountStatement.create_migrate()
            await AccountStatementLob.create_migrate()
            await Disbursement.create_migrate()
            await DisbursementBatchControl.create_migrate()
            await DisbursementBatchControlGeo.create_migrate()
            await DisbursementBatchControlGeoAttributes.create_migrate()
            await DisbursementEnvelope.create_migrate()
            await DisbursementErrorRecon.create_migrate()
            await DisbursementRecon.create_migrate()
            await DisbursementResolutionFinancialAddress.create_migrate()
            await DisbursementResolutionGeoAddress.create_migrate()
            await EnvelopeBatchStatusForCash.create_migrate()
            await EnvelopeControl.create_migrate()
            await NotificationLog.create_migrate()

        asyncio.run(migrate())
