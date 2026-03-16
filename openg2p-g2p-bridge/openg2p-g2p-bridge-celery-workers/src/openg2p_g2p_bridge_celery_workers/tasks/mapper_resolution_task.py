import asyncio
import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    Disbursement,
    DisbursementBatchControl,
    DisbursementResolutionFinancialAddress,
    ProcessStatus,
)
from openg2p_g2pconnect_mapper_lib.client import MapperResolveClient
from openg2p_g2pconnect_mapper_lib.schemas import ResolveRequest

from openg2p_g2p_bridge_mapper_connectors.factory import MapperFactory

from sqlalchemy import exists, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import get_engine
from ..helpers import FAKeys, ResolveHelper

# Configure logging
_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mapper_resolution_worker")
def mapper_resolution_worker(disbursement_batch_control_id: str):
    _logger.info(f"Resolving the batch: {disbursement_batch_control_id}")
    session_maker = sessionmaker(bind=_engine.get("db_engine_bridge"), expire_on_commit=False)

    with session_maker() as session:
        try:
            disbursement_batch_control = (
                session.execute(
                    select(DisbursementBatchControl).filter(
                        DisbursementBatchControl.id == disbursement_batch_control_id
                    )
                )
                .scalars()
                .first()
            )
            if not disbursement_batch_control:
                _logger.error(f"No DisbursementBatchControl found for id {disbursement_batch_control_id}")
                raise ValueError(f"No DisbursementBatchControl found for id {disbursement_batch_control_id}")

            dfa_exists = select(1).where(
                DisbursementResolutionFinancialAddress.disbursement_id == Disbursement.id
            )

            disbursements = (
                session.execute(
                    select(Disbursement).filter(
                        Disbursement.disbursement_batch_control_id == disbursement_batch_control_id,
                        ~exists(dfa_exists),
                    )
                )
                .scalars()
                .all()
            )
            _logger.info(
                f"Found {len(disbursements)} disbursements for batch control {disbursement_batch_control_id}"
            )

            beneficiary_disbursement_map = {d.beneficiary_id: d.id for d in disbursements}
            _logger.info(f"Sending resolve request to url {_config.mapper_resolve_api_url}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                resolve_response, error_msg = loop.run_until_complete(make_resolve_request(disbursements))
            finally:
                loop.close()

            if not resolve_response:
                _logger.error(
                    f"Failed to resolve the request for batch {disbursement_batch_control_id}: {error_msg}"
                )
                raise ValueError(
                    f"Failed to resolve the request for batch {disbursement_batch_control_id}: {error_msg}"
                )

            process_and_store_resolution(
                disbursement_batch_control_id,
                resolve_response,
                beneficiary_disbursement_map,
                session,
            )
            _logger.info(
                f"Mapper resolution completed successfully for batch: {disbursement_batch_control_id}"
            )

        except Exception as e:
            disbursement_batch_control.fa_resolution_latest_error_code = str(e)
            disbursement_batch_control.fa_resolution_timestamp = datetime.now()
            disbursement_batch_control.fa_resolution_attempts += 1
            if disbursement_batch_control.fa_resolution_attempts >= _config.mapper_resolution_max_attempts:
                disbursement_batch_control.fa_resolution_status = ProcessStatus.ERROR.value
            else:
                disbursement_batch_control.fa_resolution_status = ProcessStatus.PENDING.value
            session.add(disbursement_batch_control)
            session.commit()


async def make_resolve_request(disbursements):
    _logger.info("Making resolve request")
    resolve_helper = ResolveHelper.get_component()

    single_resolve_requests = [
        resolve_helper.construct_single_resolve_request(d.beneficiary_id) for d in disbursements
    ]
    resolve_request: ResolveRequest = resolve_helper.construct_resolve_request(single_resolve_requests)

    mapper = MapperFactory.get_component().get_mapper()
    resolve_response: ResolveResponse | None = mapper.resolve(resolve_request)
    if not resolve_response:
        return None, "Failed to resolve the request"
    return resolve_response, None


def process_and_store_resolution(
    disbursement_batch_control_id,
    resolve_response,
    beneficiary_disbursement_map,
    session,
):
    _logger.info("Processing and storing resolution")
    resolve_helper = ResolveHelper.get_component()
    disbursement_resolution_financial_address_list = []
    batch_has_error = False
    for single_response in resolve_response.message.resolve_response:
        _logger.info(f"Processing the response for beneficiary: {single_response.id}")
        disbursement_id = beneficiary_disbursement_map.get(single_response.id)
        if disbursement_id and single_response.fa:
            _logger.info(f"Resolved the request for beneficiary: {single_response.id}")
            deconstructed_fa = resolve_helper.deconstruct_fa(single_response.fa)
            _logger.info(f"Deconstructed FA To Store: {deconstructed_fa}")
            disbursement_resolution_financial_address = DisbursementResolutionFinancialAddress(
                disbursement_batch_control_id=disbursement_batch_control_id,
                disbursement_id=disbursement_id,
                beneficiary_id=single_response.id,
                mapper_resolved_fa=single_response.fa,
                mapper_resolved_name=(
                    single_response.account_provider_info.name
                    if single_response.account_provider_info
                    else None
                ),
                bank_account_number=deconstructed_fa.get(FAKeys.account_number.value, None),
                bank_code=deconstructed_fa.get(FAKeys.bank_code.value, None),
                branch_code=deconstructed_fa.get(FAKeys.branch_code.value, None),
                mapper_resolved_fa_type=deconstructed_fa.get(FAKeys.fa_type.value, None),
                mobile_number=deconstructed_fa.get(FAKeys.mobile_number.value, None),
                mobile_wallet_provider=deconstructed_fa.get(FAKeys.mobile_wallet_provider.value, None),
                email_address=deconstructed_fa.get(FAKeys.email_address.value, None),
                email_wallet_provider=deconstructed_fa.get(FAKeys.email_wallet_provider.value, None),
            )
            disbursement_resolution_financial_address_list.append(disbursement_resolution_financial_address)
        else:
            _logger.error(f"Failed to resolve the request for beneficiary: {single_response.id}")
            batch_has_error = True

    session.add_all(disbursement_resolution_financial_address_list)
    if not batch_has_error:
        _logger.info("Batch has no error")
        session.query(DisbursementBatchControl).filter(
            DisbursementBatchControl.id == disbursement_batch_control_id
        ).update(
            {
                DisbursementBatchControl.fa_resolution_status: ProcessStatus.PROCESSED.value,
                DisbursementBatchControl.sponsor_bank_dispatch_status: ProcessStatus.PENDING.value,
                DisbursementBatchControl.fa_resolution_timestamp: datetime.now(),
                DisbursementBatchControl.fa_resolution_latest_error_code: None,
                DisbursementBatchControl.fa_resolution_attempts: DisbursementBatchControl.fa_resolution_attempts
                + 1,
            }
        )
    else:
        _logger.info("Batch has error")
        raise ValueError(f"Batch {disbursement_batch_control_id} has errors in mapper resolution")
    _logger.info("Stored the resolution")
    session.flush()
    session.commit()
