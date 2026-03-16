import logging
import re

from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_agency_allocator.models.agency import (
    G2PAgencyProgramBenefitCode,
)
from openg2p_g2p_bridge_models.schemas import AgencyDetailForPayment
from sqlalchemy.orm import sessionmaker

from ..engine import get_engine

_logger = logging.getLogger("openg2p_g2p_bridge")
_engine = get_engine()


def extract(tag, data):
    if f"#{tag}#" not in data:
        return None
    match = re.search(rf"#{tag}#([^#]*)", data)
    return match.group(1) if match else None


class AgencyHelper(BaseService):
    def retrieve_agency_details(
        self, agency_id: str, benefit_program_id: str, benefit_code_id: str
    ) -> AgencyDetailForPayment:
        """
        Retrieve the agency financial address details from g2p_agency_program_benefit_codes, parsing additional_info for BANK, BRANCH, ACCOUNT, TYPE. Also fetch agency_admin_email and agency_admin_phone from DisbursementBatchControlGeoAttributes using agency_id.
        """
        pbms_session_maker = sessionmaker(bind=_engine.get("db_engine_pbms"), expire_on_commit=False)
        with pbms_session_maker() as session:
            record = (
                session.query(G2PAgencyProgramBenefitCode)
                .filter(
                    G2PAgencyProgramBenefitCode.agency_id == agency_id,
                    G2PAgencyProgramBenefitCode.program_id == benefit_program_id,
                    G2PAgencyProgramBenefitCode.benefit_code_id == benefit_code_id,
                )
                .first()
            )
            if not record or not record.additional_info:
                _logger.error(
                    f"No agency program benefit code found for agency {agency_id}, program {benefit_program_id}, code {benefit_code_id}"
                )
                return None
            info = record.additional_info
            agency_detail_for_payment = AgencyDetailForPayment(
                agency_name=record.agency_name,
                agency_account_number=extract("ACCOUNT", info),
                agency_account_type=extract("TYPE", info),
                agency_account_branch_code=extract("BRANCH", info),
                agency_account_bank_code=extract("BANK", info),
            )
            return agency_detail_for_payment
