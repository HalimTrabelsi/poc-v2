import base64
import enum
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import httpx
import orjson
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.models import MapperResolvedFaType
from openg2p_g2pconnect_common_lib.schemas import RequestHeader
from openg2p_g2pconnect_mapper_lib.schemas import (
    ResolveRequest,
    ResolveRequestMessage,
    SingleResolveRequest,
)
from pydantic import BaseModel

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class FAKeys(enum.Enum):
    account_number = "account_number"
    bank_code = "bank_code"
    branch_code = "branch_code"
    account_type = "account_type"
    mobile_number = "mobile_number"
    mobile_wallet_provider = "mobile_wallet_provider"
    email_address = "email_address"
    email_wallet_provider = "email_wallet_provider"
    fa_type = "fa_type"


class KeyValuePair(BaseModel):
    key: FAKeys
    value: str


class ResolveHelper(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)
        self._keymanager_auth_token: str = None
        self._keymanager_auth_token_expiry: datetime = None

    def construct_single_resolve_request(self, id: str) -> SingleResolveRequest:
        _logger.info(f"Constructing single resolve request for ID: {id}")
        single_resolve_request = SingleResolveRequest(
            reference_id=str(uuid.uuid4()),
            timestamp=str(datetime.now()),
            id=id,
            scope="details",
        )
        _logger.info(f"Constructed single resolve request for ID: {id}")
        return single_resolve_request

    def construct_resolve_request(
        self, single_resolve_requests: List[SingleResolveRequest]
    ) -> ResolveRequest:
        _logger.info(
            f"Constructing resolve request for {len(single_resolve_requests)} single resolve requests"
        )
        resolve_request_message = ResolveRequestMessage(
            transaction_id=str(uuid.uuid4()),
            resolve_request=single_resolve_requests,
        )

        resolve_request = ResolveRequest(
            header=RequestHeader(
                message_id=str(uuid.uuid4()),
                message_ts=str(datetime.now()),
                action="resolve",
                sender_id=_config.mapper_request_sender_id,
                sender_uri="",
                total_count=len(single_resolve_requests),
            ),
            message=resolve_request_message,
        )
        _logger.info(
            f"Constructed resolve request for {len(single_resolve_requests)} single resolve requests"
        )
        return resolve_request

    def _deconstruct(self, value: str, strategy: str) -> List[KeyValuePair]:
        _logger.info(f"Deconstructing ID/FA: {value}")
        regex_res = re.match(strategy, value)
        deconstructed_list = []
        if regex_res:
            regex_res = regex_res.groupdict()
            try:
                # Coalesce None (from optional groups) to empty strings
                deconstructed_list = [
                    KeyValuePair(key=FAKeys(k), value=(v if v is not None else ""))
                    for k, v in regex_res.items()
                ]
            except Exception as e:
                _logger.error(f"Error while deconstructing ID/FA: {e}")
                raise ValueError("Error while deconstructing ID/FA") from e
        _logger.info(f"Deconstructed ID/FA: {value}")
        return deconstructed_list

    def deconstruct_fa(self, fa: str) -> dict:
        _logger.info("Deconstructing FA")
        deconstruct_strategy = self._get_deconstruct_strategy(fa)
        _logger.info(f"Deconstruction strategy: {deconstruct_strategy}")
        if deconstruct_strategy:
            deconstructed_pairs = self._deconstruct(fa, deconstruct_strategy)
            deconstructed_fa = {pair.key.value: pair.value for pair in deconstructed_pairs}
            _logger.info(f"Deconstructed FA Returning: {deconstructed_fa}")
            return deconstructed_fa
        return {}

    def _get_deconstruct_strategy(self, fa: str) -> str:
        _logger.info("Getting deconstruction strategy")
        if fa.endswith(MapperResolvedFaType.BANK_ACCOUNT.value):
            return _config.bank_fa_deconstruct_strategy
        elif fa.endswith(MapperResolvedFaType.MOBILE_WALLET.value):
            return _config.mobile_wallet_fa_deconstruct_strategy
        elif fa.endswith(MapperResolvedFaType.EMAIL_WALLET.value):
            return _config.email_wallet_fa_deconstruct_strategy
        _logger.info("Deconstruction strategy not found!")
        return ""

    async def create_jwt_token(
        self,
        payload,
        expiration_minutes=60,
        include_payload=False,
        include_certificate=False,
        include_cert_hash=False,
    ):
        if isinstance(payload, dict):
            payload = orjson.dumps(payload)
        elif isinstance(payload, str):
            payload = payload.encode()
        cookies = {}
        if _config.oauth_enabled:
            cookies["Authorization"] = await self.get_keymanager_auth_token()
        current_time = self.get_current_isotimestamp()
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{_config.keymanager_api_base_url}/jwtSign",
                json={
                    "id": "string",
                    "version": "string",
                    "requesttime": current_time,
                    "metadata": {},
                    "request": {
                        "dataToSign": self.urlsafe_b64encode(payload),
                        "applicationId": _config.sign_key_keymanager_app_id or "",
                        "referenceId": _config.sign_key_keymanager_ref_id or "",
                        "includePayload": include_payload,
                        "includeCertificate": include_certificate,
                        "includeCertHash": include_cert_hash,
                    },
                },
                cookies=cookies,
                timeout=_config.keymanager_api_timeout,
            )
        _logger.debug("Keymanager JWT Sign API response: %s", response.text)
        response.raise_for_status()
        return ((response.json() or {}).get("response") or {}).get("jwtSignedData")

    async def get_keymanager_auth_token(self):
        if (
            self._keymanager_auth_token
            and self._keymanager_auth_token_expiry
            and self._keymanager_auth_token_expiry > datetime.now(timezone.utc)
        ):
            return self._keymanager_auth_token
        url = _config.oauth_url
        payload = {
            "client_id": _config.oauth_client_id,
            "client_secret": _config.oauth_client_secret,
            "grant_type": "client_credentials",
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=payload, timeout=_config.keymanager_api_timeout)
        response_data = response.json()
        expires_in = response_data.get("expires_in", 900)
        self._keymanager_auth_token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        self._keymanager_auth_token = response_data["access_token"]
        return self._keymanager_auth_token

    def urlsafe_b64encode(self, input_data: bytes) -> str:
        return base64.urlsafe_b64encode(input_data).decode().rstrip("=")

    def get_current_isotimestamp(self):
        return f"{datetime.now().isoformat(timespec='milliseconds')}Z"
