import re
from typing import List

import orjson
from openg2p_fastapi_auth_models.models import LoginProvider
from openg2p_fastapi_auth_models.schemas import AuthCredentials
from openg2p_fastapi_common.service import BaseService
from openg2p_spar_models.models import Strategy
from openg2p_spar_models.schemas import (
    STRATEGY_ID_KEY,
    Fa,
    KeyValuePair,
)


class StrategyHelper(BaseService):
    async def _construct(self, values: List[KeyValuePair], strategy_id: int) -> str:
        strategy: Strategy = await Strategy().get_strategy(id=strategy_id)
        try:
            constructed_str = strategy.construct_strategy.format(
                **{key_value.key: key_value.value for key_value in values}
            )
            return constructed_str
        except Exception as e:
            raise ValueError("Error while constructing ID/FA.") from e

    def _deconstruct(self, value: str, strategy: str) -> List[KeyValuePair]:
        regex_res = re.match(strategy, value)
        deconstructed_list = []
        if regex_res:
            regex_res = regex_res.groupdict()
            try:
                deconstructed_list = [
                    KeyValuePair(key=k, value=v) for k, v in regex_res.items()
                ]
            except Exception as e:
                raise ValueError("Error while deconstructing ID/FA") from e
        return deconstructed_list

    async def construct_id(
        self,
        auth: AuthCredentials,
    ) -> str:
        login_provider: LoginProvider = await LoginProvider.get_login_provider_from_iss(
            auth.iss
        )
        constructed_id = await self._construct(
            [
                KeyValuePair(
                    key=key,
                    value=(
                        value
                        if isinstance(value, str)
                        else orjson.dumps(value).decode()
                    ),
                )
                for key, value in auth.model_dump().items()
            ],
            login_provider.strategy_id,
        )

        return constructed_id

    async def construct_fa(self, fa: Fa) -> str:
        constructed_fa = await self._construct(
            [
                KeyValuePair(
                    key=key,
                    value=(
                        value
                        if isinstance(value, str)
                        else orjson.dumps(value).decode().strip('"')
                    ),
                )
                for key, value in fa.dict().items()
            ],
            fa.strategy_id,
        )
        return constructed_fa

    async def deconstruct_fa(self, fa: str, additional_info: List[dict]) -> dict:
        strategy_id = additional_info[0].get(STRATEGY_ID_KEY)
        if strategy_id:
            strategy = await Strategy.get_strategy(
                id=strategy_id,
            )
            if strategy:
                deconstructed_pairs = self._deconstruct(
                    fa, strategy.deconstruct_strategy
                )
                deconstructed_fa = {
                    pair.key: pair.value for pair in deconstructed_pairs
                }
                deconstructed_fa["strategy_id"] = strategy_id
                return deconstructed_fa
        return {}

    async def deconstruct_fa_test(self, fa: str, strategy_id: int) -> dict:
        if strategy_id:
            strategy = await Strategy.get_strategy(
                id=strategy_id,
            )
            if strategy:
                deconstructed_pairs = self._deconstruct(
                    fa, strategy.deconstruct_strategy
                )
                deconstructed_fa = {
                    pair.key: pair.value for pair in deconstructed_pairs
                }

                return deconstructed_fa
        return {}
