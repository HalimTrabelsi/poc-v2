from enum import Enum

from .dfsp import (
    BankSchema,
    BanksRequest,
    BanksRequestBody,
    BanksRequestPayload,
    BanksResponse,
    BanksResponseBody,
    BanksResponsePayload,
    BranchesRequest,
    BranchesRequestBody,
    BranchesRequestPayload,
    BranchesResponse,
    BranchesResponseBody,
    BranchesResponsePayload,
    BranchSchema,
    WalletServiceProviderSchema,
    WalletServiceProvidersRequest,
    WalletServiceProvidersRequestBody,
    WalletServiceProvidersRequestPayload,
    WalletServiceProvidersResponse,
    WalletServiceProvidersResponseBody,
    WalletServiceProvidersResponsePayload,
)
from .link import (
    LinkRequest,
    LinkRequestBody,
    LinkRequestPayload,
    LinkResponse,
    LinkResponseBody,
    LinkResponsePayload,
    LinkStatusReasonCode,
    SingleLinkRequest,
    SingleLinkResponse,
    StatusEnum,
)
from .resolve import (
    AccountProviderInfo,
    ResolveRequest,
    ResolveRequestBody,
    ResolveRequestPayload,
    ResolveResponse,
    ResolveResponseBody,
    ResolveResponsePayload,
    ResolveScope,
    ResolveStatusReasonCode,
    SingleResolveRequest,
    SingleResolveResponse,
)
from .strategy import (
    STRATEGY_ID_KEY,
    BankAccountFa,
    EmailWalletFa,
    Fa,
    FaUnion,
    KeyValuePair,
    MobileWalletFa,
    StrategyCreateSchema,
    StrategySchema,
    StrategyTypeEnum,
    StrategyUpdateSchema,
)
from .strategy import (
    ProviderTypeEnum as StrategyProviderTypeEnum,
)
from .unlink import (
    SingleUnlinkRequest,
    SingleUnlinkResponse,
    UnlinkRequest,
    UnlinkRequestBody,
    UnlinkRequestPayload,
    UnlinkResponse,
    UnlinkResponseBody,
    UnlinkResponsePayload,
    UnlinkStatusReasonCode,
)
from .update import (
    SingleUpdateRequest,
    SingleUpdateResponse,
    UpdateRequest,
    UpdateRequestBody,
    UpdateRequestPayload,
    UpdateResponse,
    UpdateResponseBody,
    UpdateResponsePayload,
    UpdateStatusReasonCode,
)


class StatusReasonCodeEnum(Enum):
    rjct_action_not_supported = "rjct.action.not_supported"
    rjct_jwt_invalid = "rjct.jwt.invalid"
    rjct_signature_invalid = "rjct.signature.invalid"
    rjct_permission_denied = "rjct.permission.denied"
    rjct_internal_error = "rjct.internal.error"
