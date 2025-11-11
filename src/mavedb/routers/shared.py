from typing import Any, Mapping, Union

ROUTER_BASE_PREFIX = "/api/v1"

BASE_RESPONSES: Mapping[int, dict[str, Any]] = {
    400: {"description": "Bad request. Check parameters and payload."},
    401: {"description": "Authentication required."},
    403: {"description": "Forbidden. Insufficient permissions."},
    404: {"description": "Resource not found."},
    409: {"description": "Conflict with current resource state."},
    416: {"description": "Requested range not satisfiable."},
    422: {"description": "Unprocessable entity. Validation failed."},
    429: {"description": "Too many requests. Rate limit exceeded."},
    500: {"description": "Internal server error."},
    501: {"description": "Not implemented. The server does not support the functionality required."},
    502: {"description": "Bad gateway. Upstream responded invalidly."},
    503: {"description": "Service unavailable. Temporary overload or maintenance."},
    504: {"description": "Gateway timeout. Upstream did not respond in time."},
}

BASE_400_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {400: BASE_RESPONSES[400]}
BASE_401_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {401: BASE_RESPONSES[401]}
BASE_403_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {403: BASE_RESPONSES[403]}
BASE_404_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {404: BASE_RESPONSES[404]}
BASE_409_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {409: BASE_RESPONSES[409]}
BASE_416_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {416: BASE_RESPONSES[416]}
BASE_422_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {422: BASE_RESPONSES[422]}
BASE_429_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {429: BASE_RESPONSES[429]}
BASE_500_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {500: BASE_RESPONSES[500]}
BASE_501_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {501: BASE_RESPONSES[501]}
BASE_502_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {502: BASE_RESPONSES[502]}
BASE_503_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {503: BASE_RESPONSES[503]}
BASE_504_RESPONSE: Mapping[Union[int, str], dict[str, Any]] = {504: BASE_RESPONSES[504]}

PUBLIC_ERROR_RESPONSES = {**BASE_404_RESPONSE, **BASE_500_RESPONSE}
ACCESS_CONTROL_ERROR_RESPONSES = {**BASE_401_RESPONSE, **BASE_403_RESPONSE}
VALIDATION_ERROR_RESPONSES = {**BASE_400_RESPONSE, **BASE_422_RESPONSE}
GATEWAY_ERROR_RESPONSES = {**BASE_502_RESPONSE, **BASE_503_RESPONSE, **BASE_504_RESPONSE}
