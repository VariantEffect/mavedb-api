import logging
from typing import Optional

from mavedb.lib.logging.context import logging_context, save_to_logging_context

logger = logging.getLogger(__name__)


class PermissionResponse:
    """The outcome of a permission check.

    Deliberately not truthy. Callers must read `.permitted`; evaluating the response
    object itself in a boolean context silently resolves any permission check to True.
    `__bool__` raises to ensure this contract is enforced.
    """

    def __bool__(self) -> bool:
        raise TypeError(
            "PermissionResponse is not truthy; check `.permitted` instead. "
            "A bare `if has_permission(...)` is always true and silently permits everything."
        )

    def __init__(self, permitted: bool, http_code: int = 403, message: Optional[str] = None):
        self.permitted = permitted
        self.http_code = http_code if not permitted else None
        self.message = message if not permitted else None

        save_to_logging_context({"permission_message": self.message, "access_permitted": self.permitted})
        if self.permitted:
            logger.debug(
                msg="Access to the requested resource is permitted.",
                extra=logging_context(),
            )
        else:
            logger.debug(
                msg="Access to the requested resource is not permitted.",
                extra=logging_context(),
            )
