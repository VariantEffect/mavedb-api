from unittest.mock import Mock

from mavedb.lib.annotation.calibration import CALIBRATION_SCOPE_EXTENSION_NAME
from mavedb.lib.permissions.principal import Principal
from mavedb.models.enums.user_role import UserRole
from tests.helpers.constants import PRIVATE_CALIBRATION_OWNER_ID


def admin_principal() -> Principal:
    return Principal(Mock(user=Mock(id=1, username="admin"), active_roles=[UserRole.admin]))


def owner_principal(owner_id: int = PRIVATE_CALIBRATION_OWNER_ID) -> Principal:
    return Principal(Mock(user=Mock(id=owner_id, username="owner"), active_roles=[]))


def scope_of(annotation) -> str:
    """The disclosed principal of an annotation, which every emitted object must carry."""
    scopes = [
        extension.value
        for extension in (annotation.extensions or [])
        if extension.name == CALIBRATION_SCOPE_EXTENSION_NAME
    ]
    assert len(scopes) == 1, f"expected exactly one calibration scope extension, found {scopes}"
    return scopes[0]
