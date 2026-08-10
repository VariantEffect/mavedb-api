from unittest.mock import Mock

from mavedb.lib.permissions.principal import Principal
from mavedb.models.enums.user_role import UserRole
from tests.helpers.constants import PRIVATE_CALIBRATION_OWNER_ID


def admin_principal() -> Principal:
    return Principal(Mock(user=Mock(id=1, username="admin"), active_roles=[UserRole.admin]))


def owner_principal(owner_id: int = PRIVATE_CALIBRATION_OWNER_ID) -> Principal:
    return Principal(Mock(user=Mock(id=owner_id, username="owner"), active_roles=[]))
