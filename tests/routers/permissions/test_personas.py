"""The persona tripwire: every role a user can hold has a canary persona acting with it (#810)."""

from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole
from tests.routers.permissions.canary import ADMIN, PERSONAS, ROLE_PERSONAS, VISIBILITY


def test_personas_every_role_has_a_persona():
    """Each user role and each collection role maps to a persona the canary signs in as, so a new role can't go
    unswept."""
    roles = [*UserRole, *ContributionRole]
    missing = [role for role in roles if role not in ROLE_PERSONAS]

    assert not missing, (
        "Roles no canary persona acts with. Plant a persona holding each in tests/routers/permissions/canary.py "
        f"and add it to ROLE_PERSONAS: {missing}"
    )


def test_personas_role_personas_are_swept():
    """Each persona a role maps to is one the sweep calls routes as, or the admin, who is the positive control."""
    unswept = [persona for persona in ROLE_PERSONAS.values() if persona not in (*PERSONAS, ADMIN)]

    assert not unswept, f"ROLE_PERSONAS names personas the sweep never uses: {unswept}"


def test_personas_visibility_names_only_personas():
    """Every persona ``VISIBILITY`` grants a record to is one the sweep calls routes as."""
    unknown = {persona for personas in VISIBILITY.values() for persona in personas} - set(PERSONAS)

    assert not unknown, f"VISIBILITY names personas the sweep never uses: {sorted(unknown)}"
