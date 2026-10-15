"""Tests for forwarding URNs that publication has retired."""

# ruff: noqa: E402

import pytest

# The module under test reaches fastapi and starlette directly, and arq, biocommons and cdot through
# mavedb.deps, all of which live in the `server` extra. Guarding the import keeps the core-dependency
# CI job skipping this module rather than failing to collect it. The DB fixtures need psycopg2.
pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")
pytest.importorskip("arq")
pytest.importorskip("cdot")

from mavedb.lib.temp_urns import generate_temp_urn
from mavedb.lib.urn_redirects import forwarded_path, record_urn_redirect
from mavedb.models.urn_redirect import UrnRedirect

from tests.helpers.constants import VALID_EXPERIMENT_SET_URN, VALID_EXPERIMENT_URN, VALID_SCORE_SET_URN

RETIRED_URN = "tmp:00000000-0000-4000-8000-000000000001"
LIVE_TMP_URN = "tmp:00000000-0000-4000-8000-000000000002"
PUBLISHED_URN = VALID_SCORE_SET_URN

# Every entity type publication renames. Adding an entry extends the fixture and each parametrized
# test below, which are all driven by these keys.
#
#   urn             the URN setup_lib_db_with_score_set gives this entity
#   route           the collection its URN sits under in a request path
#   score_set_path  the attribute path reaching it from that score set, empty for the score set
#                   itself. Every type here is renamed by score set publication; an entity with
#                   its own publishing workflow would need the fixture to build it another way.
#   sub_resource    a path segment the API serves after its URN, or None where it serves none
ENTITY_TYPES = {
    "score_set": {
        "urn": VALID_SCORE_SET_URN,
        "route": "score-sets",
        "score_set_path": "",
        "sub_resource": "scores",
    },
    "experiment": {
        "urn": VALID_EXPERIMENT_URN,
        "route": "experiments",
        "score_set_path": "experiment",
        "sub_resource": "score-sets",
    },
    "experiment_set": {
        "urn": VALID_EXPERIMENT_SET_URN,
        "route": "experiment-sets",
        "score_set_path": "experiment.experiment_set",
        "sub_resource": None,
    },
}

ENTITY_TYPES_WITH_SUB_RESOURCE = [name for name, entity in ENTITY_TYPES.items() if entity["sub_resource"]]


@pytest.mark.integration
class TestRecordUrnRedirect:
    def test_records_a_rename(self, session):
        record_urn_redirect(session, RETIRED_URN, PUBLISHED_URN)
        session.commit()

        redirect = session.query(UrnRedirect).one()
        assert redirect.old_urn == RETIRED_URN
        assert redirect.new_urn == PUBLISHED_URN

    def test_ignores_a_record_that_had_no_urn(self, session):
        record_urn_redirect(session, None, PUBLISHED_URN)
        session.commit()

        assert session.query(UrnRedirect).count() == 0

    def test_ignores_a_rename_that_changes_nothing(self, session):
        record_urn_redirect(session, PUBLISHED_URN, PUBLISHED_URN)
        session.commit()

        assert session.query(UrnRedirect).count() == 0


@pytest.mark.integration
class TestForwardedPath:
    @pytest.fixture
    def retired(self, session, setup_lib_db_with_score_set):
        """Retire a temporary URN onto a public record of every entity type, keyed by type.

        A real record is needed, not just a row in the table: forwarding withholds a target it cannot
        confirm is public, so a redirect pointing at nothing forwards nowhere.
        """
        retired = {}
        for entity_type, entity in ENTITY_TYPES.items():
            record = setup_lib_db_with_score_set
            for attribute in filter(None, entity["score_set_path"].split(".")):
                record = getattr(record, attribute)

            record.private = False

            # A redirect row is unique on the URN it retires, so every type needs its own.
            retired_urn = generate_temp_urn()
            record_urn_redirect(session, retired_urn, record.urn)
            retired[entity_type] = (retired_urn, record)

        session.commit()
        return retired

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES)
    def test_forwards_a_retired_urn(self, session, retired, entity_type):
        retired_urn, record = retired[entity_type]
        route = ENTITY_TYPES[entity_type]["route"]

        assert forwarded_path(session, f"/api/v1/{route}/{retired_urn}") == f"/api/v1/{route}/{record.urn}"

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES_WITH_SUB_RESOURCE)
    def test_forwards_a_sub_resource_of_a_retired_urn(self, session, retired, entity_type):
        retired_urn, record = retired[entity_type]
        route = ENTITY_TYPES[entity_type]["route"]
        sub_resource = ENTITY_TYPES[entity_type]["sub_resource"]

        assert (
            forwarded_path(session, f"/api/v1/{route}/{retired_urn}/{sub_resource}")
            == f"/api/v1/{route}/{record.urn}/{sub_resource}"
        )

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES)
    def test_withholds_a_target_that_is_private(self, session, retired, entity_type):
        retired_urn, record = retired[entity_type]
        route = ENTITY_TYPES[entity_type]["route"]

        record.private = True
        session.commit()

        assert forwarded_path(session, f"/api/v1/{route}/{retired_urn}") is None

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES)
    def test_withholds_a_target_that_does_not_exist(self, session, entity_type):
        """A deleted record leaves its redirect row behind. Without the fixture, none of these exist."""
        entity = ENTITY_TYPES[entity_type]

        record_urn_redirect(session, RETIRED_URN, entity["urn"])
        session.commit()

        assert forwarded_path(session, f"/api/v1/{entity['route']}/{RETIRED_URN}") is None

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES)
    def test_leaves_a_path_naming_no_temporary_urn_alone(self, session, retired, entity_type):
        _, record = retired[entity_type]
        route = ENTITY_TYPES[entity_type]["route"]

        assert forwarded_path(session, f"/api/v1/{route}/{record.urn}") is None

    def test_forwards_a_variant_of_a_retired_score_set(self, session, retired):
        """A variant URN is built from its score set's, so the score set's redirect carries it."""
        retired_urn, score_set = retired["score_set"]

        assert forwarded_path(session, f"/api/v1/variants/{retired_urn}#4") == f"/api/v1/variants/{score_set.urn}#4"

    @pytest.mark.parametrize("entity_type", ENTITY_TYPES)
    def test_leaves_a_live_temporary_urn_alone(self, session, retired, entity_type):
        """An unpublished record still answers to its temporary URN, and has no row in the table."""
        route = ENTITY_TYPES[entity_type]["route"]

        assert forwarded_path(session, f"/api/v1/{route}/{LIVE_TMP_URN}") is None
