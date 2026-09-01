"""Tests for forwarding URNs that publication has retired."""

import pytest

from mavedb.lib.urn_redirects import forwarded_path, record_urn_redirect
from mavedb.models.urn_redirect import UrnRedirect

from tests.helpers.constants import VALID_SCORE_SET_URN

RETIRED_URN = "tmp:00000000-0000-4000-8000-000000000001"
PUBLISHED_URN = VALID_SCORE_SET_URN


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
        """Retire a temporary URN onto a real, public score set.

        A real record is needed, not just a row in the table: forwarding withholds a target it cannot
        confirm is public, so a redirect pointing at nothing forwards nowhere.
        """
        setup_lib_db_with_score_set.private = False
        record_urn_redirect(session, RETIRED_URN, PUBLISHED_URN)
        session.commit()

    def test_withholds_a_target_that_is_private(self, session, retired, setup_lib_db_with_score_set):
        setup_lib_db_with_score_set.private = True
        session.commit()

        assert forwarded_path(session, f"/api/v1/score-sets/{RETIRED_URN}") is None

    def test_withholds_a_target_that_does_not_exist(self, session):
        """A deleted record leaves its redirect row behind."""
        record_urn_redirect(session, RETIRED_URN, PUBLISHED_URN)
        session.commit()

        assert forwarded_path(session, f"/api/v1/score-sets/{RETIRED_URN}") is None

    def test_forwards_a_retired_urn(self, session, retired):
        assert forwarded_path(session, f"/api/v1/score-sets/{RETIRED_URN}") == f"/api/v1/score-sets/{PUBLISHED_URN}"

    def test_forwards_a_sub_resource_of_a_retired_urn(self, session, retired):
        assert (
            forwarded_path(session, f"/api/v1/score-sets/{RETIRED_URN}/scores")
            == f"/api/v1/score-sets/{PUBLISHED_URN}/scores"
        )

    def test_forwards_a_variant_of_a_retired_score_set(self, session, retired):
        """A variant URN is built from its score set's, so the score set's redirect carries it."""
        assert forwarded_path(session, f"/api/v1/variants/{RETIRED_URN}#4") == f"/api/v1/variants/{PUBLISHED_URN}#4"

    def test_leaves_a_path_naming_no_temporary_urn_alone(self, session, retired):
        assert forwarded_path(session, f"/api/v1/score-sets/{PUBLISHED_URN}") is None

    def test_leaves_a_live_temporary_urn_alone(self, session, retired):
        """An unpublished record still answers to its temporary URN, and has no row in the table."""
        live_urn = "tmp:00000000-0000-4000-8000-000000000002"
        assert forwarded_path(session, f"/api/v1/score-sets/{live_urn}") is None
