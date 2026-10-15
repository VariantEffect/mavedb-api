# ruff: noqa: E402

from urllib.parse import quote, unquote

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel

from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    create_seq_score_set,
    create_seq_score_set_with_variants,
    publish_score_set,
)

UNKNOWN_TMP_URN = "tmp:00000000-0000-4000-8000-00000000ffff"

COLLECTIONS = {"experiment_set": "experiment-sets", "experiment": "experiments", "score_set": "score-sets"}


@pytest.fixture
def published(session, data_provider, client, setup_router_db, data_files):
    """Publish a score set, and report what each of its records was called before and after."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = publish_score_set(client, score_set["urn"])

    return {
        "experiment_set": (experiment["experimentSetUrn"], published_score_set["experiment"]["experimentSetUrn"]),
        "experiment": (experiment["urn"], published_score_set["experiment"]["urn"]),
        "score_set": (score_set["urn"], published_score_set["urn"]),
    }


@pytest.mark.integration
class TestForwardingRetiredUrns:
    @pytest.mark.parametrize("record", ["experiment_set", "experiment", "score_set"])
    def test_retired_urn_is_forwarded_to_the_published_record(self, client, published, record):
        """Publication renames all three records, so a stale link to any of them has to resolve."""
        retired_urn, published_urn = published[record]
        collection = COLLECTIONS[record]

        response = client.get(f"/api/v1/{collection}/{retired_urn}", follow_redirects=False)

        assert response.status_code == 308
        assert response.headers["location"] == f"/api/v1/{collection}/{published_urn}"

    @pytest.mark.parametrize("record", ["experiment_set", "experiment", "score_set"])
    def test_a_client_following_the_forward_reaches_the_record(self, client, published, record):
        retired_urn, published_urn = published[record]
        collection = COLLECTIONS[record]

        response = client.get(f"/api/v1/{collection}/{retired_urn}")

        assert response.status_code == 200
        assert response.json()["urn"] == published_urn

    def test_a_sub_resource_of_a_retired_urn_is_forwarded(self, client, published):
        """The complaint in mavedb-ui#617 is about a page, which loads more than the record itself."""
        retired_urn, published_urn = published["score_set"]

        response = client.get(f"/api/v1/score-sets/{retired_urn}/scores", follow_redirects=False)

        assert response.status_code == 308
        assert response.headers["location"] == f"/api/v1/score-sets/{published_urn}/scores"

    def test_a_variant_of_a_retired_score_set_is_forwarded(self, client, published):
        """A variant URN is derived from its score set's, and needs no redirect of its own."""
        retired_urn, published_urn = published["score_set"]

        variant_path = f"/api/v1/variants/{quote(f'{retired_urn}#1', safe='')}/csv-namespaces"

        response = client.get(variant_path, follow_redirects=False)

        assert response.status_code == 308
        location = response.headers["location"]
        assert unquote(location) == f"/api/v1/variants/{published_urn}#1/csv-namespaces"
        # The '#' has to come back encoded, or a client reads the rest of the location as a fragment.
        assert "%23" in location
        assert client.get(variant_path).status_code == 200

    def test_a_query_string_survives_forwarding(self, client, published):
        retired_urn, published_urn = published["score_set"]

        response = client.get(f"/api/v1/score-sets/{retired_urn}/scores?start=0&limit=1", follow_redirects=False)

        assert response.status_code == 308
        assert response.headers["location"] == f"/api/v1/score-sets/{published_urn}/scores?start=0&limit=1"

    def test_a_live_temporary_urn_is_served_not_forwarded(self, client, setup_router_db):
        """An unpublished record still answers to the temporary URN it was created with."""
        experiment = create_experiment(client)
        score_set = create_seq_score_set(client, experiment["urn"])

        response = client.get(f"/api/v1/score-sets/{score_set['urn']}", follow_redirects=False)

        assert response.status_code == 200
        assert response.json()["urn"] == score_set["urn"]

    def test_an_unknown_temporary_urn_is_still_a_404(self, client, setup_router_db):
        response = client.get(f"/api/v1/score-sets/{UNKNOWN_TMP_URN}", follow_redirects=False)

        assert response.status_code == 404

    @pytest.mark.parametrize(
        "record,model",
        [
            ("score_set", ScoreSetDbModel),
            ("experiment", ExperimentDbModel),
            ("experiment_set", ExperimentSetDbModel),
        ],
    )
    def test_a_retired_urn_is_not_forwarded_to_a_private_record(self, session, client, published, record, model):
        """A Location header names its target, to an anonymous caller, before any route checks anything.

        Publication only records a redirect onto a record it is making public, and nothing in the
        application returns a published record to private, so this state is reached here the only way
        it could be reached in production: out of band.
        """
        retired_urn, published_urn = published[record]
        session.query(model).filter(model.urn == published_urn).one().private = True
        session.commit()

        response = client.get(f"/api/v1/{COLLECTIONS[record]}/{retired_urn}", follow_redirects=False)

        assert response.status_code == 404
        assert published_urn not in response.text

    def test_a_retired_urn_is_not_forwarded_to_a_deleted_record(self, session, client, published):
        """A deleted record leaves its redirect row behind, pointing at a URN that resolves to nothing."""
        retired_urn, published_urn = published["score_set"]
        session.delete(session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == published_urn).one())
        session.commit()

        response = client.get(f"/api/v1/score-sets/{retired_urn}", follow_redirects=False)

        assert response.status_code == 404
        assert published_urn not in response.text

    def test_a_write_to_a_retired_urn_is_not_forwarded(self, client, published):
        """Forwarding a stale publish request would rename the live public record it reached."""
        retired_urn, _ = published["score_set"]

        response = client.post(f"/api/v1/score-sets/{retired_urn}/publish", follow_redirects=False)

        assert response.status_code == 404
