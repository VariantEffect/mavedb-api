# ruff: noqa: E402

"""Citing publications under a row-level security read policy on ``publication_identifiers`` (#945).

The api's roles (#827) and the real policy (#809) don't exist yet, so these tests stand them in: a role without
login rights that isn't a superuser and doesn't own the tables, and a policy that shows a publication only when a
record the caller can see cites it. "Can see" is reduced to public or created by the caller. #809 replaces this
fixture with the real policy.
"""

from contextlib import contextmanager

import pytest
from sqlalchemy import event, func, select, text

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from tests.helpers.constants import TEST_BIORXIV_IDENTIFIER, TEST_PUBMED_IDENTIFIER, TEST_USER
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set
from tests.helpers.util.user import change_ownership

API_ROLE = "mavedb_test_rls_api"

# A record makes a publication visible when it cites it and the caller can see the record.
VISIBLE_CITATION = """
    EXISTS (
        SELECT 1 FROM {citations} c JOIN {owners} o ON o.id = c.{owner_id}
         WHERE c.db_name = publication_identifiers.db_name AND c.identifier = publication_identifiers.identifier
           AND (NOT o.private OR o.created_by_id = current_setting('mavedb_test.user_id')::int)
    )
"""
READ_POLICY = " OR ".join(
    VISIBLE_CITATION.format(citations=citations, owners=owners, owner_id=owner_id)
    for citations, owners, owner_id in [
        ("scoreset_publication_identifiers", "scoresets", "scoreset_id"),
        ("experiment_publication_identifiers", "experiments", "experiment_id"),
        ("score_calibration_publication_identifiers", "score_calibrations", "score_calibration_id"),
    ]
)


@pytest.fixture
def as_api_role(session, setup_router_db):
    """Enable the read policy, and return a context manager that runs the requests inside it as the api role."""
    session.execute(
        text(
            f"""
            DO $$
            BEGIN
                CREATE ROLE {API_ROLE} NOLOGIN;
            -- Roles span the cluster, which parallel test databases share.
            EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
            END $$
            """
        )
    )
    session.execute(text(f"GRANT USAGE ON SCHEMA public TO {API_ROLE}"))
    session.execute(text(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {API_ROLE}"))
    session.execute(text(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {API_ROLE}"))
    session.execute(text("ALTER TABLE publication_identifiers ENABLE ROW LEVEL SECURITY"))
    session.execute(text(f"CREATE POLICY read_cited ON publication_identifiers FOR SELECT USING ({READ_POLICY})"))
    session.execute(text("CREATE POLICY write_any ON publication_identifiers FOR INSERT WITH CHECK (true)"))
    session.commit()

    caller_id = session.scalars(select(User.id).filter_by(username=TEST_USER["username"])).one()

    def switch_role(session, transaction, connection):
        connection.exec_driver_sql(
            f"SET LOCAL ROLE {API_ROLE}; SELECT set_config('mavedb_test.user_id', '{caller_id}', true)"
        )

    @contextmanager
    def acting():
        # Start the next transaction, which each request opens, inside the role.
        session.commit()
        event.listen(session, "after_begin", switch_role)
        try:
            yield
        finally:
            event.remove(session, "after_begin", switch_role)
            session.rollback()

    return acting


def publication_rows(session, db_name, identifier):
    return session.scalars(select(PublicationIdentifier).filter_by(db_name=db_name, identifier=identifier)).all()


def cited_keys(score_set):
    return [
        (publication["dbName"], publication["identifier"]) for publication in score_set["primaryPublicationIdentifiers"]
    ]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cite_publication_cited_only_by_another_users_private_record_reuses_it(
    client, session, as_api_role, mock_publication_fetch
):
    publication = {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER}
    experiment = create_experiment(client)
    others = create_seq_score_set(client, experiment["urn"], update={"primaryPublicationIdentifiers": [publication]})
    change_ownership(session, others["urn"], ScoreSet)
    (stored,) = publication_rows(session, "PubMed", TEST_PUBMED_IDENTIFIER)

    with as_api_role():
        assert publication_rows(session, "PubMed", TEST_PUBMED_IDENTIFIER) == []
        score_set = create_seq_score_set(
            client, experiment["urn"], update={"primaryPublicationIdentifiers": [publication]}
        )

    assert [row.id for row in publication_rows(session, "PubMed", TEST_PUBMED_IDENTIFIER)] == [stored.id]
    assert score_set["primaryPublicationIdentifiers"][0]["id"] == stored.id


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cite_publication_new_to_mavedb_stores_and_cites_it(client, session, as_api_role, mock_publication_fetch):
    experiment = create_experiment(client)

    with as_api_role():
        score_set = create_seq_score_set(
            client,
            experiment["urn"],
            update={"primaryPublicationIdentifiers": [{"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER}]},
        )

    assert cited_keys(score_set) == [("bioRxiv", TEST_BIORXIV_IDENTIFIER)]
    assert len(publication_rows(session, "bioRxiv", TEST_BIORXIV_IDENTIFIER)) == 1


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cite_new_publication_in_both_lists_fetches_it_once(
    client, session, as_api_role, mock_publication_fetch, requests_mock
):
    """The row stored for one list stays hidden until its citation commits, so the lists must resolve together."""
    publication = {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER}
    experiment = create_experiment(client)
    requests_mock.reset_mock()

    with as_api_role():
        score_set = create_seq_score_set(
            client,
            experiment["urn"],
            update={"primaryPublicationIdentifiers": [publication], "secondaryPublicationIdentifiers": [publication]},
        )

    assert requests_mock.call_count == 1
    assert cited_keys(score_set) == [("bioRxiv", TEST_BIORXIV_IDENTIFIER)]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_search_external_publication_hidden_matches_publication_not_in_mavedb(
    client, session, as_api_role, mock_publication_fetch
):
    def search():
        response = client.post("/api/v1/publication-identifiers/search-external", json={"text": TEST_PUBMED_IDENTIFIER})
        assert response.status_code == 200
        return response.json()

    # The stored row is cited by nothing, so the policy hides it. Without the policy, its stored metadata shows.
    (stored,) = publication_rows(session, "PubMed", TEST_PUBMED_IDENTIFIER)
    stored.title = "Stored, not fetched"
    session.commit()
    visible = search()
    with as_api_role():
        hidden = search()
    session.execute(PublicationIdentifier.__table__.delete())
    session.commit()
    absent = search()

    assert hidden == absent
    assert visible != absent
    assert session.scalar(select(func.count()).select_from(PublicationIdentifier)) == 0
