"""The RLS policies (#809), checked as the api's login with row-level security enabled.

The rest of the suite runs as a superuser, which skips every policy, so these tests enable RLS on the tables
they read, in the test's own database, and log in as ``mavedb_api`` with the principal sent the way
``mavedb.db.principal`` sends it. Rows are seeded as the superuser and named, and each test compares the names a
persona can see with the names it should.
"""

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, ProgrammingError

from mavedb.db.rls import API_ROLE, WORKER_ROLE

pytestmark = pytest.mark.integration

RECORD_TABLES = ("scoresets", "experiments", "experiment_sets", "score_calibrations", "collections")

# How a row is identified, for tables without an id column.
ROW_KEYS = {
    "collection_score_sets": "collection_id || ':' || score_set_id",
    "collection_experiments": "collection_id || ':' || experiment_id",
}

DATES = {"creation_date": "2026-01-01", "modification_date": "2026-01-01"}


@dataclass(frozen=True)
class Persona:
    user: Optional[str]
    roles: str = "{}"


PERSONAS = {
    "anonymous": Persona(None),
    "stranger": Persona("stranger"),
    "owner": Persona("owner"),
    "contributor": Persona("contributor"),
    "mapper": Persona("mapper", "{mapper}"),
    "admin": Persona("admin", "{admin}"),
    "admin_without_role": Persona("admin"),
}


class Seed:
    """Rows inserted as the superuser, by name, and the table each belongs to."""

    def __init__(self, session):
        self.session = session
        self.ids: dict[str, object] = {}
        self.rows: dict[str, set[str]] = defaultdict(set)

    def add(self, table: str, label: Optional[str] = None, /, **values) -> object:
        columns = ", ".join(f'"{column}"' for column in values)
        params = ", ".join(f":{column}" for column in values)
        key = self.session.execute(
            text(f"INSERT INTO {table} ({columns}) VALUES ({params}) RETURNING {ROW_KEYS.get(table, 'id')}"), values
        ).scalar_one()
        if label is not None:
            self.ids[label] = key
            self.rows[table].add(label)
        return key

    def __getitem__(self, name: str):
        return self.ids[name]

    def enable(self, *tables: str) -> None:
        for table in tables:
            self.session.execute(text(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
        self.session.commit()

    def principal(self, persona: Persona) -> dict:
        return {"user_id": "" if persona.user is None else str(self[persona.user]), "roles": persona.roles}


def _send(conn, seed: Seed, persona: Persona) -> None:
    conn.execute(
        text("SELECT set_config('app.user_id', :user_id, true), set_config('app.active_roles', :roles, true)"),
        seed.principal(persona),
    )


def _visible(conn, seed: Seed, table: str) -> set[str]:
    visible = set(conn.execute(text(f"SELECT {ROW_KEYS.get(table, 'id')} FROM {table}")).scalars())
    return {name for name in seed.rows[table] if seed[name] in visible}


@contextmanager
def _as(login_as, seed: Seed, persona: Persona, role: str = API_ROLE):
    with login_as(role).begin() as conn:
        if persona.user is not None:
            _send(conn, seed, persona)
        yield conn


@pytest.fixture
def records(session):
    """Public and private records of each kind, owned by ``owner``."""
    seed = Seed(session)
    for name in ("owner", "contributor", "stranger", "mapper", "admin"):
        seed.ids[name] = seed.add(
            "users",
            username=f"0000-0000-0000-{len(seed.ids):04d}",
            is_superuser=False,
            is_staff=False,
            is_active=True,
            is_first_login=False,
        )
    orcid = session.execute(text("SELECT username FROM users WHERE id = :id"), {"id": seed["contributor"]})
    seed.ids["contributor_row"] = seed.add("contributors", orcid_id=orcid.scalar_one())
    license_id = seed.add("licenses", long_name="l", short_name="l", text="l", active=True, **DATES)
    seed.ids["disease"] = seed.add("mondo_terms", code="MONDO:0000001", system="MONDO", label="d", **DATES)

    for visibility, private in (("public", False), ("private", True)):
        seed.add(
            "experiment_sets",
            f"es_{visibility}",
            extra_metadata="{}",
            private=private,
            approved=False,
            num_experiments=1,
            created_by_id=seed["owner"],
            urn=f"urn:mavedb:es-{visibility}",
            **DATES,
        )
        seed.add(
            "experiments",
            f"e_{visibility}",
            title="t",
            short_description="s",
            abstract_text="a",
            method_text="m",
            extra_metadata="{}",
            private=private,
            approved=False,
            num_scoresets=1,
            experiment_set_id=seed[f"es_{visibility}"],
            external_links="{}",
            created_by_id=seed["owner"],
            urn=f"urn:mavedb:e-{visibility}",
            **DATES,
        )
        seed.add(
            "scoresets",
            f"s_{visibility}",
            title="t",
            method_text="m",
            abstract_text="a",
            short_description="s",
            extra_metadata="{}",
            dataset_columns="{}",
            normalised=False,
            private=private,
            approved=False,
            num_variants=0,
            experiment_id=seed[f"e_{visibility}"],
            licence_id=license_id,
            external_links="{}",
            created_by_id=seed["owner"],
            urn=f"urn:mavedb:s-{visibility}",
            **DATES,
        )
    # Listed on the score set only: the experiment and experiment set reach them through it (#564).
    session.execute(
        text("INSERT INTO scoreset_contributors (scoreset_id, contributor_id) VALUES (:s, :c)"),
        {"s": seed["s_private"], "c": seed["contributor_row"]},
    )

    for name, score_set, private, investigator_provided, creator in (
        ("c_public_on_public", "s_public", False, False, "stranger"),
        ("c_investigator_private_on_public", "s_public", True, True, "owner"),
        ("c_community_private_on_public", "s_public", True, False, "stranger"),
        ("c_public_on_private", "s_private", False, False, "owner"),
        ("c_investigator_private_on_private", "s_private", True, True, "owner"),
    ):
        seed.add(
            "score_calibrations",
            name,
            score_set_id=seed[score_set],
            title=name,
            research_use_only=False,
            primary=False,
            investigator_provided=investigator_provided,
            private=private,
            created_by_id=seed[creator],
            modified_by_id=seed[creator],
            disease_term_id=seed["disease"],
            **DATES,
        )

    seed.add("collections", "col_public", name="p", private=False, created_by_id=seed["stranger"], **DATES)
    seed.add("collections", "col_private", name="q", private=True, created_by_id=seed["owner"], **DATES)
    session.execute(
        text(
            "INSERT INTO collection_user_associations (collection_id, user_id, contribution_role) "
            "VALUES (:c, :u, 'viewer')"
        ),
        {"c": seed["col_private"], "u": seed["contributor"]},
    )
    session.commit()
    return seed


ALL_CALIBRATIONS = {
    "c_public_on_public",
    "c_investigator_private_on_public",
    "c_community_private_on_public",
    "c_public_on_private",
    "c_investigator_private_on_private",
}
PUBLIC_ONLY = {
    "scoresets": {"s_public"},
    "experiments": {"e_public"},
    "experiment_sets": {"es_public"},
    "score_calibrations": {"c_public_on_public"},
    "collections": {"col_public"},
}
BOTH_RECORDS = {
    "scoresets": {"s_public", "s_private"},
    "experiments": {"e_public", "e_private"},
    "experiment_sets": {"es_public", "es_private"},
}

RECORDS_EXPECTED = {
    "anonymous": PUBLIC_ONLY,
    "stranger": {**PUBLIC_ONLY, "score_calibrations": {"c_public_on_public", "c_community_private_on_public"}},
    "owner": {
        **BOTH_RECORDS,
        "score_calibrations": ALL_CALIBRATIONS - {"c_community_private_on_public"},
        "collections": {"col_public", "col_private"},
    },
    "contributor": {
        **BOTH_RECORDS,
        "score_calibrations": {"c_public_on_public", "c_public_on_private", "c_investigator_private_on_private"},
        "collections": {"col_public", "col_private"},
    },
    "mapper": {
        **BOTH_RECORDS,
        "score_calibrations": {"c_public_on_public", "c_public_on_private"},
        "collections": {"col_public"},
    },
    "admin": {**BOTH_RECORDS, "score_calibrations": ALL_CALIBRATIONS, "collections": {"col_public", "col_private"}},
    "admin_without_role": PUBLIC_ONLY,
}


@pytest.fixture
def records_enforcing(records):
    """Row-level security on for the record tables."""
    records.enable(*RECORD_TABLES)


@pytest.mark.usefixtures("records_enforcing")
@pytest.mark.parametrize("table", RECORD_TABLES)
@pytest.mark.parametrize("persona", PERSONAS)
def test_record_policy_persona_sees_exactly_their_rows(records, login_as, persona, table):
    with _as(login_as, records, PERSONAS[persona]) as conn:
        assert _visible(conn, records, table) == RECORDS_EXPECTED[persona][table]


@pytest.mark.usefixtures("records_enforcing")
@pytest.mark.parametrize("table", RECORD_TABLES)
def test_record_policy_worker_sees_every_row(records, login_as, table):
    with _as(login_as, records, PERSONAS["anonymous"], WORKER_ROLE) as conn:
        assert _visible(conn, records, table) == records.rows[table]


@pytest.mark.usefixtures("records_enforcing")
def test_record_policy_principal_left_empty_by_a_previous_transaction_reads_as_anonymous(records, login_as):
    with login_as(API_ROLE).connect() as conn:
        with conn.begin():
            _send(conn, records, PERSONAS["admin"])
        with conn.begin():
            assert _visible(conn, records, "scoresets") == {"s_public"}


@pytest.mark.usefixtures("records_enforcing")
def test_record_policy_insert_returning_own_private_record_succeeds(records, login_as):
    with _as(login_as, records, PERSONAS["stranger"]) as conn:
        row = conn.execute(
            text(
                "INSERT INTO experiment_sets (extra_metadata, private, approved, num_experiments, created_by_id, "
                "creation_date, modification_date) VALUES ('{}', true, false, 0, :me, now(), now()) RETURNING id"
            ),
            {"me": records["stranger"]},
        ).scalar_one()

    assert row is not None


@pytest.mark.usefixtures("records_enforcing")
def test_record_policy_insert_on_behalf_of_another_user_is_refused(records, login_as):
    with _as(login_as, records, PERSONAS["stranger"]) as conn:
        with pytest.raises(ProgrammingError, match="row-level security"):
            conn.execute(
                text(
                    "INSERT INTO experiment_sets (extra_metadata, private, approved, num_experiments, "
                    "created_by_id, creation_date, modification_date) "
                    "VALUES ('{}', true, false, 0, :owner, now(), now())"
                ),
                {"owner": records["owner"]},
            )


@pytest.mark.usefixtures("records_enforcing")
def test_record_policy_update_of_a_hidden_row_changes_nothing(records, login_as, session):
    with _as(login_as, records, PERSONAS["stranger"]) as conn:
        updated = conn.execute(text("UPDATE scoresets SET title = 'x' WHERE id = :id"), {"id": records["s_private"]})

    assert updated.rowcount == 0
    title = session.execute(text("SELECT title FROM scoresets WHERE id = :id"), {"id": records["s_private"]})
    assert title.scalar_one() == "t"


@pytest.mark.usefixtures("records_enforcing")
def test_calibration_policy_contributor_removed_from_score_set_sees_neither_it_nor_their_calibration(
    records, login_as, session
):
    calibration = records.add(
        "score_calibrations",
        score_set_id=records["s_private"],
        title="added by the contributor",
        research_use_only=False,
        primary=False,
        investigator_provided=True,
        private=True,
        created_by_id=records["contributor"],
        modified_by_id=records["contributor"],
        disease_term_id=records["disease"],
        **DATES,
    )
    session.execute(
        text("DELETE FROM scoreset_contributors WHERE contributor_id = :c"), {"c": records["contributor_row"]}
    )
    session.commit()

    with _as(login_as, records, PERSONAS["contributor"]) as conn:
        score_sets = set(conn.execute(text("SELECT id FROM scoresets")).scalars())
        calibrations = set(conn.execute(text("SELECT id FROM score_calibrations")).scalars())

    assert records["s_private"] not in score_sets
    assert calibration not in calibrations


@pytest.fixture
def targets(records):
    """A sequence and an accession under each score set, with no target gene yet."""
    seed = records
    for visibility in ("public", "private"):
        seed.add(
            "target_sequences",
            f"ts_{visibility}",
            sequence_type="dna",
            sequence="ATG",
            score_set_id=seed[f"s_{visibility}"],
            **DATES,
        )
        seed.add(
            "target_accessions",
            f"ta_{visibility}",
            accession="NM_000001.1",
            is_base_editor=False,
            score_set_id=seed[f"s_{visibility}"],
            **DATES,
        )
    seed.session.commit()
    return seed


def _add_public_target_gene(seed: Seed, **target_names: str) -> None:
    seed.add(
        "target_genes",
        name="g",
        category="protein_coding",
        scoreset_id=seed["s_public"],
        **DATES,
        **{column: seed[name] for column, name in target_names.items()},
    )
    seed.session.commit()


@pytest.fixture
def owned(targets):
    """Rows owned by the records, one under a visible parent and one under a hidden one where it applies."""
    seed = targets
    for visibility in ("public", "private"):
        variant = seed.add(
            "variants",
            f"v_{visibility}",
            data="{}",
            scoreset_id=seed[f"s_{visibility}"],
            urn=f"urn:mavedb:s-{visibility}#1",
            **DATES,
        )
        seed.add(
            "mapping_records",
            f"mr_{visibility}",
            variant_id=variant,
            score_set_id=seed[f"s_{visibility}"],
            assay_level="genomic",
            mapping_api_version="1",
            mapped_date="2026-01-01",
            valid_from="2026-01-01",
        )

    # Under a public calibration and a private one, both on the public score set.
    for name, calibration in (("public", "c_public_on_public"), ("hidden", "c_investigator_private_on_public")):
        seed.add(
            "calibration_controls",
            f"cc_{name}",
            calibration_id=seed[calibration],
            variant_id=seed["v_public"],
            clinical_status="benign",
            created_by_id=seed["owner"],
            modified_by_id=seed["owner"],
            **DATES,
        )
        seed.add(
            "score_calibration_functional_classifications",
            f"fc_{name}",
            calibration_id=seed[calibration],
            label="l",
            functional_classification="normal",
        )

    for collection, record in (("col_public", "public"), ("col_private", "public"), ("col_public", "private")):
        seed.add(
            "collection_score_sets",
            f"css_{collection}_{record}",
            collection_id=seed[collection],
            score_set_id=seed[f"s_{record}"],
            position=0,
        )
        seed.add(
            "collection_experiments",
            f"ce_{collection}_{record}",
            collection_id=seed[collection],
            experiment_id=seed[f"e_{record}"],
            position=0,
        )

    for kind in ("s", "e", "es"):
        for visibility in ("public", "private"):
            seed.add(
                "urn_redirects",
                f"ur_{kind}_{visibility}",
                old_urn=f"tmp:{kind}-{visibility}",
                new_urn=f"urn:mavedb:{kind}-{visibility}",
                created_at="2026-01-01",
            )

    pipeline = seed.add(
        "pipelines", "p_owner", name="p", status="created", metadata="{}", created_by_user_id=seed["owner"]
    )
    runs = {}
    for name, pipeline_id in (("jr_owner", pipeline), ("jr_ad_hoc", None)):
        runs[name] = seed.add(
            "job_runs",
            name,
            job_type="t",
            job_function="f",
            status="pending",
            max_retries=0,
            retry_count=0,
            pipeline_id=pipeline_id,
            metadata="{}",
        )
    seed.add(
        "job_dependencies",
        "jd_owner",
        id=runs["jr_owner"],
        depends_on_job_id=runs["jr_ad_hoc"],
        dependency_type="success_required",
    )
    seed.add(
        "job_dependencies",
        "jd_ad_hoc",
        id=runs["jr_ad_hoc"],
        depends_on_job_id=runs["jr_owner"],
        dependency_type="success_required",
    )
    seed.session.commit()
    return seed


OWNED_TABLES = (
    "variants",
    "target_sequences",
    "target_accessions",
    "mapping_records",
    "calibration_controls",
    "score_calibration_functional_classifications",
    "collection_score_sets",
    "collection_experiments",
    "urn_redirects",
    "pipelines",
    "job_runs",
    "job_dependencies",
)

OWNED_EXPECTED = {
    "anonymous": {
        "variants": {"v_public"},
        "target_sequences": {"ts_public"},
        "target_accessions": {"ta_public"},
        "mapping_records": {"mr_public"},
        "calibration_controls": {"cc_public"},
        "score_calibration_functional_classifications": {"fc_public"},
        "collection_score_sets": {"css_col_public_public"},
        "collection_experiments": {"ce_col_public_public"},
        "urn_redirects": {"ur_s_public", "ur_e_public", "ur_es_public"},
        "pipelines": set(),
        "job_runs": set(),
        "job_dependencies": set(),
    },
    "owner": {
        "variants": {"v_public", "v_private"},
        "target_sequences": {"ts_public", "ts_private"},
        "target_accessions": {"ta_public", "ta_private"},
        "mapping_records": {"mr_public", "mr_private"},
        "calibration_controls": {"cc_public", "cc_hidden"},
        "score_calibration_functional_classifications": {"fc_public", "fc_hidden"},
        "collection_score_sets": {"css_col_public_public", "css_col_private_public", "css_col_public_private"},
        "collection_experiments": {"ce_col_public_public", "ce_col_private_public", "ce_col_public_private"},
        "urn_redirects": {f"ur_{kind}_{v}" for kind in ("s", "e", "es") for v in ("public", "private")},
        "pipelines": {"p_owner"},
        "job_runs": {"jr_owner"},
        "job_dependencies": {"jd_owner"},
    },
    "admin": {
        "variants": {"v_public", "v_private"},
        "target_sequences": {"ts_public", "ts_private"},
        "target_accessions": {"ta_public", "ta_private"},
        "mapping_records": {"mr_public", "mr_private"},
        "calibration_controls": {"cc_public", "cc_hidden"},
        "score_calibration_functional_classifications": {"fc_public", "fc_hidden"},
        "collection_score_sets": {"css_col_public_public", "css_col_private_public", "css_col_public_private"},
        "collection_experiments": {"ce_col_public_public", "ce_col_private_public", "ce_col_public_private"},
        "urn_redirects": {f"ur_{kind}_{v}" for kind in ("s", "e", "es") for v in ("public", "private")},
        "pipelines": {"p_owner"},
        "job_runs": {"jr_owner", "jr_ad_hoc"},
        "job_dependencies": {"jd_owner", "jd_ad_hoc"},
    },
}


@pytest.fixture
def owned_enforcing(owned):
    """Row-level security on for the record tables and the tables they own."""
    owned.enable(*RECORD_TABLES, *OWNED_TABLES)


@pytest.mark.usefixtures("owned_enforcing")
@pytest.mark.parametrize("table", OWNED_TABLES)
@pytest.mark.parametrize("persona", OWNED_EXPECTED)
def test_owned_policy_row_visible_exactly_when_its_owner_is(owned, login_as, persona, table):
    with _as(login_as, owned, PERSONAS[persona]) as conn:
        assert _visible(conn, owned, table) == OWNED_EXPECTED[persona][table]


@pytest.mark.usefixtures("owned_enforcing")
@pytest.mark.parametrize("table", OWNED_TABLES)
def test_owned_policy_worker_sees_every_row(owned, login_as, table):
    with _as(login_as, owned, PERSONAS["anonymous"], WORKER_ROLE) as conn:
        assert _visible(conn, owned, table) == owned.rows[table]


@pytest.mark.usefixtures("owned_enforcing")
def test_owned_policy_insert_under_a_hidden_parent_is_refused(owned, login_as):
    with _as(login_as, owned, PERSONAS["stranger"]) as conn:
        with pytest.raises(ProgrammingError, match="row-level security"):
            conn.execute(
                text(
                    "INSERT INTO variants (data, scoreset_id, creation_date, modification_date) "
                    "VALUES ('{}', :s, now(), now())"
                ),
                {"s": owned["s_private"]},
            )


@pytest.mark.parametrize(
    "target_names",
    [{}, {"target_sequence_id": "ts_public", "accession_id": "ta_public"}],
    ids=["neither", "both"],
)
def test_target_gene_without_exactly_one_target_is_rejected(targets, target_names):
    with pytest.raises(IntegrityError, match="target_genes_one_target"):
        _add_public_target_gene(targets, **target_names)


@pytest.mark.parametrize(
    "column, name",
    [("target_sequence_id", "ts_private"), ("accession_id", "ta_private")],
    ids=["sequence", "accession"],
)
def test_target_gene_whose_target_carries_another_score_set_is_rejected(targets, column, name):
    with pytest.raises(IntegrityError, match="score_set_fkey"):
        _add_public_target_gene(targets, **{column: name})


@pytest.mark.parametrize("column, name", [("target_sequence_id", "ts_public"), ("accession_id", "ta_public")])
def test_target_gene_with_one_target_under_its_own_score_set_is_accepted(targets, column, name):
    _add_public_target_gene(targets, **{column: name})
