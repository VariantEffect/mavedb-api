# ruff: noqa: E402

"""Routes whose writes reach tables the api's grants treat specially, run as the api's own login (#827).

The rest of the suite connects as a superuser. Fixture helpers that stand in for the worker keep that session;
only the app's own database session logs in as ``mavedb_api``.
"""

import json

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.db.rls import API_ROLE
from mavedb.deps import get_db
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.legacy_keyword import LegacyKeyword
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.user import User
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_MINIMAL_ACC_SCORESET, TEST_USER
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set, create_seq_score_set_with_mapped_variants


@pytest.fixture
def api_role_client(app_, client, login_as):
    """The test client, with the app's database session logged in as the api role."""
    api_session = Session(login_as(API_ROLE))

    def override_get_db():
        try:
            yield api_session
        finally:
            api_session.close()

    def override_user():
        user = api_session.scalars(select(User).where(User.username == TEST_USER["username"])).one()
        yield UserData(user, user.roles)

    app_.dependency_overrides[get_db] = override_get_db
    app_.dependency_overrides[get_current_user] = override_user
    app_.dependency_overrides[require_current_user] = override_user
    yield client
    api_session.close()


def _add_legacy_keyword(session, record) -> None:
    keyword = LegacyKeyword(text=f"legacy-{record.urn}")
    record.legacy_keyword_objs = [keyword]
    session.commit()


def _add_target_gene_mapping(session, score_set_urn: str) -> None:
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()
    for target_gene in score_set.target_genes:
        session.add(
            TargetGeneMapping(target_gene_id=target_gene.id, alignment_level=SequenceLevel.cdna, tool_version="test")
        )
    session.commit()


@pytest.mark.integration
def test_delete_score_set_as_api_role_deletes_through_its_cascades(
    session, data_provider, api_role_client, setup_router_db, data_files
):
    experiment = create_experiment(api_role_client)
    score_set = create_seq_score_set_with_mapped_variants(
        api_role_client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _add_target_gene_mapping(session, score_set["urn"])
    db_score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])).one()
    variant_ids = [variant.id for variant in db_score_set.variants]
    session.add(
        AnnotationEvent(
            annotation_type=AnnotationType.VRS_MAPPING,
            variant_id=variant_ids[0],
            disposition=Disposition.PRESENT,
            reason="mapped",
        )
    )
    session.commit()
    _add_legacy_keyword(session, db_score_set)

    response = api_role_client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200
    session.expire_all()
    assert not session.scalars(select(Variant).where(Variant.id.in_(variant_ids))).first()
    assert not session.scalars(select(MappedVariant).where(MappedVariant.variant_id.in_(variant_ids))).first()
    assert not session.scalars(select(AnnotationEvent).where(AnnotationEvent.variant_id.in_(variant_ids))).first()


@pytest.mark.integration
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": "20711194"})],
    indirect=["mock_publication_fetch"],
)
def test_replace_score_set_targets_as_api_role_removes_old_targets(
    session, api_role_client, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(api_role_client)
    score_set = create_seq_score_set(api_role_client, experiment["urn"])
    _add_target_gene_mapping(session, score_set["urn"])
    old_target_ids = [
        target.id
        for target in session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"]))
        .one()
        .target_genes
    ]

    response = api_role_client.patch(
        f"/api/v1/score-sets-with-variants/{score_set['urn']}",
        data={"target_genes": json.dumps(TEST_MINIMAL_ACC_SCORESET["targetGenes"])},
    )

    assert response.status_code == 200
    session.expire_all()
    assert not session.scalars(select(TargetGene).where(TargetGene.id.in_(old_target_ids))).first()
    assert not session.scalars(
        select(TargetGeneMapping).where(TargetGeneMapping.target_gene_id.in_(old_target_ids))
    ).first()


@pytest.mark.integration
def test_read_and_delete_experiment_with_legacy_keywords_as_api_role(session, api_role_client, setup_router_db):
    experiment = create_experiment(api_role_client)
    db_experiment = session.scalars(select(ExperimentDbModel).where(ExperimentDbModel.urn == experiment["urn"])).one()
    _add_legacy_keyword(session, db_experiment)

    assert api_role_client.get(f"/api/v1/experiments/{experiment['urn']}").status_code == 200
    response = api_role_client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 200
    session.expire_all()
    assert not session.scalars(select(ExperimentDbModel).where(ExperimentDbModel.urn == experiment["urn"])).first()
