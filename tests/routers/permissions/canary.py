"""Plants the canary: private records, each carrying a marker, plus the personas who must never see them (#810).

Public records carry markers too, which every persona must receive, so a rule that hides too much also fails.

Each persona is one of the shared persona clients from ``tests/conftest_optional.py``, signed in through the real
``get_current_user``. Records are written through the routes where a route can write them. States the
mapping pipeline produces (variants, mapping records, alleles, annotation links) are written directly, as the
variant and mapping jobs would leave them.
"""

import io
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

from humps import camelize
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
    csv_data_to_df,
)
from mavedb.lib.validation.dataframe.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.enums.user_role import UserRole
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_set_publication_identifier import (
    ScoreSetPublicationIdentifierAssociation,
)
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.score_set_dataset_columns import DatasetColumnsCreate
from tests.helpers.authentication import AuthenticatingClient
from tests.helpers.constants import (
    COLLECTION_MEMBER_USERS,
    TEST_FUNCTIONAL_RANGE_ABNORMAL,
    TEST_FUNCTIONAL_RANGE_NORMAL,
    TEST_MINIMAL_CALIBRATION,
    TEST_MINIMAL_SEQ_SCORESET,
)
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record, vrs_id
from tests.helpers.util.collection import create_collection
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import (
    create_test_score_calibration_in_score_set_via_client,
)
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set

ANONYMOUS = "anonymous"
"""Sends no credentials."""

OUTSIDER = "outsider"
"""Signed in, with no relation to anything planted."""

CALIBRATOR = "calibrator"
"""Created the community calibration on the owner's published score set, and holds nothing else."""

COLLECTION_VIEWER = "collection viewer"
"""A viewer of the private collection, and nothing else."""

COLLECTION_EDITOR = "collection editor"
"""An editor of the private collection, and nothing else."""

COLLECTION_ADMIN = "collection admin"
"""An admin of the private collection, and nothing else."""

CONTRIBUTOR = "contributor"
"""A contributor to the published score set only, so may read the calibration its owner provided."""

MAPPER = "mapper"
"""Acts with the mapper role, which reads private score sets but not their calibrations or collections."""

OWNER = "owner"
"""Created every planted score set, experiment and collection, but not the community calibration."""

ADMIN = "admin"
"""Acts with the admin role and may read everything, so it sets the ceiling for the positive control."""

PERSONAS = (
    ANONYMOUS,
    OUTSIDER,
    CALIBRATOR,
    COLLECTION_VIEWER,
    COLLECTION_EDITOR,
    COLLECTION_ADMIN,
    CONTRIBUTOR,
    MAPPER,
    OWNER,
)
"""Callers some planted record is hidden from. The admin sees everything, so it is the positive control's ceiling
rather than a persona the sweep checks."""

ROLE_PERSONAS: dict[Enum, str] = {
    UserRole.admin: ADMIN,
    UserRole.mapper: MAPPER,
    ContributionRole.viewer: COLLECTION_VIEWER,
    ContributionRole.editor: COLLECTION_EDITOR,
    ContributionRole.admin: COLLECTION_ADMIN,
}
"""The persona that acts with each role a user can hold. ``test_personas.py`` fails when a role has none."""

PUBLISHED = "published records"
PRIVATE_SCORE_SET = "private score set"
INVESTIGATOR_CALIBRATION = "investigator calibration"
COMMUNITY_CALIBRATION = "community calibration"
PRIVATE_COLLECTION = "private collection"
COLLECTION_ROSTER = "private collection's roster"

# The mapper role reads private score sets, experiments and experiment sets, but not calibrations or collections
# (#808 decision 9). Owning a score set doesn't entitle anyone to someone else's private calibration on it.
VISIBILITY: dict[str, frozenset[str]] = {
    PUBLISHED: frozenset(PERSONAS),
    PRIVATE_SCORE_SET: frozenset({MAPPER, OWNER}),
    INVESTIGATOR_CALIBRATION: frozenset({CONTRIBUTOR, OWNER}),
    COMMUNITY_CALIBRATION: frozenset({CALIBRATOR}),
    PRIVATE_COLLECTION: frozenset({COLLECTION_VIEWER, COLLECTION_EDITOR, COLLECTION_ADMIN, OWNER}),
    # Members who can't add users see only the collection's admins. The viewer is listed because the roster marker
    # is the viewer's own ORCID iD.
    COLLECTION_ROSTER: frozenset({COLLECTION_VIEWER, COLLECTION_ADMIN, OWNER}),
}
"""Who may see each kind of planted record, besides the admin. A private score set's kind covers what hangs off
it: its experiment and experiment set, variants, alleles, DOI, target, contributors and publication."""

CALIBRATOR_USER = {
    "username": "1010-1010-1010-1010",
    "first_name": "Calibrator",
    "last_name": "User",
    "email": "calibrator_user@test.com",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
    "is_first_login": True,
}
"""The user the ``CALIBRATOR`` persona signs in as."""

# Marker values carry 810 and are odd enough that no response contains one by chance.

SHARED_ALLELE_ID = vrs_id("canary810shared")
"""An allele a published variant and a private variant both map to, so it is public."""

SHARED_CAID = "CA810810"
"""The ClinGen allele ID of ``SHARED_ALLELE_ID``."""

PRIVATE_ALLELE_ID = vrs_id("canary810privateonly")
"""An allele only a private variant maps to."""

PRIVATE_CAID = "CA810811"
"""The ClinGen allele ID of ``PRIVATE_ALLELE_ID``."""

PUBLIC_SCORE = 8108.3081
"""The published score set's variant score."""

PRIVATE_SCORE = 8108.1081
"""A score in the private score set."""

PRIVATE_TARGET_NAME = "CANARY810"
"""The private score set's target gene name."""

PRIVATE_TARGET_SEQUENCE = "ACGTTTGATCCAGGTAAC"
"""The private score set's target sequence."""

# Upper case, because the API stores DOIs that way.
PRIVATE_DOI = "10.8108/CANARY.810"
"""The private score set's DOI."""

PRIVATE_CONTRIBUTOR_ORCID = "0000-0002-8108-1081"
"""The ORCID iD of a contributor to the private score set only."""

PRIVATE_PUBLICATION_IDENTIFIER = "81081081"
"""A PubMed ID cited only by the private score set."""

PRIVATE_GNOMAD_IDENTIFIER = "8-8108108-C-T"
"""A gnomAD variant linked only to ``PRIVATE_ALLELE_ID``."""

COMMUNITY_CALIBRATION_LABEL = "canary 810 community"
"""The start of each class label in the community calibration."""

COMMUNITY_CALIBRATION_BASELINE = 8108.2081
"""The community calibration's baseline score."""

INVESTIGATOR_CALIBRATION_LABEL = "canary 810 investigator"
"""The start of each class label in the calibration the score set's owner provided."""

COLLECTION_MEMBER_ORCID = COLLECTION_MEMBER_USERS[ContributionRole.viewer]["username"]
"""The ORCID iD of the private collection's viewer, who signs in as ``COLLECTION_VIEWER``."""


@dataclass(frozen=True)
class Marker:
    """A string that appears in a response only if the response carries the planted record it marks.

    ``kind`` is a key of ``VISIBILITY``, which says who may receive it. No other persona may get it from any route.
    """

    name: str
    value: str
    kind: str

    def hidden_from(self, persona: str) -> bool:
        """Whether ``persona`` must never receive this marker."""
        return persona != ADMIN and persona not in VISIBILITY[self.kind]


@dataclass
class Canary:
    """What was planted: each persona's client, the markers, and the identifiers routes are called with."""

    clients: dict[str, AuthenticatingClient]
    markers: list[Marker]
    private_score_set_urn: str
    private_experiment_urn: str
    private_experiment_set_urn: str
    private_variant_urns: list[str]
    private_target_gene_id: int
    public_score_set_urn: str
    public_experiment_urn: str
    public_experiment_set_urn: str
    public_variant_urns: list[str]
    community_calibration_urn: str
    community_classification_ids: list[int]
    investigator_calibration_urn: str
    investigator_classification_ids: list[int]
    collection_urn: str
    owner_orcid: str
    owner_id: int

    def hidden_markers(self, persona: str) -> list[Marker]:
        """The markers ``persona`` must never receive."""
        return [marker for marker in self.markers if marker.hidden_from(persona)]


def plant_canary(
    session: Session,
    data_provider: Any,
    *,
    owner: AuthenticatingClient,
    outsider: AuthenticatingClient,
    mapper: AuthenticatingClient,
    admin: AuthenticatingClient,
    anonymous: AuthenticatingClient,
    client_for: Callable[..., AuthenticatingClient],
    make_contributor: Callable[..., AuthenticatingClient],
    make_collection_member: Callable[..., AuthenticatingClient],
) -> Canary:
    """Plant one marked private record of each guarded kind, and give each persona its client.

    Args:
        session: The test database, holding the users and reference data ``setup_router_db`` writes.
        data_provider: The HGVS data provider variants are validated against.
        owner, outsider, mapper, admin, anonymous: The shared persona clients.
        client_for, make_contributor, make_collection_member: The shared fixtures that build the personas whose
            relation the canary creates.

    Returns:
        What was planted, for the sweep to call routes with and search responses for.
    """
    # A published score set, whose calibrations stay private when it is published.
    public_experiment = create_experiment(owner, {"title": "Canary public experiment"})
    public_score_set = create_seq_score_set(owner, public_experiment["urn"], {"title": "Canary public score set"})
    _insert_variants(
        session,
        data_provider,
        public_score_set["urn"],
        f"hgvs_nt,hgvs_pro,score\nc.1A>T,p.Thr1Ser,{PUBLIC_SCORE}\n",
    )
    contributor = make_contributor(public_score_set["urn"], ScoreSet)
    # Publishing replaces the tmp: URNs, so the variant URNs are read after it.
    public_score_set = publish_score_set(owner, public_score_set["urn"])
    public_variant_urns = _variant_urns(session, public_score_set["urn"])
    investigator_calibration = create_test_score_calibration_in_score_set_via_client(
        owner,
        public_score_set["urn"],
        _calibration_payload(INVESTIGATOR_CALIBRATION_LABEL),
    )

    # An unpublished score set, with its unpublished experiment and experiment set.
    private_experiment = create_experiment(owner, {"title": "Canary private experiment"})
    target_gene = deepcopy(TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0])
    target_gene["name"] = PRIVATE_TARGET_NAME
    target_gene["targetSequence"]["sequence"] = PRIVATE_TARGET_SEQUENCE
    private_score_set = create_seq_score_set(
        owner,
        private_experiment["urn"],
        {
            "title": "Canary private score set",
            "doiIdentifiers": [{"identifier": PRIVATE_DOI}],
            "targetGenes": [target_gene],
        },
    )
    private_variant_urns = _insert_variants(
        session,
        data_provider,
        private_score_set["urn"],
        f"hgvs_nt,hgvs_pro,score\nc.1A>T,p.Thr1Ser,{PRIVATE_SCORE}\nc.2C>T,p.Thr1Met,1.0\n",
    )
    # A contributor with no account, so only the private score set's responses can carry their ORCID iD.
    add_contributor(
        session,
        private_score_set["urn"],
        ScoreSet,
        PRIVATE_CONTRIBUTOR_ORCID,
        "Canary",
        "Contributor",
    )

    # A private collection holding the public score set, with a viewer.
    collection = create_collection(owner, {"name": "Canary private collection", "private": True})
    response = owner.post(
        f"/api/v1/collections/{collection['urn']}/score-sets",
        json={"scoreSetUrn": public_score_set["urn"]},
    )
    assert response.status_code == 200, response.text
    collection_viewer = make_collection_member(collection["urn"], ContributionRole.viewer)
    collection_editor = make_collection_member(collection["urn"], ContributionRole.editor)
    collection_admin = make_collection_member(collection["urn"], ContributionRole.admin)

    # A community calibration: on the owner's score set, by someone with no other relation to anything planted.
    calibrator = client_for(CALIBRATOR_USER)
    community_calibration = create_test_score_calibration_in_score_set_via_client(
        calibrator,
        public_score_set["urn"],
        _calibration_payload(COMMUNITY_CALIBRATION_LABEL, COMMUNITY_CALIBRATION_BASELINE),
    )
    assert community_calibration["investigatorProvided"] is False
    assert investigator_calibration["investigatorProvided"] is True

    _plant_alleles(session, public_variant_urns, private_variant_urns)
    _plant_publication(session, private_score_set["urn"])
    # Statistics read the materialized view, which the worker refreshes on a schedule.
    PublishedVariantsMV.refresh(session, concurrently=False)
    session.commit()

    private_target_gene_id = (
        session.scalars(select(ScoreSet).where(ScoreSet.urn == private_score_set["urn"])).one().target_genes[0].id
    )
    markers = [
        # Public records every persona must still receive, so a rule that hides too much fails the positive control.
        Marker("published score set URN", public_score_set["urn"], PUBLISHED),
        Marker("published experiment URN", public_score_set["experiment"]["urn"], PUBLISHED),
        Marker(
            "published experiment set URN",
            public_score_set["experiment"]["experimentSetUrn"],
            PUBLISHED,
        ),
        Marker("published variant score", repr(PUBLIC_SCORE), PUBLISHED),
        Marker("allele a published score set produces", SHARED_ALLELE_ID, PUBLISHED),
        Marker("CAID a published score set produces", SHARED_CAID, PUBLISHED),
        Marker("private score set URN", private_score_set["urn"], PRIVATE_SCORE_SET),
        Marker("private experiment URN", private_experiment["urn"], PRIVATE_SCORE_SET),
        Marker(
            "private experiment set URN",
            private_experiment["experimentSetUrn"],
            PRIVATE_SCORE_SET,
        ),
        Marker("private variant score", repr(PRIVATE_SCORE), PRIVATE_SCORE_SET),
        Marker("private target gene", PRIVATE_TARGET_NAME, PRIVATE_SCORE_SET),
        Marker("private target sequence", PRIVATE_TARGET_SEQUENCE, PRIVATE_SCORE_SET),
        Marker("private DOI", PRIVATE_DOI, PRIVATE_SCORE_SET),
        Marker("private score set's contributor", PRIVATE_CONTRIBUTOR_ORCID, PRIVATE_SCORE_SET),
        Marker("allele only a private score set produces", PRIVATE_ALLELE_ID, PRIVATE_SCORE_SET),
        Marker("CAID only a private score set produces", PRIVATE_CAID, PRIVATE_SCORE_SET),
        Marker(
            "gnomAD variant linked only to a private allele",
            PRIVATE_GNOMAD_IDENTIFIER,
            PRIVATE_SCORE_SET,
        ),
        Marker(
            "publication cited only by a private score set",
            PRIVATE_PUBLICATION_IDENTIFIER,
            PRIVATE_SCORE_SET,
        ),
        Marker(
            "community calibration URN",
            community_calibration["urn"],
            COMMUNITY_CALIBRATION,
        ),
        Marker(
            "community calibration classes",
            COMMUNITY_CALIBRATION_LABEL,
            COMMUNITY_CALIBRATION,
        ),
        Marker(
            "community calibration baseline",
            repr(COMMUNITY_CALIBRATION_BASELINE),
            COMMUNITY_CALIBRATION,
        ),
        Marker(
            "investigator calibration URN",
            investigator_calibration["urn"],
            INVESTIGATOR_CALIBRATION,
        ),
        Marker(
            "investigator calibration classes",
            INVESTIGATOR_CALIBRATION_LABEL,
            INVESTIGATOR_CALIBRATION,
        ),
        Marker("private collection URN", collection["urn"], PRIVATE_COLLECTION),
        Marker(
            "private collection's member",
            COLLECTION_MEMBER_ORCID,
            COLLECTION_ROSTER,
        ),
    ]

    return Canary(
        clients={
            ANONYMOUS: anonymous,
            OUTSIDER: outsider,
            CONTRIBUTOR: contributor,
            OWNER: owner,
            MAPPER: mapper,
            CALIBRATOR: calibrator,
            COLLECTION_VIEWER: collection_viewer,
            COLLECTION_EDITOR: collection_editor,
            COLLECTION_ADMIN: collection_admin,
            ADMIN: admin,
        },
        markers=markers,
        private_score_set_urn=private_score_set["urn"],
        private_experiment_urn=private_experiment["urn"],
        private_experiment_set_urn=private_experiment["experimentSetUrn"],
        private_variant_urns=private_variant_urns,
        private_target_gene_id=private_target_gene_id,
        public_score_set_urn=public_score_set["urn"],
        public_experiment_urn=public_score_set["experiment"]["urn"],
        public_experiment_set_urn=public_score_set["experiment"]["experimentSetUrn"],
        public_variant_urns=public_variant_urns,
        community_calibration_urn=community_calibration["urn"],
        community_classification_ids=[c["id"] for c in community_calibration["functionalClassifications"]],
        investigator_calibration_urn=investigator_calibration["urn"],
        investigator_classification_ids=[c["id"] for c in investigator_calibration["functionalClassifications"]],
        collection_urn=collection["urn"],
        owner_orcid=owner.username,
        owner_id=session.scalars(select(User.id).where(User.username == owner.username)).one(),
    )


def _variant_urns(session: Session, score_set_urn: str) -> list[str]:
    return list(
        session.scalars(select(Variant.urn).join(ScoreSet).where(ScoreSet.urn == score_set_urn).order_by(Variant.id))
    )


def _insert_variants(session: Session, data_provider, score_set_urn: str, scores_csv: str) -> list[str]:
    """Write a score set's variants as the variant-creation job leaves them."""
    item = session.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
    scores, counts, score_columns_metadata, count_columns_metadata = validate_and_standardize_dataframe_pair(
        csv_data_to_df(io.BytesIO(scores_csv.encode())),
        None,
        None,
        None,
        item.target_genes,
        data_provider,
    )
    create_variants(session, item, create_variants_data(scores, counts, None))
    item.processing_state = ProcessingState.success
    item.dataset_columns = DatasetColumnsCreate(
        score_columns=columns_for_dataset(scores),
        count_columns=columns_for_dataset(counts),
        score_columns_metadata=score_columns_metadata or {},
        count_columns_metadata=count_columns_metadata or {},
    ).model_dump()
    session.add(item)
    session.commit()
    return _variant_urns(session, score_set_urn)


def _calibration_payload(label: str, baseline: Optional[float] = None) -> dict:
    """A calibration request body, with ``label`` starting each class label and an optional baseline score."""
    payload = deepcopy(TEST_MINIMAL_CALIBRATION)
    payload["title"] = f"{label} calibration"
    payload["functional_classifications"] = [
        {**TEST_FUNCTIONAL_RANGE_NORMAL, "label": f"{label} normal"},
        {**TEST_FUNCTIONAL_RANGE_ABNORMAL, "label": f"{label} abnormal"},
    ]
    if baseline is not None:
        payload["baseline_score"] = baseline
    return camelize(payload)


def _post_mapped(allele_id: str) -> dict:
    """The VRS allele the mapper stores for an allele; only its shape matters here."""
    digest = allele_id.removeprefix("ga4gh:VA.")
    return {
        "id": allele_id,
        "type": "Allele",
        "state": {"type": "LiteralSequenceExpression", "sequence": "A"},
        "digest": digest,
        "location": {
            "id": f"ga4gh:SL.{digest}",
            "type": "SequenceLocation",
            "start": 1215,
            "end": 1216,
            "digest": digest,
            "sequenceReference": {
                "type": "SequenceReference",
                "label": "NM_000546.6",
                "refgetAccession": "SQ.0123456789abcdefghijABCDEFGHIJ_-",
            },
        },
    }


def _plant_alleles(session: Session, public_variant_urns: list[str], private_variant_urns: list[str]) -> None:
    """Map a public and a private variant to one shared allele, and another private variant to an allele of its own."""
    gnomad_variant = GnomADVariant(
        db_name="gnomAD",
        db_identifier=PRIVATE_GNOMAD_IDENTIFIER,
        db_version="v4.1",
        allele_count=1,
        allele_number=8108,
        allele_frequency=1 / 8108,
        faf95_max=0.0001,
        faf95_max_ancestry="nfe",
    )
    session.add(gnomad_variant)
    session.commit()

    shared = AlleleSpec(
        digest=SHARED_ALLELE_ID,
        is_authoritative=True,
        clingen_allele_id=SHARED_CAID,
        hgvs_c="NM_000546.6:c.1216G>A",
        post_mapped=_post_mapped(SHARED_ALLELE_ID),
    )
    seed_mapping_record(session, public_variant_urns[0], alleles=[shared])
    seed_mapping_record(session, private_variant_urns[0], alleles=[shared])
    seed_mapping_record(
        session,
        private_variant_urns[1],
        alleles=[
            AlleleSpec(
                digest=PRIVATE_ALLELE_ID,
                is_authoritative=True,
                clingen_allele_id=PRIVATE_CAID,
                hgvs_c="NM_000546.6:c.1217G>A",
                post_mapped=_post_mapped(PRIVATE_ALLELE_ID),
                gnomad_variant_ids=[gnomad_variant.id],
            )
        ],
    )


def _plant_publication(session: Session, score_set_urn: str) -> None:
    session.add(
        PublicationIdentifier(
            identifier=PRIVATE_PUBLICATION_IDENTIFIER,
            db_name="PubMed",
            title="Canary publication",
            authors=[],
            abstract="Cited only by a private score set.",
            publication_year=2026,
            publication_journal="Canary",
            url=f"http://www.ncbi.nlm.nih.gov/pubmed/{PRIVATE_PUBLICATION_IDENTIFIER}",
        )
    )
    session.flush()
    score_set = session.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
    session.add(
        ScoreSetPublicationIdentifierAssociation(
            score_set_id=score_set.id,
            db_name="PubMed",
            identifier=PRIVATE_PUBLICATION_IDENTIFIER,
            primary=True,
        )
    )
    session.commit()
