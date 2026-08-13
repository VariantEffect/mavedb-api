"""Test configuration and fixtures for tests/scripts."""

from datetime import date

import pytest

from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_calibrations import variants_for_functional_classification
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.enums.functional_classification import FunctionalClassification
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_ACMG_BS3_STRONG_CLASSIFICATION,
    TEST_ACMG_PS3_STRONG_CLASSIFICATION,
    TEST_LICENSE,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
)


@pytest.fixture
def sample_user(session):
    user = User(**TEST_USER)
    session.add(user)
    session.commit()
    return user


@pytest.fixture
def sample_extra_user(session):
    user = User(**EXTRA_USER)
    session.add(user)
    session.commit()
    return user


@pytest.fixture
def sample_license(session):
    license_ = License(**TEST_LICENSE)
    session.add(license_)
    session.commit()
    return license_


@pytest.fixture
def sample_experiment_set(session, sample_user):
    experiment_set = ExperimentSet(extra_metadata={}, created_by=sample_user)
    session.add(experiment_set)
    session.commit()
    return experiment_set


@pytest.fixture
def sample_experiment(session, sample_experiment_set, sample_user):
    experiment = Experiment(
        title="Sample Experiment",
        short_description="A sample experiment for testing purposes",
        abstract_text="This is an abstract for the sample experiment.",
        method_text="This is a method description for the sample experiment.",
        extra_metadata={},
        experiment_set=sample_experiment_set,
        created_by=sample_user,
        # Required by the public-dump view models, which validate the whole metadata graph.
        modified_by=sample_user,
    )
    session.add(experiment)
    session.commit()
    return experiment


@pytest.fixture
def sample_score_set(sample_experiment, sample_user, sample_license, session):
    score_set = ScoreSet(
        title="Sample Score Set",
        short_description="A sample score set for testing purposes",
        abstract_text="This is an abstract for the sample score set.",
        method_text="This is a method description for the sample score set.",
        extra_metadata={},
        experiment=sample_experiment,
        created_by=sample_user,
        license=sample_license,
        target_genes=[
            TargetGene(
                name="Sample Gene",
                category="protein_coding",
                target_sequence=TargetSequence(label="testsequence", sequence_type="dna", sequence="ATGCAT"),
            )
        ],
    )
    session.add(score_set)
    session.commit()
    return score_set


@pytest.fixture
def make_taxonomy(session):
    """Factory for Taxonomy rows distinguished by code/organism_name."""
    counter = {"n": TEST_SAVED_TAXONOMY["id"]}

    def _make(*, code=None, organism_name=None):
        counter["n"] += 1
        taxonomy = Taxonomy(
            **{
                **TEST_SAVED_TAXONOMY,
                "id": counter["n"],
                "code": code if code is not None else TEST_SAVED_TAXONOMY["code"],
                "organism_name": organism_name or TEST_SAVED_TAXONOMY["organism_name"],
                "url": f"https://example.test/taxonomy/{counter['n']}",
            }
        )
        session.add(taxonomy)
        session.commit()
        return taxonomy

    return _make


@pytest.fixture
def make_score_set(session, sample_experiment, sample_user, sample_license):
    """Factory for score sets with varying target genes / taxonomy / publication state.

    gene_names: names for sequence-based target genes (normalized for grouping/ordering tests).
    taxonomies: optional list of Taxonomy rows, one per gene_names entry (or a single Taxonomy
        applied to all genes). Omit for genes with no taxonomy at all.
    accession_gene_names: names for accession-based target genes (never matched by
        --taxonomy-id/--organism, since they carry no target_sequence).
    """

    counter = {"n": 0}

    def _make(
        *,
        gene_names=("Sample Gene",),
        taxonomies=None,
        accession_gene_names=(),
        published=False,
    ):
        counter["n"] += 1
        target_genes: list[TargetGene] = []

        if taxonomies is not None and not isinstance(taxonomies, (list, tuple)):
            taxonomies = [taxonomies] * len(gene_names)

        for i, name in enumerate(gene_names):
            taxonomy = taxonomies[i] if taxonomies else None
            target_sequence = TargetSequence(
                label=f"seq-{counter['n']}-{i}",
                sequence_type="dna",
                sequence="ATGCAT",
                taxonomy=taxonomy,
            )
            target_genes.append(TargetGene(name=name, category="protein_coding", target_sequence=target_sequence))

        for name in accession_gene_names:
            target_genes.append(
                TargetGene(
                    name=name,
                    category="protein_coding",
                    target_accession=TargetAccession(accession=f"NM_{counter['n']}.1"),
                )
            )

        score_set = ScoreSet(
            title=f"Sample Score Set {counter['n']}",
            short_description="A sample score set for testing purposes",
            abstract_text="Abstract",
            method_text="Method",
            extra_metadata={},
            experiment=sample_experiment,
            created_by=sample_user,
            license=sample_license,
            published_date=date(2024, 1, 1) if published else None,
            target_genes=target_genes,
        )
        session.add(score_set)
        session.commit()
        return score_set

    return _make


@pytest.fixture
def make_collection(session, sample_user):
    """Factory for a Collection containing the given score sets, in order."""
    counter = {"n": 0}

    def _make(*, score_sets=()):
        counter["n"] += 1
        collection = Collection(name=f"Collection {counter['n']}", private=False, created_by=sample_user)
        session.add(collection)
        session.flush()
        for position, score_set in enumerate(score_sets):
            session.add(
                CollectionScoreSetAssociation(collection_id=collection.id, score_set_id=score_set.id, position=position)
            )
        session.commit()
        return collection

    return _make


# ---------------------------------------------------------------------------
# Public data dump
#
# The dump selects on published + CC0, so these fixtures build that shape explicitly rather than reusing
# the generic score-set factories above, whose license is deliberately not CC0.
# ---------------------------------------------------------------------------

CC0_LICENSE_ID = 900
OTHER_LICENSE_ID = 901


@pytest.fixture
def anonymous_principal():
    """The principal the dump is built for.

    The archive has no requesting user, so every artifact is composed for the public. Constructing this
    explicitly in tests keeps that from being an accident of a default argument.
    """
    return Principal()


@pytest.fixture
def anonymous_viewer(anonymous_principal):
    """The calibration viewer the dump's artifacts are scoped to."""
    return anonymous_principal.viewer_for(ScoreCalibrationViewer)


@pytest.fixture
def dump_acmg_classifications(session):
    """The PS3/BS3 rows a calibration's functional ranges point at."""
    session.add(ACMGClassification(**TEST_ACMG_PS3_STRONG_CLASSIFICATION))
    session.add(ACMGClassification(**TEST_ACMG_BS3_STRONG_CLASSIFICATION))
    session.commit()


@pytest.fixture
def dump_licenses(session):
    """A CC0 license and a non-CC0 one, so exclusion by license can be exercised."""
    session.add(License(**{**TEST_LICENSE, "id": CC0_LICENSE_ID, "short_name": "CC0", "long_name": "CC0 1.0"}))
    session.add(License(**{**TEST_LICENSE, "id": OTHER_LICENSE_ID, "short_name": "CC-BY", "long_name": "CC BY 4.0"}))
    session.commit()


@pytest.fixture
def dump_experiment(session, sample_user):
    """A published experiment under a published experiment set.

    Separate from `sample_experiment` because the dump's selection query requires a published date at
    every level of the hierarchy, and publishing the shared fixture would change what the other script
    tests see.
    """
    experiment_set = ExperimentSet(
        extra_metadata={},
        created_by=sample_user,
        modified_by=sample_user,
        published_date=date(2024, 1, 1),
        urn="urn:mavedb:00000001",
    )
    session.add(experiment_set)
    session.commit()

    experiment = Experiment(
        title="Dump Experiment",
        short_description="An experiment for public dump tests",
        abstract_text="Abstract",
        method_text="Method",
        extra_metadata={},
        experiment_set=experiment_set,
        created_by=sample_user,
        modified_by=sample_user,
        published_date=date(2024, 1, 1),
        urn="urn:mavedb:00000001-a",
    )
    session.add(experiment)
    session.commit()
    return experiment


@pytest.fixture
def dump_taxonomy(session):
    """A taxonomy for the dump's target sequences, which the public-dump view models require."""
    taxonomy = Taxonomy(**TEST_SAVED_TAXONOMY)
    session.add(taxonomy)
    session.commit()
    return taxonomy


@pytest.fixture
def make_dump_score_set(session, sample_user, dump_experiment, dump_licenses, dump_taxonomy):
    """Factory for the score-set shapes the dump distinguishes between.

    Args:
        variant_scores: one score_data dict per variant. Keys become the score columns.
        count_columns: count column names; each variant gets a count_data value for every one.
        mapped: attach a mapped variant to each variant.
        current: whether those mappings are current. False models a fully superseded score set,
            which the dump must treat as having no annotations at all.
        post_mapped: whether each mapping carries a post-mapped VRS allele. False leaves it NULL, which
            is how a variant the mapper could not place is stored, and which the README documents as
            yielding a null `annotation`. Note that the shared `TEST_MINIMAL_MAPPED_VARIANT` uses an
            empty dict here, a shape production never stores and the annotation layer cannot parse.
        published: sets published_date, which the dump's selection query requires.
        cc0: whether the score set carries the CC0 license the dump requires.
    """
    counter = {"n": 0}

    def _make(
        *,
        variant_scores=({"score": 1.0},),
        count_columns=(),
        mapped=True,
        current=True,
        post_mapped=False,
        published=True,
        cc0=True,
    ):
        counter["n"] += 1
        urn = f"urn:mavedb:{counter['n']:08d}-a-1"

        score_columns = list(variant_scores[0].keys()) if variant_scores else ["score"]
        score_set = ScoreSet(
            title=f"Dump Score Set {counter['n']}",
            short_description="A score set for public dump tests",
            abstract_text="Abstract",
            method_text="Method",
            extra_metadata={},
            urn=urn,
            experiment=dump_experiment,
            created_by=sample_user,
            modified_by=sample_user,
            licence_id=CC0_LICENSE_ID if cc0 else OTHER_LICENSE_ID,
            published_date=date(2024, 1, 1) if published else None,
            dataset_columns={"score_columns": score_columns, "count_columns": list(count_columns)},
            target_genes=[
                TargetGene(
                    name="Dump Gene",
                    category="protein_coding",
                    target_sequence=TargetSequence(
                        label=f"dumpseq-{counter['n']}",
                        sequence_type="dna",
                        sequence="ATGCAT",
                        taxonomy=dump_taxonomy,
                    ),
                )
            ],
        )
        session.add(score_set)
        session.commit()
        session.refresh(score_set)

        for index, score_data in enumerate(variant_scores, start=1):
            variant = Variant(
                urn=f"{urn}#{index}",
                score_set_id=score_set.id,
                hgvs_nt=f"c.{index}A>G",
                data={
                    "score_data": dict(score_data),
                    "count_data": {column: index for column in count_columns},
                },
            )
            session.add(variant)
            session.commit()
            session.refresh(variant)

            if mapped:
                session.add(
                    MappedVariant(
                        **{
                            **TEST_MINIMAL_MAPPED_VARIANT,
                            "current": current,
                            "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X if post_mapped else None,
                        },
                        variant_id=variant.id,
                        clingen_allele_id=f"CA{counter['n']}{index}",
                    )
                )
                session.commit()

        session.refresh(score_set)
        return score_set

    return _make


@pytest.fixture
def make_dump_calibration(session, sample_user, dump_acmg_classifications):
    """Factory for a calibration with an abnormal (PS3) range below -1.0 and a normal (BS3) range above 1.0.

    `private` and `research_use_only` are the two axes the dump treats differently: the first decides
    whether an anonymous viewer may read it at all, the second only how it is labeled.

    Range membership is materialized from each variant's score by the same helper the creation endpoint
    uses, so a score of -2.0 lands in the abnormal range and one of 3.0 in the normal range. Membership is
    stored as an association rather than recomputed at read time, so a fixture that sets the bounds
    without populating it would classify every variant as `indeterminate` no matter its score, and every
    assertion about a calibration column would silently hold in that one degenerate state.

    Call this only after the score set's variants exist; a variant added later is not classified.
    """
    counter = {"n": 0}

    def _make(score_set, *, private=False, research_use_only=False, title=None):
        counter["n"] += 1
        urn = f"urn:mavedb:calibration-{counter['n']:08d}-0000-0000-0000-000000000000"

        calibration = ScoreCalibration(
            score_set_id=score_set.id,
            urn=urn,
            title=title or f"Dump Calibration {counter['n']}",
            baseline_score=0.0,
            research_use_only=research_use_only,
            # The view models reject a primary calibration that is private or research-use-only, so
            # `primary` is derived rather than exposed: a fixture that set it independently could build a
            # score set the public-dump view models refuse to validate.
            primary=not (private or research_use_only),
            private=private,
            calibration_metadata={},
            created_by_id=sample_user.id,
            modified_by_id=sample_user.id,
        )
        session.add(calibration)
        session.commit()
        session.refresh(calibration)

        abnormal = session.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.PS3).first()
        normal = session.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.BS3).first()

        ranges = [
            ScoreCalibrationFunctionalClassification(
                calibration=calibration,
                label="abnormal range",
                description="An abnormal functional range",
                functional_classification=FunctionalClassification.abnormal,
                range=[-5.0, -1.0],
                inclusive_lower_bound=True,
                inclusive_upper_bound=False,
                acmg_classification_id=abnormal.id,
            ),
            ScoreCalibrationFunctionalClassification(
                calibration=calibration,
                label="normal range",
                description="A normal functional range",
                functional_classification=FunctionalClassification.normal,
                range=[1.0, 5.0],
                inclusive_lower_bound=True,
                inclusive_upper_bound=False,
                acmg_classification_id=normal.id,
            ),
        ]
        for functional_range in ranges:
            session.add(functional_range)
        session.commit()

        for functional_range in ranges:
            functional_range.variants = variants_for_functional_classification(session, functional_range, use_sql=True)
        session.commit()
        session.refresh(calibration)
        return calibration

    return _make


@pytest.fixture
def add_clinvar_control(session):
    """Attach a ClinVar clinical control to a mapped variant, for a given `MM_YYYY` release."""

    def _add(mapped_variant, *, db_version, significance="Pathogenic", review_status="criteria provided"):
        mapped_variant.clinical_controls.append(
            ClinicalControl(
                db_identifier="183058",
                gene_symbol="PTEN",
                clinical_significance=significance,
                clinical_review_status=review_status,
                db_name="ClinVar",
                db_version=db_version,
            )
        )
        session.add(mapped_variant)
        session.commit()

    return _add
