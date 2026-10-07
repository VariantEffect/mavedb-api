"""Test configuration and fixtures for tests/scripts."""

from datetime import date

import pytest
from sqlalchemy import select

from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.allele import Allele
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record
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

try:
    from .conftest_optional import *  # noqa: F401, F403

except ModuleNotFoundError:
    pass


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
def make_score_set(session, sample_experiment, sample_user, sample_license):
    """Factory for score sets with varying target genes and publication state.

    gene_names: names for sequence-based target genes. Passed through verbatim, so tests
        can exercise the curator-authored decorations the symbol extractor sees
        ("BRCA1 RING domain").
    mapped_hgnc_names: optional HGNC symbol per gene_names entry, as the mapper would
        populate it. None/omitted leaves the column NULL, which is what forces the
        extractor to fall back to the curator-authored name.
    accession_gene_names: names for accession-based target genes, which carry no
        target_sequence.
    """

    counter = {"n": 0}

    def _make(
        *,
        gene_names=("Sample Gene",),
        mapped_hgnc_names=None,
        accession_gene_names=(),
        published=False,
        num_variants=0,
    ):
        counter["n"] += 1
        target_genes: list[TargetGene] = []

        for i, name in enumerate(gene_names):
            target_sequence = TargetSequence(
                label=f"seq-{counter['n']}-{i}",
                sequence_type="dna",
                sequence="ATGCAT",
            )
            target_genes.append(
                TargetGene(
                    name=name,
                    category="protein_coding",
                    target_sequence=target_sequence,
                    mapped_hgnc_name=mapped_hgnc_names[i] if mapped_hgnc_names else None,
                )
            )

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
            num_variants=num_variants,
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
        mapped: give each variant a mapping — a live ``MappingRecord`` with an authoritative allele on
            the allele substrate, plus a row in the frozen ``MappedVariant`` table, since the dump still
            sources ``mapped-variants.json`` from the latter.
        current: whether those mappings are current. False models a fully superseded score set,
            which the dump must treat as having no annotations at all.
        placeable: whether the mapper placed each variant. False gives it a `MappingRecord` with no
            allele link at all, which is how an unplaceable variant is stored — an `Allele`'s identity
            is its post-mapped VRS id, so one cannot exist without a post-mapped representation. Do not
            reintroduce a "linked allele with a NULL `post_mapped`" shape; no write path produces it.
        published: publishes the score set — stamps `published_date`, which the dump's selection query
            requires, and clears `private`, which permission checks read. Publishing sets both.
        cc0: whether the score set carries the CC0 license the dump requires.
    """
    counter = {"n": 0}

    def _make(
        *,
        variant_scores=({"score": 1.0},),
        count_columns=(),
        mapped=True,
        current=True,
        placeable=True,
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
            # `ScoreSet.private` defaults to True, and publishing clears it alongside stamping
            # `published_date` (see the publish route). Setting only the date would build a score set that
            # is published *and* private — a state production never creates, and one that reads as
            # unreadable to any permission check while still satisfying the dump's selection query.
            private=not published,
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
                            "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X if placeable else None,
                        },
                        variant_id=variant.id,
                        clingen_allele_id=f"CA{counter['n']}{index}",
                    )
                )
                session.commit()

                # The annotation and CSV artifacts read the allele substrate, not the row above. An
                # unplaceable variant gets the record and no allele, matching the `if post_mapped_allele:`
                # guard in `worker/jobs/variant_processing/mapping.py`.
                record = seed_mapping_record(
                    session,
                    variant,
                    assay_level="genomic",
                    alleles=[
                        AlleleSpec(
                            digest=f"dump-allele-{counter['n']}-{index}",
                            level="genomic",
                            is_authoritative=True,
                            clingen_allele_id=f"CA{counter['n']}{index}",
                            post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
                        )
                    ]
                    if placeable
                    else [],
                )
                if not current:
                    # A fully superseded score set: the record is in history, but nothing is live.
                    record.retire(session)
                    session.commit()

        session.refresh(score_set)
        return score_set

    return _make


@pytest.fixture
def add_clinvar_control(session):
    """Attach a ClinVar clinical control to a variant's authoritative allele, for an `MM_YYYY` release."""

    def _add(variant, *, db_version, significance="Pathogenic", review_status="criteria provided"):
        """Link a control to *variant*'s authoritative allele, which is where the CSV layer reads it."""
        allele = session.scalars(
            select(Allele)
            .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
            .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
            .where(MappingRecord.variant_id == variant.id)
            .where(MappingRecordAllele.is_authoritative.is_(True))
        ).first()
        assert allele is not None, f"variant {variant.urn} has no authoritative allele to link a control to"

        control = ClinvarControl(
            db_identifier="183058",
            gene_symbol="PTEN",
            clinical_significance=significance,
            clinical_review_status=review_status,
            db_name="ClinVar",
            db_version=db_version,
        )
        session.add(control)
        session.commit()
        session.add(ClinvarAlleleLink(allele_id=allele.id, clinvar_control_id=control.id))
        session.commit()

    return _add
