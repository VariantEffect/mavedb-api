"""Test configuration and fixtures for tests/scripts."""

from datetime import date

import pytest

from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from tests.helpers.constants import EXTRA_USER, TEST_LICENSE, TEST_SAVED_TAXONOMY, TEST_USER


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
