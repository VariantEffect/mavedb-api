from typing import BinaryIO, Optional

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased, contains_eager, joinedload, selectinload, Session

from mavedb.lib.exceptions import ValidationError
from mavedb.lib.mave.constants import (
    HGVS_NT_COLUMN,
    HGVS_PRO_COLUMN,
    HGVS_SPLICE_COLUMN,
    VARIANT_COUNT_DATA,
    VARIANT_SCORE_DATA,
)
from mavedb.lib.validation.constants.general import null_values_list
from mavedb.lib.mave.utils import is_csv_null
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.keyword import Keyword
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.uniprot_offset import UniprotOffset
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.search import ScoreSetsSearch

VariantData = dict[str, Optional[dict[str, dict]]]


class HGVSColumns:
    NUCLEOTIDE: str = "hgvs_nt"  # dataset.constants.hgvs_nt_column
    TRANSCRIPT: str = "hgvs_splice"  # dataset.constants.hgvs_splice_column
    PROTEIN: str = "hgvs_pro"  # dataset.constants.hgvs_pro_column

    @classmethod
    def options(cls) -> list[str]:
        return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]


def search_score_sets(db: Session, owner: Optional[User], search: ScoreSetsSearch) -> list[ScoreSet]:
    query = db.query(ScoreSet)  # \
    # .filter(ScoreSet.private.is_(False))

    if owner is not None:
        query = query.filter(ScoreSet.created_by_id == owner.id)

    if search.published is not None:
        if search.published:
            query = query.filter(ScoreSet.published_date.isnot(None))
        else:
            query = query.filter(ScoreSet.published_date.is_(None))

    if search.text:
        lower_search_text = search.text.lower()
        query = query.filter(
            or_(
                ScoreSet.urn.icontains(lower_search_text),
                ScoreSet.title.icontains(lower_search_text),
                ScoreSet.short_description.icontains(lower_search_text),
                ScoreSet.abstract_text.icontains(lower_search_text),
                ScoreSet.target_genes.any(func.lower(TargetGene.name).icontains(lower_search_text)),
                ScoreSet.target_genes.any(func.lower(TargetGene.category).icontains(lower_search_text)),
                ScoreSet.keyword_objs.any(func.lower(Keyword.text).icontains(lower_search_text)),
                ScoreSet.target_genes.any(
                    TargetGene.target_sequence.has(
                        TargetSequence.reference.has(
                            func.lower(ReferenceGenome.organism_name).icontains(lower_search_text)
                        )
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.target_sequence.has(
                        TargetSequence.reference.has(func.lower(ReferenceGenome.short_name).contains(lower_search_text))
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.target_accession.has(func.lower(TargetAccession.assembly).contains(lower_search_text))
                ),
                # TODO(#94): add LICENSE, plus TAX_ID if numeric
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.identifier).icontains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.abstract).icontains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.title).icontains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.publication_journal).icontains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.jsonb_path_exists(
                        PublicationIdentifier.authors, f"""$[*].name ? (@ like_regex "{lower_search_text}" flag "i")"""
                    )
                ),
                ScoreSet.doi_identifiers.any(func.lower(DoiIdentifier.identifier).contains(lower_search_text)),
                ScoreSet.target_genes.any(
                    TargetGene.uniprot_offset.has(
                        UniprotOffset.identifier.has(
                            func.lower(UniprotIdentifier.identifier).contains(lower_search_text)
                        )
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.refseq_offset.has(
                        RefseqOffset.identifier.has(func.lower(RefseqIdentifier.identifier).contains(lower_search_text))
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.ensembl_offset.has(
                        EnsemblOffset.identifier.has(
                            func.lower(EnsemblIdentifier.identifier).contains(lower_search_text)
                        )
                    )
                ),
            )
        )

    if search.targets:
        query = query.filter(ScoreSet.target_genes.any(TargetGene.name.in_(search.targets)))

    if search.target_organism_names:
        query = query.filter(
            ScoreSet.target_genes.any(
                TargetGene.target_sequence.has(
                    TargetSequence.reference.has(ReferenceGenome.organism_name.in_(search.target_organism_names))
                )
            )
        )

    if search.target_types:
        query = query.filter(ScoreSet.target_genes.any(TargetGene.category.in_(search.target_types)))

    if search.publication_identifiers:
        query = query.filter(
            ScoreSet.publication_identifiers.any(PublicationIdentifier.identifier.in_(search.publication_identifiers))
        )

    if search.databases:
        query = query.filter(ScoreSet.publication_identifiers.any(PublicationIdentifier.db_name.in_(search.databases)))

    if search.journals:
        query = query.filter(
            ScoreSet.publication_identifiers.any(PublicationIdentifier.publication_journal.in_(search.journals))
        )

    if search.authors:
        query = query.filter(
            ScoreSet.publication_identifiers.any(
                func.jsonb_path_query_array(PublicationIdentifier.authors, "$.name").op("?|")(search.authors)
            )
        )

    if search.target_accessions:
        query = query.filter(
            ScoreSet.target_genes.any(
                TargetGene.target_accession.has(TargetAccession.accession.in_(search.target_accessions))
            )
        )

    score_sets: list[ScoreSet] = (
        query.join(ScoreSet.experiment)
        .options(
            contains_eager(ScoreSet.experiment).options(
                joinedload(Experiment.experiment_set),
                joinedload(Experiment.keyword_objs),
                joinedload(Experiment.created_by),
                joinedload(Experiment.modified_by),
                joinedload(Experiment.keyword_objs),
                joinedload(Experiment.doi_identifiers),
                joinedload(Experiment.publication_identifier_associations).joinedload(
                    ExperimentPublicationIdentifierAssociation.publication
                ),
                joinedload(Experiment.raw_read_identifiers),
                selectinload(Experiment.score_sets).options(
                    joinedload(ScoreSet.keyword_objs),
                    joinedload(ScoreSet.doi_identifiers),
                    joinedload(ScoreSet.publication_identifier_associations).joinedload(
                        ScoreSetPublicationIdentifierAssociation.publication
                    ),
                    joinedload(ScoreSet.target_genes).options(
                        joinedload(TargetGene.ensembl_offset).joinedload(EnsemblOffset.identifier),
                        joinedload(TargetGene.refseq_offset).joinedload(RefseqOffset.identifier),
                        joinedload(TargetGene.uniprot_offset).joinedload(UniprotOffset.identifier),
                        joinedload(TargetGene.target_sequence).joinedload(TargetSequence.reference),
                        joinedload(TargetGene.target_accession),
                    ),
                ),
            ),
            joinedload(ScoreSet.keyword_objs),
            joinedload(ScoreSet.license),
            joinedload(ScoreSet.doi_identifiers),
            joinedload(ScoreSet.publication_identifier_associations).joinedload(
                ScoreSetPublicationIdentifierAssociation.publication
            ),
            joinedload(ScoreSet.target_genes).options(
                joinedload(TargetGene.ensembl_offset).joinedload(EnsemblOffset.identifier),
                joinedload(TargetGene.refseq_offset).joinedload(RefseqOffset.identifier),
                joinedload(TargetGene.uniprot_offset).joinedload(UniprotOffset.identifier),
                joinedload(TargetGene.target_sequence).joinedload(TargetSequence.reference),
                joinedload(TargetGene.target_accession),
            ),
        )
        .order_by(Experiment.title)
        .all()
    )
    if not score_sets:
        score_sets = []
    return score_sets  # filter_visible_score_sets(score_sets)


def find_meta_analyses_for_experiment_sets(db: Session, urns: list[str]) -> list[ScoreSet]:
    """
    Find all score sets that are meta-analyses for score sets from a specified collection of experiment sets.

    :param db: An active database session.
    :param urns: A list of experiment set URNS.
    :return: A score set that is a meta-analysis for score sets belonging to exactly the collection of experiment sets
      specified by urns; or None if there is no such meta-analysis.
    """
    # Ensure that URNs are not repeated in the list.
    urns = list(set(urns))

    # Find all score sets that are meta-analyses for a superset of the specified URNs and are meta-analyses for
    # exactly len(urns) score sets.
    score_set_aliases = [aliased(ScoreSet) for _ in urns]
    experiment_aliases = [aliased(Experiment) for _ in urns]
    experiment_set_aliases = [aliased(ExperimentSet) for _ in urns]
    analyzed_score_set = aliased(ScoreSet)
    analyzed_experiment = aliased(Experiment)
    analyzed_experiment_set = aliased(ExperimentSet)
    urn_filters = [
        ScoreSet.meta_analyzes_score_sets.of_type(score_set_aliases[i]).any(
            score_set_aliases[i]
            .experiment.of_type(experiment_aliases[i])
            .has(
                experiment_aliases[i]
                .experiment_set.of_type(experiment_set_aliases[i])
                .has(experiment_set_aliases[i].urn == urn)
            )
        )
        for i, urn in enumerate(urns)
    ]
    return (
        db.query(ScoreSet)
        .join(ScoreSet.meta_analyzes_score_sets.of_type(analyzed_score_set))
        .join(analyzed_score_set.experiment.of_type(analyzed_experiment))
        .join(analyzed_experiment.experiment_set.of_type(analyzed_experiment_set))
        .filter(*urn_filters)
        .group_by(ScoreSet.id)
        .having(func.count(func.distinct(analyzed_experiment_set.id)) == len(urns))
        .all()
    )


def find_meta_analyses_for_score_sets(db: Session, urns: list[str]) -> list[ScoreSet]:
    """
    Find all score sets that are meta-analyses for a specified collection of other score sets.

    :param db: An active database session.
    :param urns: A list of score set URNS.
    :return: A score set that is a meta-analysis for exactly the collection of score sets specified by urns; or None if
      there is no such meta-analysis.
    """
    # Ensure that URNs are not repeated in the list.
    urns = list(set(urns))

    # Find all score sets that are meta-analyses for a superset of the specified URNs and are meta-analyses for
    # exactly len(urns) score sets.
    score_set_aliases = [aliased(ScoreSet) for _ in urns]
    analyzed_score_set = aliased(ScoreSet)
    urn_filters = [
        ScoreSet.meta_analyzes_score_sets.of_type(score_set_aliases[i]).any(score_set_aliases[i].urn == urn)
        for i, urn in enumerate(urns)
    ]
    return (
        db.query(ScoreSet)
        .join(ScoreSet.meta_analyzes_score_sets.of_type(analyzed_score_set))
        .filter(*urn_filters)
        .group_by(ScoreSet.id)
        .having(func.count(analyzed_score_set.id) == len(urns))
        .all()
    )


def filter_visible_score_sets(items: list[ScoreSet]):
    # TODO Take the user into account.
    return filter(lambda item: not item.private, items or [])


def arrays_equal(array1: np.ndarray, array2: np.ndarray):
    # if the shape isn't the same the arrays are different.
    # otherwise for each value make sure either both values are null
    # or the values are equal.
    return array1.shape == array2.shape and all(
        # note that each of the three expressions here is a boolean ndarray
        # so combining them with bitwise `&` and `|` works:
        (pd.isnull(array1) & pd.isnull(array2))
        | (array1 == array2)
    )


def validate_datasets_define_same_variants(scores, counts):
    """
    Checks if two `pd.DataFrame` objects parsed from uploaded files
    define the same variants.

    Parameters
    ----------
    scores : `pd.DataFrame`
        Scores dataframe parsed from an uploaded scores file.
    counts : `pd.DataFrame`
        Scores dataframe parsed from an uploaded counts file.
    """
    # TODO First, confirm that the two dataframes have the same HGVS columns.
    if any(
        col in scores and not arrays_equal(scores[col].sort_values(), counts[col].sort_values())
        for col in (HGVS_NT_COLUMN, HGVS_SPLICE_COLUMN, HGVS_PRO_COLUMN)
    ):
        raise ValidationError(
            "Your score and counts files do not define the same variants. "
            "Check that the hgvs columns in both files match."
        )


def create_variants_data(scores, counts=None, index_col=None) -> list[VariantData]:
    """
    Given two `defaultdict`s `score_map` and `count_map`, create an
    `OrderedDict` indexed by `hgvs_nt` where the keys are the attribute
    fields required to instantiate a `variant.models.Variant` instance.

    NOTE: Assumes that the dataframes are indexed by their primary columns,
    and that they define the same variants in both hgvs columns.

    Parameters
    ----------
    scores : Union[`pd.DataFrame`, str]
        Map indexed by the primary hgvs column inferred during validation.
        Map values are `dict` records where the key-pairs are column-value
        pairs inferred from the `scores` file uploaded during submission.
    counts : Union[`pd.DataFrame`, str] optional
        Map indexed by the primary hgvs column inferred during validation.
        Map values are `dict` records where the key-pairs are column-value
        pairs inferred from the `counts` file uploaded during submission.
    index_col : str
        Column to use as index, which is used when grouping rows between
        dataframes.

    Returns
    -------
    `list`
        Formatted records that can be used to create `variant.models.Variant`
        instances.
    """

    if isinstance(scores, str):
        scores = pd.read_json(scores, orient="records")
    if isinstance(counts, str):
        counts = pd.read_json(counts, orient="records")

    has_count_data = counts is not None and len(counts) > 0
    has_score_data = scores is not None and len(scores) > 0

    if index_col:
        scores.index = pd.Index(scores[index_col])
        if has_count_data:
            counts.index = pd.Index(counts[index_col])

    if not has_score_data:
        return []

    if has_count_data:
        assert_index_equal(scores.index.sort_values(), counts.index.sort_values())
        validate_datasets_define_same_variants(scores, counts)

    variants = []
    for primary_hgvs, group in scores.groupby(by=scores.index, sort=False):
        score_records = group.to_dict(orient="records")
        if has_count_data:
            count_records = counts[counts.index == primary_hgvs].to_dict(orient="records")
            assert len(score_records) == len(count_records)
        else:
            # Make duplicates to zip with self when no count data.
            count_records = [r.copy() for r in score_records]

        for sr, cr in zip(score_records, count_records):
            hgvs_nt = sr.pop(HGVS_NT_COLUMN)
            hgvs_splice = sr.pop(HGVS_SPLICE_COLUMN)
            hgvs_pro = sr.pop(HGVS_PRO_COLUMN)

            if is_csv_null(hgvs_nt) or hgvs_nt is np.NaN or hgvs_nt == "nan":
                hgvs_nt = None
            if is_csv_null(hgvs_splice) or hgvs_splice is np.NaN or hgvs_splice == "nan":
                hgvs_splice = None
            if is_csv_null(hgvs_pro) or hgvs_pro is np.NaN or hgvs_pro == "nan":
                hgvs_pro = None

            cr.pop(HGVS_NT_COLUMN)
            cr.pop(HGVS_SPLICE_COLUMN)
            cr.pop(HGVS_PRO_COLUMN)

            # JSON fields cannot store np.NaN values, so convert np.NaN to None.
            for key, value in sr.items():
                if is_csv_null(value) or value is np.NaN:
                    sr[key] = None
            if cr:
                for key, value in cr.items():
                    if is_csv_null(value) or value is np.NaN:
                        cr[key] = None

            data = {VARIANT_SCORE_DATA: sr, VARIANT_COUNT_DATA: {} if cr == sr else cr}
            variant = {
                HGVS_NT_COLUMN: hgvs_nt,
                HGVS_SPLICE_COLUMN: hgvs_splice,
                HGVS_PRO_COLUMN: hgvs_pro,
                "data": data,
            }
            variants.append(variant)

    return variants


def create_variants(db, score_set: ScoreSet, variants_data: list[VariantData], batch_size=None) -> int:
    num_variants = len(variants_data)
    variant_urns = bulk_create_urns(num_variants, score_set, True)
    variants = (
        # TODO: Is there a nicer way to handle this than passing dicts into kwargs
        # of the class initializer?
        Variant(urn=urn, score_set_id=score_set.id, **kwargs)  # type: ignore
        for urn, kwargs in zip(variant_urns, variants_data)
    )
    db.bulk_save_objects(variants)
    db.add(score_set)
    return len(score_set.variants)


def bulk_create_urns(n, score_set, reset_counter=False) -> list[str]:
    start_value = 0 if reset_counter else score_set.num_variants
    parent_urn = score_set.urn
    child_urns = ["{}#{}".format(parent_urn, start_value + (i + 1)) for i in range(n)]
    current_value = start_value + n
    score_set.num_variants = current_value
    return child_urns


def csv_data_to_df(file_data: BinaryIO) -> pd.DataFrame:
    extra_na_values = list(
        set(
            list(null_values_list)
            + [str(x).lower() for x in null_values_list]
            + [str(x).upper() for x in null_values_list]
            + [str(x).capitalize() for x in null_values_list]
        )
    )

    ingested_df = pd.read_csv(
        filepath_or_buffer=file_data,
        sep=",",
        encoding="utf-8",
        quotechar="'",
        comment="#",
        na_values=extra_na_values,
        keep_default_na=True,
        dtype={**{col: str for col in HGVSColumns.options()}, "scores": float},
    )

    for c in HGVSColumns.options():
        if c not in ingested_df.columns:
            ingested_df[c] = np.NaN

    return ingested_df


def columns_for_dataset(dataset: Optional[pd.DataFrame]) -> list[str]:
    if dataset is None:
        return []

    return [col for col in dataset.columns if col not in HGVSColumns.options()]
