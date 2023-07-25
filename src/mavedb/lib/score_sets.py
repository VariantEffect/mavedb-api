from typing import Optional

import numpy as np
import pandas as pd
import json
from pandas.testing import assert_index_equal
from sqlalchemy import func, or_, and_
from sqlalchemy.orm import Session, aliased

from mavedb.lib.array_comparison import assert_array_equal
from mavedb.lib.exceptions import ValidationError
from mavedb.lib.mave.constants import (
    HGVS_NT_COLUMN,
    HGVS_PRO_COLUMN,
    HGVS_SPLICE_COLUMN,
    VARIANT_COUNT_DATA,
    VARIANT_SCORE_DATA,
)
from mavedb.lib.mave.utils import is_csv_null
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.keyword import Keyword
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.reference_map import ReferenceMap
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.view_models.search import ScoreSetsSearch

VariantData = dict[str, Optional[dict[str, dict]]]


def search_score_sets(db: Session, owner: Optional[User], search: ScoreSetsSearch) -> list[ScoreSet]:
    query = db.query(ScoreSet)  # \
    # .filter(ScoreSet.private.is_(False))

    if owner is not None:
        query = query.filter(ScoreSet.created_by_id == owner.id)

    if search.published is not None:
        if search.published:
            query = query.filter(ScoreSet.published_date is not None)
        else:
            query = query.filter(ScoreSet.published_date is None)

    if search.text:
        lower_search_text = search.text.lower()
        query = query.filter(
            or_(
                ScoreSet.urn.contains(lower_search_text),
                ScoreSet.title.contains(lower_search_text),
                ScoreSet.short_description.contains(lower_search_text),
                ScoreSet.abstract_text.contains(lower_search_text),
                ScoreSet.target_gene.has(func.lower(TargetGene.name).contains(lower_search_text)),
                ScoreSet.target_gene.has(func.lower(TargetGene.category).contains(lower_search_text)),
                ScoreSet.keyword_objs.any(func.lower(Keyword.text).contains(lower_search_text)),
                # TODO Add: ORGANISM_NAME UNIPROT, ENSEMBL, REFSEQ, LICENSE, plus TAX_ID if numeric
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.identifier).contains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.abstract).contains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.title).contains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.publication_journal).contains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.jsonb_path_exists(
                        PublicationIdentifier.authors, f"""$[*].name ? (@ like_regex "{lower_search_text}" flag "i")"""
                    )
                ),
            )
        )

    if search.targets:
        query = query.filter(ScoreSet.target_gene.has(TargetGene.name.in_(search.targets)))

    if search.target_organism_names:
        query = query.filter(
            ScoreSet.target_gene.has(
                TargetGene.reference_maps.any(
                    ReferenceMap.genome.has(ReferenceGenome.organism_name.in_(search.target_organism_names))
                )
            )
        )

    if search.target_types:
        query = query.filter(ScoreSet.target_gene.has(TargetGene.category.in_(search.target_types)))

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

    score_sets: list[ScoreSet] = (
        query.join(ScoreSet.experiment).join(ScoreSet.target_gene).order_by(Experiment.title).all()
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
        .group_by(ScoreSet)
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
        .group_by(ScoreSet)
        .having(func.count(analyzed_score_set.id) == len(urns))
        .all()
    )


def filter_visible_score_sets(items: list[ScoreSet]):
    # TODO Take the user into account.
    return filter(lambda item: not item.private, items or [])


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
    try:
        if HGVS_NT_COLUMN in scores:
            assert_array_equal(
                scores[HGVS_NT_COLUMN].sort_values().values,
                counts[HGVS_NT_COLUMN].sort_values().values,
            )
        if HGVS_SPLICE_COLUMN in scores:
            assert_array_equal(
                scores[HGVS_SPLICE_COLUMN].sort_values().values,
                counts[HGVS_SPLICE_COLUMN].sort_values().values,
            )
        if HGVS_PRO_COLUMN in scores:
            assert_array_equal(
                scores[HGVS_PRO_COLUMN].sort_values().values,
                counts[HGVS_PRO_COLUMN].sort_values().values,
            )
    except AssertionError:
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
