from typing import Optional

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

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
from mavedb.models.keyword import Keyword
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.reference_map import ReferenceMap
from mavedb.models.scoreset import Scoreset
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.view_models.search import ScoresetsSearch

VariantData = dict[str, Optional[dict[str, dict]]]


def search_scoresets(db: Session, owner: Optional[User], search: ScoresetsSearch) -> list[Scoreset]:
    scoresets_query = db.query(Scoreset)  # \
    # .filter(Scoreset.private.is_(False))

    if owner is not None:
        scoresets_query = scoresets_query.filter(Scoreset.created_by_id == owner.id)

    if search.published is not None:
        if search.published:
            scoresets_query = scoresets_query.filter(Scoreset.published_date is not None)
        else:
            scoresets_query = scoresets_query.filter(Scoreset.published_date is None)

    if search.text:
        lower_search_text = search.text.lower()
        scoresets_query = scoresets_query.filter(
            or_(
                Scoreset.urn.contains(lower_search_text),
                Scoreset.title.contains(lower_search_text),
                Scoreset.short_description.contains(lower_search_text),
                Scoreset.abstract_text.contains(lower_search_text),
                Scoreset.target_gene.has(func.lower(TargetGene.name).contains(lower_search_text)),
                Scoreset.target_gene.has(func.lower(TargetGene.category).contains(lower_search_text)),
                Scoreset.keyword_objs.any(func.lower(Keyword.text).contains(lower_search_text))
                # TODO Add: ORGANISM_NAME UNIPROT, ENSEMBL, REFSEQ, LICENSE, plus TAX_ID if numeric
            )
        )

    if search.targets:
        scoresets_query = scoresets_query.filter(Scoreset.target_gene.has(TargetGene.name.in_(search.targets)))

    if search.target_organism_names:
        scoresets_query = scoresets_query.filter(
            Scoreset.target_gene.has(
                TargetGene.reference_maps.any(
                    ReferenceMap.genome.has(ReferenceGenome.organism_name.in_(search.target_organism_names))
                )
            )
        )

    if search.target_types:
        scoresets_query = scoresets_query.filter(Scoreset.target_gene.has(TargetGene.category.in_(search.target_types)))

    scoresets: list[Scoreset] = (
        scoresets_query.join(Scoreset.experiment).join(Scoreset.target_gene).order_by(Experiment.title).all()
    )
    if not scoresets:
        scoresets = []
    return scoresets  # filter_visible_scoresets(scoresets)


def filter_visible_scoresets(items: list[Scoreset]):
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
    print(scores.columns)
    print(counts.columns)
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
    for (primary_hgvs, group) in scores.groupby(by=scores.index, sort=False):
        score_records = group.to_dict(orient="records")
        if has_count_data:
            count_records = counts[counts.index == primary_hgvs].to_dict(orient="records")
            assert len(score_records) == len(count_records)
        else:
            # Make duplicates to zip with self when no count data.
            count_records = [r.copy() for r in score_records]

        for (sr, cr) in zip(score_records, count_records):
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
