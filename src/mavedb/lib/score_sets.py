import logging
from collections import Counter, defaultdict
from operator import attrgetter
from typing import TYPE_CHECKING, BinaryIO, Optional, Sequence

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query, Session, aliased, contains_eager, joinedload, selectinload

from mavedb.lib.exceptions import ValidationError
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.mave.constants import (
    HGVS_NT_COLUMN,
    HGVS_PRO_COLUMN,
    HGVS_SPLICE_COLUMN,
    VARIANT_COUNT_DATA,
    VARIANT_SCORE_DATA,
)
from mavedb.lib.mave.utils import is_csv_null
from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.types.authentication import UserData
from mavedb.lib.validation.constants.general import null_values_list
from mavedb.models.contributor import Contributor
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_set_publication_identifier import (
    ScoreSetPublicationIdentifierAssociation,
)
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.uniprot_offset import UniprotOffset
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.search import ControlledKeywordFilterOption, ScoreSetsSearch

if TYPE_CHECKING:
    from mavedb.lib.permissions import Action

VariantData = dict[str, Optional[dict[str, dict]]]

logger = logging.getLogger(__name__)


class HGVSColumns:
    NUCLEOTIDE: str = "hgvs_nt"  # dataset.constants.hgvs_nt_column
    TRANSCRIPT: str = "hgvs_splice"  # dataset.constants.hgvs_splice_column
    PROTEIN: str = "hgvs_pro"  # dataset.constants.hgvs_pro_column

    @classmethod
    def options(cls) -> list[str]:
        return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]


def build_search_score_sets_query_filter(
    db: Session, query: Query[ScoreSet], owner_or_contributor: Optional[User], search: ScoreSetsSearch
):
    # Exclude score sets that have been publicly superseded (i.e., have at least one
    # published superseding version). Uses NOT EXISTS instead of LEFT OUTER JOIN to
    # avoid row multiplication when multiple superseding versions point to the same
    # original via replaces_id (which has no uniqueness constraint). A LEFT JOIN would
    # produce N rows per original, all counted against the LIMIT, causing paginated
    # searches to return fewer unique score sets than requested.
    query = query.filter(~ScoreSet.superseding_score_set.has(ScoreSet.published_date.isnot(None)))

    if owner_or_contributor is not None:
        query = query.filter(
            or_(
                ScoreSet.created_by_id == owner_or_contributor.id,
                ScoreSet.contributors.any(Contributor.orcid_id == owner_or_contributor.username),
            )
        )

    if search.published is not None:
        if search.published:
            query = query.filter(ScoreSet.published_date.isnot(None))
        else:
            query = query.filter(ScoreSet.published_date.is_(None))

    if search.text:
        lower_search_text = search.text.lower().strip()
        query = query.filter(
            or_(
                ScoreSet.urn.icontains(lower_search_text),
                ScoreSet.title.icontains(lower_search_text),
                ScoreSet.short_description.icontains(lower_search_text),
                ScoreSet.abstract_text.icontains(lower_search_text),
                ScoreSet.target_genes.any(func.lower(TargetGene.name).icontains(lower_search_text)),
                ScoreSet.target_genes.any(func.lower(TargetGene.category).icontains(lower_search_text)),
                ScoreSet.target_genes.any(
                    TargetGene.target_sequence.has(
                        TargetSequence.taxonomy.has(func.lower(Taxonomy.organism_name).icontains(lower_search_text))
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.target_sequence.has(
                        TargetSequence.taxonomy.has(func.lower(Taxonomy.common_name).icontains(lower_search_text))
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.target_accession.has(func.lower(TargetAccession.assembly).icontains(lower_search_text))
                ),
                # TODO(#94): add LICENSE, plus TAXONOMY CODE if numeric
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.identifier).icontains(lower_search_text)
                ),
                ScoreSet.publication_identifiers.any(
                    func.lower(PublicationIdentifier.doi).icontains(lower_search_text)
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
                        PublicationIdentifier.authors,
                        f"""$[*].name ? (@ like_regex "{lower_search_text}" flag "i")""",
                    )
                ),
                ScoreSet.doi_identifiers.any(func.lower(DoiIdentifier.identifier).icontains(lower_search_text)),
                ScoreSet.target_genes.any(
                    TargetGene.uniprot_offset.has(
                        UniprotOffset.identifier.has(
                            func.lower(UniprotIdentifier.identifier).icontains(lower_search_text)
                        )
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.refseq_offset.has(
                        RefseqOffset.identifier.has(
                            func.lower(RefseqIdentifier.identifier).icontains(lower_search_text)
                        )
                    )
                ),
                ScoreSet.target_genes.any(
                    TargetGene.ensembl_offset.has(
                        EnsemblOffset.identifier.has(
                            func.lower(EnsemblIdentifier.identifier).icontains(lower_search_text)
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
                    TargetSequence.taxonomy.has(Taxonomy.organism_name.in_(search.target_organism_names))
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

    if search.keywords:
        for item in search.keywords:
            query = query.filter(
                ScoreSet.experiment.has(
                    Experiment.keyword_objs.any(
                        ExperimentControlledKeywordAssociation.controlled_keyword.has(
                            and_(
                                ControlledKeyword.key == item.key,
                                ControlledKeyword.label == item.label,
                            )
                        )
                    )
                )
            )

    return query


def search_score_sets(db: Session, owner_or_contributor: Optional[User], search: ScoreSetsSearch):
    save_to_logging_context({"score_set_search_criteria": search.model_dump()})

    query = db.query(ScoreSet)
    query = build_search_score_sets_query_filter(db, query, owner_or_contributor, search)

    score_sets: list[ScoreSet] = (
        query.join(ScoreSet.experiment)
        .options(
            # Use selectinload for ALL relationships loaded via the main query. The presence of
            # contains_eager disables SQLAlchemy's subquery-wrapping logic for the ENTIRE query,
            # not just the relationships nested inside it. This means any joinedload that adds a
            # LEFT OUTER JOIN to the main SQL query — even for many-to-one relationships — can
            # corrupt the LIMIT clause by applying it to joined rows rather than unique score sets,
            # causing fewer results than expected and suppressing the count query fallback.
            # The only JOINs that should remain in the main query are the explicit experiment
            # INNER JOIN (required by contains_eager) and the superseding score set LEFT OUTER JOIN
            # added by the filter builder.
            contains_eager(ScoreSet.experiment).options(
                selectinload(Experiment.experiment_set),
                selectinload(Experiment.keyword_objs).joinedload(
                    ExperimentControlledKeywordAssociation.controlled_keyword
                ),
                selectinload(Experiment.created_by),
                selectinload(Experiment.modified_by),
                selectinload(Experiment.doi_identifiers),
                selectinload(Experiment.publication_identifier_associations).joinedload(
                    ExperimentPublicationIdentifierAssociation.publication
                ),
                selectinload(Experiment.raw_read_identifiers),
                selectinload(Experiment.score_sets).options(
                    joinedload(ScoreSet.doi_identifiers),
                    joinedload(ScoreSet.publication_identifier_associations).joinedload(
                        ScoreSetPublicationIdentifierAssociation.publication
                    ),
                    joinedload(ScoreSet.target_genes).options(
                        joinedload(TargetGene.ensembl_offset).joinedload(EnsemblOffset.identifier),
                        joinedload(TargetGene.refseq_offset).joinedload(RefseqOffset.identifier),
                        joinedload(TargetGene.uniprot_offset).joinedload(UniprotOffset.identifier),
                        joinedload(TargetGene.target_sequence).joinedload(TargetSequence.taxonomy),
                        joinedload(TargetGene.target_accession),
                    ),
                ),
            ),
            selectinload(ScoreSet.license),
            selectinload(ScoreSet.doi_identifiers),
            selectinload(ScoreSet.publication_identifier_associations).joinedload(
                ScoreSetPublicationIdentifierAssociation.publication
            ),
            selectinload(ScoreSet.target_genes).options(
                joinedload(TargetGene.ensembl_offset).joinedload(EnsemblOffset.identifier),
                joinedload(TargetGene.refseq_offset).joinedload(RefseqOffset.identifier),
                joinedload(TargetGene.uniprot_offset).joinedload(UniprotOffset.identifier),
                joinedload(TargetGene.target_sequence).joinedload(TargetSequence.taxonomy),
                joinedload(TargetGene.target_accession),
            ),
        )
        .order_by(Experiment.title)
        .offset(search.offset if search.offset is not None else None)
        .limit(search.limit + 1 if search.limit is not None else None)
        .all()
    )
    if not score_sets:
        score_sets = []

    offset = search.offset if search.offset is not None else 0
    num_score_sets = offset + len(score_sets)
    if search.limit is not None and num_score_sets > offset + search.limit:
        # In the main query, we have allowed limit + 1 results. The extra record tells us whether we need to run a count
        # query.
        score_sets = score_sets[: search.limit]
        count_query = db.query(ScoreSet)
        count_query = build_search_score_sets_query_filter(db, count_query, owner_or_contributor, search)
        num_score_sets = count_query.order_by(None).limit(None).count()

    save_to_logging_context({"matching_resources": num_score_sets})
    logger.debug(msg=f"Score set search yielded {len(score_sets)} matching resources.", extra=logging_context())

    return {"score_sets": score_sets, "num_score_sets": num_score_sets}


def score_set_search_filter_options_from_counter(counter: Counter):
    return [{"value": value, "count": count} for value, count in counter.items()]


def fetch_score_set_search_filter_options(
    db: Session, requester: Optional[UserData], owner_or_contributor: Optional[User], search: ScoreSetsSearch
):
    save_to_logging_context({"score_set_search_criteria": search.model_dump()})

    query = db.query(ScoreSet)
    query = build_search_score_sets_query_filter(db, query, owner_or_contributor, search)
    score_sets: list[ScoreSet] = query.all()
    if not score_sets:
        score_sets = []

    # Target related counters
    target_category_counter: Counter[str] = Counter()
    target_name_counter: Counter[str] = Counter()
    target_organism_name_counter: Counter[str] = Counter()
    target_accession_counter: Counter[str] = Counter()
    # Publication related counters
    publication_author_name_counter: Counter[str] = Counter()
    publication_db_name_counter: Counter[str] = Counter()
    publication_journal_counter: Counter[str] = Counter()
    # Controlled keywords related counters
    controlled_keywords_counter: dict[str, Counter[str]] = defaultdict(Counter)

    # --- PERFORMANCE NOTE ---
    # The following counter construction loop is a bottleneck for large score set queries.
    # Practical future optimizations might include:
    #   - Batch permission checks and attribute access outside the loop if possible
    #   - Use parallelization (e.g., multiprocessing or concurrent.futures) for large datasets
    #   - Pre-fetch or denormalize target/publication data in the DB query
    #   - Profile and refactor nested attribute lookups to minimize Python overhead
    for score_set in score_sets:
        # Check read permission for each score set, skip if no permission
        if not has_permission(requester, score_set, Action.READ).permitted:
            continue

        # Target related options
        for target in getattr(score_set, "target_genes", []):
            category = getattr(target, "category", None)
            if category:
                target_category_counter[category] += 1

            name = getattr(target, "name", None)
            if name:
                target_name_counter[name] += 1

            target_sequence = getattr(target, "target_sequence", None)
            taxonomy = getattr(target_sequence, "taxonomy", None)
            organism_name = getattr(taxonomy, "organism_name", None)

            if organism_name:
                target_organism_name_counter[organism_name] += 1

            target_accession = getattr(target, "target_accession", None)
            accession = getattr(target_accession, "accession", None)

            if accession:
                target_accession_counter[accession] += 1

        # Publication related options
        for publication_association in getattr(score_set, "publication_identifier_associations", []):
            publication = getattr(publication_association, "publication", None)

            authors = getattr(publication, "authors", [])
            for author in authors:
                name = author.get("name")
                if name:
                    publication_author_name_counter[name] += 1

            db_name = getattr(publication, "db_name", None)
            if db_name:
                publication_db_name_counter[db_name] += 1

            journal = getattr(publication, "publication_journal", None)
            if journal:
                publication_journal_counter[journal] += 1

        # Controlled keywords related options
        for controlled_keyword in getattr(score_set.experiment, "keyword_objs", []):
            keyword = getattr(controlled_keyword, "controlled_keyword", [])
            if not keyword:
                continue
            key = getattr(keyword, "key", None)
            label = getattr(keyword, "label", None)
            if key and label:
                controlled_keywords_counter[key][label] += 1

    controlled_keywords_counter_list = []
    for key, label_counter in controlled_keywords_counter.items():
        for label, count in label_counter.items():
            controlled_keywords_counter_list.append(ControlledKeywordFilterOption(key=key, value=label, count=count))

    logger.debug(msg="Score set search filter options were fetched.", extra=logging_context())

    return {
        "target_gene_categories": score_set_search_filter_options_from_counter(target_category_counter),
        "target_gene_names": score_set_search_filter_options_from_counter(target_name_counter),
        "target_organism_names": score_set_search_filter_options_from_counter(target_organism_name_counter),
        "target_accessions": score_set_search_filter_options_from_counter(target_accession_counter),
        "publication_author_names": score_set_search_filter_options_from_counter(publication_author_name_counter),
        "publication_db_names": score_set_search_filter_options_from_counter(publication_db_name_counter),
        "publication_journals": score_set_search_filter_options_from_counter(publication_journal_counter),
        "keywords": controlled_keywords_counter_list,
    }


def fetch_superseding_score_set_in_search_result(
    score_sets: list[ScoreSet], requesting_user: Optional["UserData"], search: ScoreSetsSearch
) -> list[ScoreSet]:
    """
    Remove superseded score set from search results.
    Check whether all of the score set are correct versions.
    """
    from mavedb.lib.permissions import Action

    if search.published:
        filtered_score_sets_tail = [
            find_publish_or_private_superseded_score_set_tail(score_set, Action.READ, requesting_user, search.published)
            for score_set in score_sets
        ]
    else:
        filtered_score_sets_tail = [
            find_superseded_score_set_tail(score_set, Action.READ, requesting_user) for score_set in score_sets
        ]
    # Remove None item.
    filtered_score_sets = [score_set for score_set in filtered_score_sets_tail if score_set is not None]
    if filtered_score_sets:
        final_score_sets = sorted(set(filtered_score_sets), key=attrgetter("urn"))
    else:
        final_score_sets = []
    return final_score_sets


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


def find_superseded_score_set_tail(
    score_set: ScoreSet, action: Optional["Action"] = None, user_data: Optional["UserData"] = None
) -> Optional[ScoreSet]:
    while score_set.superseding_score_set is not None:
        next_score_set_in_chain = score_set.superseding_score_set

        # If we were given a permission to check and the next score set in the chain does not have that permission,
        # pretend like we have reached the end of the chain. Otherwise, continue to the next score set.
        if action is not None and not has_permission(user_data, next_score_set_in_chain, action).permitted:
            return score_set

        score_set = next_score_set_in_chain

    # Handle unpublished superseding score set case.
    # The score set has a published superseded score set but has not superseding score set.
    if action is not None and not has_permission(user_data, score_set, action).permitted:
        while score_set.superseded_score_set is not None:
            next_score_set_in_chain = score_set.superseded_score_set
            if has_permission(user_data, next_score_set_in_chain, action).permitted:
                return next_score_set_in_chain
            else:
                score_set = next_score_set_in_chain
        return None

    return score_set


def find_publish_or_private_superseded_score_set_tail(
    score_set: ScoreSet, action: Optional["Action"] = None, user_data: Optional["UserData"] = None, publish: bool = True
) -> Optional[ScoreSet]:
    from mavedb.lib.permissions import has_permission

    if publish:
        while score_set.superseding_score_set is not None:
            next_score_set_in_chain = score_set.superseding_score_set
            # Find the final published one.
            if (
                action is not None
                and has_permission(user_data, score_set, action).permitted
                and next_score_set_in_chain.published_date is None
            ):
                return score_set
            score_set = next_score_set_in_chain
    else:
        # Unpublished score set should not be superseded.
        # It should not have superseding score set, but possible have superseded score set.
        if (
            action is not None
            and score_set.published_date is None
            and has_permission(user_data, score_set, action).permitted
        ):
            return score_set
        else:
            return None
    return score_set


def get_current_mapped_variants_for_annotation(db: Session, score_set: ScoreSet) -> Sequence[MappedVariant]:
    """
    Load the current mapped variants for a score set with the relationships required to build VA-Spec
    annotations eagerly loaded.

    This is the single source of truth for the eager-load shape shared by the annotated-variant
    streaming endpoints and the public data export. The annotation builders reach through
    ``MappedVariant.variant.score_set`` for publications, contributors, license, experiment, and score
    calibrations, so each of those is loaded up front to avoid per-variant lazy loads.
    """
    return (
        db.query(MappedVariant)
        .join(MappedVariant.variant)
        .join(Variant.score_set)
        .filter(Variant.score_set_id == score_set.id)
        .filter(MappedVariant.current.is_(True))
        .options(
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set),
            contains_eager(MappedVariant.variant)
            .contains_eager(Variant.score_set)
            .selectinload(ScoreSet.publication_identifier_associations),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.created_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.modified_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.license),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.experiment),
            contains_eager(MappedVariant.variant)
            .contains_eager(Variant.score_set)
            .selectinload(ScoreSet.score_calibrations),
        )
        .all()
    )


def is_replaces_id_unique_violation(exc: IntegrityError) -> bool:
    """
    Return True if the IntegrityError was caused by the unique constraint on score_set.replaces_id.
    """
    orig = getattr(exc, "orig", None)
    if orig is None:
        return False

    diag = getattr(orig, "diag", None)
    detail = getattr(diag, "detail", "") or ""
    return "replaces_id" in detail


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
        (pd.isnull(array1) & pd.isnull(array2)) | (array1 == array2)
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


def refresh_variant_urns(db: Session, score_set: ScoreSet):
    variants = db.execute(select(Variant).where(Variant.score_set_id == score_set.id)).scalars()

    for variant in variants:
        if not variant.urn:
            raise ValueError("All variants should have an associated URN.")

        variant_number = variant.urn.split("#")[1]
        refreshed_urn = f"{score_set.urn}#{variant_number}"
        variant.urn = refreshed_urn
        db.add(variant)

    db.commit()


def bulk_create_urns(n, score_set, reset_counter=False) -> list[str]:
    start_value = 0 if reset_counter else score_set.num_variants
    parent_urn = score_set.urn
    child_urns = ["{}#{}".format(parent_urn, start_value + (i + 1)) for i in range(n)]
    current_value = start_value + n
    score_set.num_variants = current_value
    return child_urns


def csv_data_to_df(file_data: BinaryIO, induce_hgvs_cols: bool = True) -> pd.DataFrame:
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
        index_col=False,
        na_values=extra_na_values,
        keep_default_na=True,
        dtype={**{col: str for col in HGVSColumns.options()}, "scores": float},
    )

    if induce_hgvs_cols:
        for c in HGVSColumns.options():
            if c not in ingested_df.columns:
                ingested_df[c] = np.NaN

    return ingested_df


def columns_for_dataset(dataset: Optional[pd.DataFrame]) -> list[str]:
    if dataset is None:
        return []

    return [col for col in dataset.columns if col not in HGVSColumns.options()]
