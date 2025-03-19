import itertools
from collections import OrderedDict, Counter
from enum import Enum
from typing import Any, Union, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Table, func, select, Select
from sqlalchemy.orm import Session

from mavedb.deps import get_db
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.experiment import (
    Experiment,
    experiments_doi_identifiers_association_table,
    experiments_raw_read_identifiers_association_table,
)
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.models.raw_read_identifier import RawReadIdentifier
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.score_set import (
    ScoreSet,
    score_sets_doi_identifiers_association_table,
    score_sets_raw_read_identifiers_association_table,
)
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.uniprot_offset import UniprotOffset
from mavedb.models.user import User

router = APIRouter(
    prefix="/api/v1/statistics",
    tags=["statistics"],
    responses={404: {"description": "Not found"}},
)

TARGET_ACCESSION_TAXONOMY = "Homo sapiens"

## Union types

RecordModels = Union[type[Experiment], type[ScoreSet]]
RecordAssociationTables = Union[
    Table,
    type[ExperimentControlledKeywordAssociation],
    type[ExperimentPublicationIdentifierAssociation],
    type[ScoreSetPublicationIdentifierAssociation],
]


## Enum classes hold valid endpoints for different statistics routes.


class RecordNames(str, Enum):
    experiment = "experiment"
    scoreSet = "score-set"


class RecordFields(str, Enum):
    publicationIdentifiers = "publication-identifiers"
    keywords = "keywords"
    doiIdentifiers = "doi-identifiers"
    rawReadIdentifiers = "raw-read-identifiers"
    createdBy = "created-by"


class GroupBy(str, Enum):
    month = "month"
    year = "year"


def _model_and_association_from_record_field(
    record: RecordNames, field: Optional[RecordFields]
) -> tuple[RecordModels, Optional[RecordAssociationTables]]:
    """
    Given a member of the RecordNames and RecordFields Enums, generate the model and association table that can be used
    to generate statistics for those fields.

    This function should be used for generating statistics for fields shared between Experiments and Score Sets.
    If necessary, Experiment Sets can be handled in a similar manner in the future.
    """
    record_to_model_map: dict[RecordNames, RecordModels] = {
        RecordNames.experiment: Experiment,
        RecordNames.scoreSet: ScoreSet,
    }
    record_to_assc_map: dict[RecordNames, dict[RecordFields, RecordAssociationTables]] = {
        RecordNames.experiment: {
            RecordFields.doiIdentifiers: experiments_doi_identifiers_association_table,
            RecordFields.publicationIdentifiers: ExperimentPublicationIdentifierAssociation,
            RecordFields.rawReadIdentifiers: experiments_raw_read_identifiers_association_table,
            RecordFields.keywords: ExperimentControlledKeywordAssociation,
        },
        RecordNames.scoreSet: {
            RecordFields.doiIdentifiers: score_sets_doi_identifiers_association_table,
            RecordFields.publicationIdentifiers: ScoreSetPublicationIdentifierAssociation,
            RecordFields.rawReadIdentifiers: score_sets_raw_read_identifiers_association_table,
        },
    }

    queried_model = record_to_model_map[record]
    queried_model_assc = record_to_assc_map[record]

    if field is None or field not in queried_model_assc:
        return queried_model, None

    return queried_model, queried_model_assc[field]


def _join_model_and_filter_unpublished(query: Select, model: RecordModels) -> Select:
    return query.join(model).where(model.published_date.is_not(None))


def _count_for_identifier_in_query(db: Session, query: Select[tuple[Any, int]]) -> dict[Any, int]:
    return {value: count for value, count in db.execute(query).all() if value is not None}


########################################################################################
#  Record statistics
########################################################################################


@router.get(
    "/record/{record}/keywords", status_code=200, response_model=Union[dict[str, int], dict[str, dict[str, int]]]
)
def experiment_keyword_statistics(
    record: RecordNames, db: Session = Depends(get_db)
) -> Union[dict[str, int], dict[str, dict[str, int]]]:
    """
    Returns a dictionary of counts for the distinct values of the `value` field (member of the `controlled_keywords` table).
    Don't include any NULL field values. Don't include any keywords from unpublished experiments.
    """
    if record == RecordNames.scoreSet:
        raise HTTPException(
            422,
            "The 'keywords' field can only be used with the 'experiment' model. Score sets do not have associated keywords.",
        )

    queried_model, queried_assc = _model_and_association_from_record_field(record, RecordFields.keywords)

    if queried_assc is None:
        raise HTTPException(500, "No association table associated with the keywords field when one was expected.")

    query = _join_model_and_filter_unpublished(
        select(ControlledKeyword.value, func.count(ControlledKeyword.value)).join(queried_assc), queried_model
    ).group_by(ControlledKeyword.value)

    return _count_for_identifier_in_query(db, query)


@router.get("/record/{record}/publication-identifiers", status_code=200, response_model=dict[str, dict[str, int]])
def experiment_publication_identifier_statistics(
    record: RecordNames, db: Session = Depends(get_db)
) -> dict[str, dict[str, int]]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `publication_identifiers` table).
    Don't include any publication identifiers from unpublished experiments.
    """
    queried_model, queried_assc = _model_and_association_from_record_field(record, RecordFields.publicationIdentifiers)

    if queried_assc is None:
        raise HTTPException(
            500, "No association table associated with the publication identifiers field when one was expected."
        )

    query = _join_model_and_filter_unpublished(
        select(
            PublicationIdentifier.identifier,
            PublicationIdentifier.db_name,
            func.count(PublicationIdentifier.identifier),
        ).join(queried_assc),
        queried_model,
    ).group_by(PublicationIdentifier.identifier, PublicationIdentifier.db_name)

    publication_identifiers: dict[str, dict[str, int]] = {}

    for identifier, db_name, count in db.execute(query).all():
        # We don't need to worry about overwriting existing identifiers within these internal dictionaries because
        # of the SQL group by clause.
        if db_name in publication_identifiers:
            publication_identifiers[db_name][identifier] = count
        else:
            publication_identifiers[db_name] = {identifier: count}

    return publication_identifiers


@router.get("/record/{record}/raw-read-identifiers", status_code=200, response_model=dict[str, int])
def experiment_raw_read_identifier_statistics(record: RecordNames, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `raw_read_identifiers` table).
    Don't include any raw read identifiers from unpublished experiments.
    """
    queried_model, queried_assc = _model_and_association_from_record_field(record, RecordFields.rawReadIdentifiers)

    if queried_assc is None:
        raise HTTPException(
            500, "No association table associated with the raw read identifiers field when one was expected."
        )

    query = _join_model_and_filter_unpublished(
        select(RawReadIdentifier.identifier, func.count(RawReadIdentifier.identifier)).join(queried_assc), queried_model
    ).group_by(RawReadIdentifier.identifier)

    return _count_for_identifier_in_query(db, query)


@router.get("/record/{record}/doi-identifiers", status_code=200, response_model=dict[str, int])
def experiment_doi_identifiers_statistics(record: RecordNames, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `doi_identifiers` table).
    Don't include any DOI identifiers from unpublished experiments.
    """
    queried_model, queried_assc = _model_and_association_from_record_field(record, RecordFields.doiIdentifiers)

    if queried_assc is None:
        raise HTTPException(
            500, "No association table associated with the doi identifiers field when one was expected."
        )

    query = _join_model_and_filter_unpublished(
        select(DoiIdentifier.identifier, func.count(DoiIdentifier.identifier)).join(queried_assc), queried_model
    ).group_by(DoiIdentifier.identifier)

    return _count_for_identifier_in_query(db, query)


@router.get("/record/{record}/created-by", status_code=200, response_model=dict[str, int])
def experiment_created_by_statistics(record: RecordNames, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `username` field (member of the `users` table).
    Don't include any usernames from unpublished experiments.
    """
    queried_model, queried_assc = _model_and_association_from_record_field(record, RecordFields.createdBy)

    query = (
        select(User.username, func.count(User.id))
        .join(queried_model, queried_model.created_by_id == User.id)
        .filter(queried_model.published_date.is_not(None))
        .group_by(User.id)
    )

    return _count_for_identifier_in_query(db, query)


@router.get("/record/{model}/published/count", status_code=200, response_model=dict[str, int])
def record_counts(model: RecordNames, group: Optional[GroupBy] = None, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the number of published records of the `model` parameter.
    Optionally, group the counts by the published month or year.
    """
    queried_model, queried_assc = _model_and_association_from_record_field(model, None)

    # Protect against Nonetype publication dates with where clause.
    # We can safely ignore Mypy Nonetype errors in the following dictcomps.
    objs = db.scalars(
        select(queried_model.published_date)
        .where(queried_model.published_date.isnot(None))
        .order_by(queried_model.published_date)
    ).all()

    if group == GroupBy.month:
        grouped = {k: len(list(g)) for k, g in itertools.groupby(objs, lambda t: t.strftime("%Y-%m"))}  # type: ignore
    elif group == GroupBy.year:
        grouped = {k: len(list(g)) for k, g in itertools.groupby(objs, lambda t: t.strftime("%Y"))}  # type: ignore
    else:
        grouped = {"count": len(objs)}

    return OrderedDict(sorted(grouped.items()))


@router.get("/record/score-set/variant/count", status_code=200, response_model=dict[str, int])
def record_variant_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the number of published and distinct variants in the database contained
    within a given record.
    """
    variants = db.execute(
        select(PublishedVariantsMV.score_set_urn, func.count(PublishedVariantsMV.variant_id))
        .group_by(PublishedVariantsMV.score_set_urn)
        .order_by(PublishedVariantsMV.score_set_urn)
    ).all()

    grouped = {urn: sum(c for _, c in g) for urn, g in itertools.groupby(variants, lambda t: t[0])}
    return OrderedDict(sorted(filter(lambda item: item[1] > 0, grouped.items())))


@router.get("/record/score-set/mapped-variant/count", status_code=200, response_model=dict[str, int])
def record_mapped_variant_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the number of published and distinct mapped variants in the database contained
    within a given record.
    """
    variants = db.execute(
        select(PublishedVariantsMV.score_set_urn, func.count(PublishedVariantsMV.mapped_variant_id))
        .group_by(PublishedVariantsMV.score_set_urn)
        .order_by(PublishedVariantsMV.score_set_urn)
    ).all()

    grouped = {urn: sum(c for _, c in g) for urn, g in itertools.groupby(variants, lambda t: t[0])}
    return OrderedDict(sorted(filter(lambda item: item[1] > 0, grouped.items())))


########################################################################################
# Target statistics
########################################################################################


##### Accession based targets #####


@router.get("/target/accession/accession", status_code=200, response_model=dict[str, int])
def target_accessions_accession_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `accession` field (member of the `target_accessions` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetAccession.accession, func.count(TargetAccession.accession)).join(TargetGene), ScoreSet
    ).group_by(TargetAccession.accession)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/accession/assembly", status_code=200, response_model=dict[str, int])
def target_accessions_assembly_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `assembly` field (member of the `target_accessions` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetAccession.assembly, func.count(TargetAccession.assembly)).join(TargetGene), ScoreSet
    ).group_by(TargetAccession.assembly)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/accession/gene", status_code=200, response_model=dict[str, int])
def target_accessions_gene_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `gene` field (member of the `target_accessions` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetAccession.gene, func.count(TargetAccession.gene)).join(TargetGene), ScoreSet
    ).group_by(TargetAccession.gene)

    return _count_for_identifier_in_query(db, query)


##### Sequence based targets #####


@router.get("/target/sequence/sequence", status_code=200, response_model=dict[str, int])
def target_sequences_sequence_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `sequence` field (member of the `target_sequences` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetSequence.sequence, func.count(TargetSequence.sequence)).join(TargetGene), ScoreSet
    ).group_by(TargetSequence.sequence)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/sequence/sequence-type", status_code=200, response_model=dict[str, int])
def target_sequences_sequence_type_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `sequence_type` field (member of the `target_sequences` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetSequence.sequence_type, func.count(TargetSequence.sequence_type)).join(TargetGene), ScoreSet
    ).group_by(TargetSequence.sequence_type)

    return _count_for_identifier_in_query(db, query)


##### Target genes #####


@router.get("/target/gene/category", status_code=200, response_model=dict[str, int])
def target_genes_category_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `category` field (member of the `target_sequences` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetGene.category, func.count(TargetGene.category)), ScoreSet
    ).group_by(TargetGene.category)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/gene/organism", status_code=200, response_model=dict[str, int])
def target_genes_organism_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `organism` field (member of the `taxonomies` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.

    NOTE: For now (and perhaps forever), all accession based targets are human genomic sequences (ie: of taxonomy `Homo sapiens`).
          It is possible this assumption changes if we add mouse (or other non-human) genomes to MaveDB.
    """
    target_sequence_query = _join_model_and_filter_unpublished(
        select(Taxonomy.organism_name, func.count(Taxonomy.organism_name)).join(TargetSequence).join(TargetGene),
        ScoreSet,
    ).group_by(Taxonomy.organism_name)
    target_accession_query = _join_model_and_filter_unpublished(
        select(func.count(TargetAccession.id)).join(TargetGene), ScoreSet
    )

    # Ensure the `Homo sapiens` key exists in the organisms counts dictionary.
    organisms = _count_for_identifier_in_query(db, target_sequence_query)
    organisms.setdefault(TARGET_ACCESSION_TAXONOMY, 0)

    count_accession_based_targets = db.execute(target_accession_query).scalar_one_or_none()
    if count_accession_based_targets:
        organisms[TARGET_ACCESSION_TAXONOMY] += count_accession_based_targets
    else:
        organisms.pop(TARGET_ACCESSION_TAXONOMY)

    return organisms


@router.get("/target/gene/ensembl-identifier", status_code=200, response_model=dict[str, int])
def target_genes_ensembl_identifier_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `ensembl_identifiers` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(EnsemblIdentifier.identifier, func.count(EnsemblIdentifier.identifier))
        .join(EnsemblOffset)
        .join(TargetGene),
        ScoreSet,
    ).group_by(EnsemblIdentifier.identifier)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/gene/refseq-identifier", status_code=200, response_model=dict[str, int])
def target_genes_refseq_identifier_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `refseq_identifiers` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(RefseqIdentifier.identifier, func.count(RefseqIdentifier.identifier))
        .join(RefseqOffset)
        .join(TargetGene),
        ScoreSet,
    ).group_by(RefseqIdentifier.identifier)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/gene/uniprot-identifier", status_code=200, response_model=dict[str, int])
def target_genes_uniprot_identifier_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `identifier` field (member of the `uniprot_identifiers` table).
    Don't include any NULL field values. Don't include any targets from unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(UniprotIdentifier.identifier, func.count(UniprotIdentifier.identifier))
        .join(UniprotOffset)
        .join(TargetGene),
        ScoreSet,
    ).group_by(UniprotIdentifier.identifier)

    return _count_for_identifier_in_query(db, query)


@router.get("/target/mapped/gene")
def mapped_target_gene_counts(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the `gene` property within the `post_mapped_metadata`
    field (member of the `target_gene` table). Don't include any NULL field values. Don't include any targets from
    unpublished score sets.
    """
    query = _join_model_and_filter_unpublished(
        select(TargetGene.post_mapped_metadata),
        ScoreSet,
    ).where(TargetGene.post_mapped_metadata.isnot(None))

    mapping_metadata = db.scalars(query).all()
    gene_counts = Counter(
        gene
        for metadata in mapping_metadata
        for key in ("genomic", "protein")
        if key in metadata
        for gene in metadata[key].get("sequence_genes", [])
    )

    # The gene will always be a string
    return dict(gene_counts)  # type: ignore


########################################################################################
# Variant (and mapped variant) statistics
########################################################################################


@router.get("/variant/count", status_code=200, response_model=dict[str, int])
def variant_counts(group: Optional[GroupBy] = None, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the number of published and distinct variants in the database.
    Optionally, group the counts by the day on which the score set (and by extension, the variant) was published.
    """
    variants = db.execute(
        select(PublishedVariantsMV.published_date, func.count(PublishedVariantsMV.variant_id))
        .group_by(PublishedVariantsMV.published_date)
        .order_by(PublishedVariantsMV.published_date)
    ).all()

    if group == GroupBy.month:
        grouped = {k: sum(c for _, c in g) for k, g in itertools.groupby(variants, lambda t: t[0].strftime("%Y-%m"))}
    elif group == GroupBy.year:
        grouped = {k: sum(c for _, c in g) for k, g in itertools.groupby(variants, lambda t: t[0].strftime("%Y"))}
    else:
        grouped = {"count": sum(count for _, count in variants)}

    return OrderedDict(sorted(grouped.items()))


@router.get("/mapped-variant/count", status_code=200, response_model=dict[str, int])
def mapped_variant_counts(
    group: Optional[GroupBy] = None, onlyCurrent: bool = True, db: Session = Depends(get_db)
) -> dict[str, int]:
    """
    Returns a dictionary of counts for the number of published and distinct variants in the database.
    Optionally, group the counts by the day on which the score set (and by extension, the variant) was published.
    Optionally, return the count of all mapped variants, not just the current/most up to date ones.
    """
    query = select(PublishedVariantsMV.published_date, func.count(PublishedVariantsMV.mapped_variant_id))

    if onlyCurrent:
        query = query.where(PublishedVariantsMV.current_mapped_variant.is_(True))

    variants = db.execute(
        query.group_by(PublishedVariantsMV.published_date).order_by(PublishedVariantsMV.published_date)
    ).all()

    if group == GroupBy.month:
        grouped = {k: sum(c for _, c in g) for k, g in itertools.groupby(variants, lambda t: t[0].strftime("%Y-%m"))}
    elif group == GroupBy.year:
        grouped = {k: sum(c for _, c in g) for k, g in itertools.groupby(variants, lambda t: t[0].strftime("%Y"))}
    else:
        grouped = {"count": sum(count for _, count in variants)}

    return OrderedDict(sorted(grouped.items()))
