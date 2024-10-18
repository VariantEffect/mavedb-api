from enum import Enum
from typing import Union

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Table, func, select
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

## Enum classes hold valid endpoints for different statistics routes.


class TargetGeneFields(str, Enum):
    category = "category"
    organism = "organism"

    ensemblIdentifier = "ensembl-identifier"
    refseqIdentifier = "refseq-identifier"
    uniprotIdentifier = "uniprot-identifier"


class TargetAccessionFields(str, Enum):
    accession = "accession"
    assembly = "assembly"
    gene = "gene"


class TargetSequenceFields(str, Enum):
    sequence = "sequence"
    sequenceType = "sequence-type"


class RecordNames(str, Enum):
    experiment = "experiment"
    scoreSet = "score-set"


class RecordFields(str, Enum):
    publicationIdentifiers = "publication-identifiers"
    keywords = "keywords"
    doiIdentifiers = "doi-identifiers"
    rawReadIdentifiers = "raw-read-identifiers"
    createdBy = "created-by"


def _target_from_field_and_model(
    db: Session,
    model: Union[type[TargetAccession], type[TargetSequence]],
    field: Union[TargetAccessionFields, TargetSequenceFields],
):
    """
    Given either the target accession or target sequence model, generate counts that can be used to create
    a statistic for those fields.
    """
    # Protection from this case occurs via FastApi/pydantic Enum validation on endpoints that reference this function.
    # If we are careful with our enumeration definitons, we should not end up here.
    if (model == TargetAccession and field not in TargetAccessionFields) or (
        model == TargetSequence and field not in TargetSequenceFields
    ):
        raise HTTPException(422, f"Field `{field.name}` is incompatible with target model `{model}`.")

    published_score_sets_stmt = select(ScoreSet).where(ScoreSet.published_date.is_not(None)).subquery()

    # getattr obscures MyPy errors by coercing return type to Any
    model_field = field.value.replace("-", "_")
    column_field = getattr(model, model_field)
    query = (
        select(column_field, func.count(column_field))
        .join(TargetGene)
        .group_by(column_field)
        .join_from(TargetGene, published_score_sets_stmt)
    )

    return db.execute(query).all()


# Accession based targets only.
@router.get("/target/accession/{field}", status_code=200, response_model=dict[str, int])
def target_accessions_by_field(field: TargetAccessionFields, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the provided `field` (member of the `target_accessions` table).
    Don't include any NULL field values.
    """
    return {
        field_val: count
        for field_val, count in _target_from_field_and_model(db, TargetAccession, field)
        if field_val is not None
    }


# Sequence based targets only.
@router.get("/target/sequence/{field}", status_code=200, response_model=dict[str, int])
def target_sequences_by_field(field: TargetSequenceFields, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the provided `field` (member of the `target_sequences` table).
    Don't include any NULL field values.
    """
    return {
        field_val: count
        for field_val, count in _target_from_field_and_model(db, TargetSequence, field)
        if field_val is not None
    }


# Statistics on fields relevant to both accession and sequence based targets. Generally, these require custom logic to harmonize both target sub types.
@router.get("/target/gene/{field}", status_code=200, response_model=dict[str, int])
def target_genes_by_field(field: TargetGeneFields, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the provided `field` (member of the `target_sequences` table).
    Don't include any NULL field values. Each field here is handled individually because of the unique structure of this
    target gene object- fields might require information from both TargetGene subtypes (accession and sequence).
    """
    association_tables: dict[TargetGeneFields, Union[type[EnsemblOffset], type[RefseqOffset], type[UniprotOffset]]] = {
        TargetGeneFields.ensemblIdentifier: EnsemblOffset,
        TargetGeneFields.refseqIdentifier: RefseqOffset,
        TargetGeneFields.uniprotIdentifier: UniprotOffset,
    }
    identifier_models: dict[
        TargetGeneFields, Union[type[EnsemblIdentifier], type[RefseqIdentifier], type[UniprotIdentifier]]
    ] = {
        TargetGeneFields.ensemblIdentifier: EnsemblIdentifier,
        TargetGeneFields.refseqIdentifier: RefseqIdentifier,
        TargetGeneFields.uniprotIdentifier: UniprotIdentifier,
    }

    # All targets linked to a published score set.
    published_score_sets_stmt = select(TargetGene).join(ScoreSet).where(ScoreSet.published_date.is_not(None)).subquery()

    # Assumes identifiers cannot be duplicated within a Target.
    if field in identifier_models.keys():
        # getattr obscures MyPy errors by coercing return type to Any
        attr_for_identifier = getattr(identifier_models[field], "identifier")

        query = (
            select(attr_for_identifier, func.count(attr_for_identifier))
            .join(association_tables[field])
            .join(published_score_sets_stmt)
            .group_by(attr_for_identifier)
        )

        return {identifier: count for identifier, count in db.execute(query).all() if identifier is not None}

    # Can't join a TargetGene query to TargetGene query, so just select the desired columns directly from the subquery.
    elif field is TargetGeneFields.category:
        query = select(published_score_sets_stmt.c.category, func.count(published_score_sets_stmt.c.category)).group_by(
            published_score_sets_stmt.c.category
        )

        return {category: count for category, count in db.execute(query).all() if category is not None}

    # Target gene organism needs special handling: it is stored differently between accession and sequence Targets.
    elif field is TargetGeneFields.organism:
        sequence_based_targets_query = (
            select(Taxonomy.organism_name, func.count(Taxonomy.organism_name))
            .join(TargetSequence)
            .join(published_score_sets_stmt)
            .group_by(Taxonomy.organism_name)
        )
        accession_based_targets_query = select(func.count(TargetAccession.id)).join(published_score_sets_stmt)

        organisms: dict[str, int] = {
            organism: count
            for organism, count in db.execute(sequence_based_targets_query).all()
            if organism is not None
        }
        accession_count = db.execute(accession_based_targets_query).scalar_one_or_none()

        # NOTE: For now (forever?), all accession based targets are human genomic sequences. It is possible this
        #       assumption changes if we add mouse (or other non-human) genomes to MaveDB.
        if "Homo sapiens" in organisms and accession_count:
            organisms["Homo sapiens"] += accession_count
        elif accession_count:
            organisms["Homo sapiens"] = accession_count

        return organisms

    # Protection from this case occurs via FastApi/pydantic Enum validation.
    else:
        raise ValueError(f"Unknown field: {field}")


def _record_from_field_and_model(
    db: Session,
    model: RecordNames,
    field: RecordFields,
):
    """
    Given a member of the RecordNames and RecordFields Enums, generate counts that can be used to create a
    statistic for those enums.

    This function should be used for generating statistics for fields shared between Experiments and Score Sets.
    If necessary, Experiment Sets can be handled in a similar manner in the future.
    """
    association_tables: dict[
        RecordNames,
        dict[
            RecordFields,
            Union[
                Table,
                type[ExperimentControlledKeywordAssociation],
                type[ExperimentPublicationIdentifierAssociation],
                type[ScoreSetPublicationIdentifierAssociation],
            ],
        ],
    ] = {
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

    models: dict[RecordNames, Union[type[Experiment], type[ScoreSet]]] = {
        RecordNames.experiment: Experiment,
        RecordNames.scoreSet: ScoreSet,
    }

    queried_model = models[model]

    # created-by field does not operate on association tables and is defined directly on score set / experiment
    # records, so we operate directly on those records.
    # getattr obscures MyPy errors by coercing return type to Any
    model_created_by_field = getattr(queried_model, "created_by_id")
    model_published_data_field = getattr(queried_model, "published_date")
    if field is RecordFields.createdBy:
        query = (
            select(User.username, func.count(User.id))
            .join(queried_model, model_created_by_field == User.id)
            .where(model_published_data_field.is_not(None))
            .group_by(User.id)
        )

        return db.execute(query).all()
    else:
        # All assc table identifiers which are linked to a published model.
        queried_assc_table = association_tables[model][field]
        published_score_sets_statement = (
            select(queried_assc_table).join(queried_model).where(model_published_data_field.is_not(None)).subquery()
        )

    # Assumes any identifiers / keywords may not be duplicated within a record.
    if field is RecordFields.doiIdentifiers:
        query = select(DoiIdentifier.identifier, func.count(DoiIdentifier.identifier)).group_by(
            DoiIdentifier.identifier
        )
    elif field is RecordFields.keywords:
        query = select(ControlledKeyword.value, func.count(ControlledKeyword.value)).group_by(ControlledKeyword.value)
    elif field is RecordFields.rawReadIdentifiers:
        query = select(RawReadIdentifier.identifier, func.count(RawReadIdentifier.identifier)).group_by(
            RawReadIdentifier.identifier
        )

    # Handle publication identifiers separately since they may have duplicated identifiers
    elif field is RecordFields.publicationIdentifiers:
        publication_query = (
            select(
                PublicationIdentifier.identifier,
                PublicationIdentifier.db_name,
                func.count(PublicationIdentifier.identifier),
            )
            .join(published_score_sets_statement)
            .group_by(PublicationIdentifier.identifier, PublicationIdentifier.db_name)
        )

        publication_identifiers: dict[str, dict[str, int]] = {}

        for identifier, db_name, count in db.execute(publication_query).all():
            # We don't need to worry about overwriting existing identifiers within these internal dictionaries because
            # of the SQL group by clause.
            if db_name in publication_identifiers:
                publication_identifiers[db_name][identifier] = count
            else:
                publication_identifiers[db_name] = {identifier: count}

        return [(db_name, identifiers) for db_name, identifiers in publication_identifiers.items()]

    # Protection from this case occurs via FastApi/pydantic Enum validation on methods which reference this one.
    else:
        return []

    return db.execute(query.join(published_score_sets_statement)).all()


# Model based statistics for shared fields.
#
# NOTE: If custom logic is needed for record models with more specific endpoint paths,
#       i.e. non-shared fields, define them above this route so as not to obscure them.
@router.get("/record/{model}/{field}", status_code=200, response_model=Union[dict[str, int], dict[str, dict[str, int]]])
def record_object_statistics(
    model: RecordNames, field: RecordFields, db: Session = Depends(get_db)
) -> Union[dict[str, int], dict[str, dict[str, int]]]:
    """
    Resolve a dictionary of statistics based on the provided model name and model field.

    Model names and fields should be members of the Enum classes defined above. Providing an invalid model name or
    model field will yield a 422 Unprocessable Entity error with details about valid enum values.
    """
    # Validation to ensure 'keywords' is only used with 'experiment'.
    if model == RecordNames.scoreSet and field == RecordFields.keywords:
        raise HTTPException(
            status_code=422, detail="The 'keywords' field can only be used with the 'experiment' model."
        )

    count_data = _record_from_field_and_model(db, model, field)

    return {field_val: count for field_val, count in count_data if field_val is not None}
