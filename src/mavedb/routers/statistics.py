from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, Table
from sqlalchemy.orm import Session
from typing import Union

from mavedb.deps import get_db
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.keyword import Keyword
from mavedb.models.raw_read_identifier import RawReadIdentifier
from mavedb.models.experiment import (
    Experiment,
    experiments_doi_identifiers_association_table,
    experiments_keywords_association_table,
    experiments_raw_read_identifiers_association_table,
)
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set import (
    ScoreSet,
    score_sets_doi_identifiers_association_table,
    score_sets_keywords_association_table,
    score_sets_raw_read_identifiers_association_table,
)
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.refseq_offset import RefseqOffset
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
    reference = "reference"

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

    query = (
        db.query(
            getattr(model, field.value.replace("-", "_")),
            func.count(getattr(model, field.value.replace("-", "_"))),
        )
        .group_by(getattr(model, field.value.replace("-", "_")))
        .join(TargetGene)
        .join(ScoreSet)
        .filter((ScoreSet.published_date.is_not(None)))
    )

    return query.all()


# Accession based targets only.
@router.get("/target/accession/{field}", status_code=200, response_model=dict[str, int])
def target_accessions_by_field(field: TargetAccessionFields, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the provided `field` (member of the `target_accessions` table).
    Don't include any NULL field values.
    """
    return {row[0]: row[1] for row in _target_from_field_and_model(db, TargetAccession, field) if row[0] is not None}


# Sequence based targets only.
@router.get("/target/sequence/{field}", status_code=200, response_model=dict[str, int])
def target_sequences_by_field(field: TargetSequenceFields, db: Session = Depends(get_db)) -> dict[str, int]:
    """
    Returns a dictionary of counts for the distinct values of the provided `field` (member of the `target_sequences` table).
    Don't include any NULL field values.
    """
    return {row[0]: row[1] for row in _target_from_field_and_model(db, TargetSequence, field) if row[0] is not None}


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

    # Assumes identifiers cannot be duplicated within a Target.
    if field in identifier_models.keys():
        query = (
            db.query(
                getattr(identifier_models[field], "identifier"),
                func.count(getattr(identifier_models[field], "identifier")),
            )
            .join(association_tables[field])
            .group_by(identifier_models[field].identifier)
            .join(TargetGene)
            .join(ScoreSet)
            .filter((ScoreSet.published_date.is_not(None)))
        )

        return {row[0]: row[1] for row in query.all() if row[0] is not None}

    elif field is TargetGeneFields.category:
        query = (
            db.query(TargetGene.category, func.count(TargetGene.category))
            .group_by(TargetGene.category)
            .join(ScoreSet)
            .filter((ScoreSet.published_date.is_not(None)))
        )

        return {row[0]: row[1] for row in query.all() if row[0] is not None}

    # Target gene organism and reference need special handling: These fields are stored differently between accession and sequence Targets.
    elif field is TargetGeneFields.organism:
        sequence_based_targets_query = (
            db.query(ReferenceGenome.organism_name, func.count(ReferenceGenome.organism_name))
            .join(TargetSequence)
            .group_by(ReferenceGenome.organism_name)
            .join(TargetGene)
            .join(ScoreSet)
            .filter((ScoreSet.published_date.is_not(None)))
        )
        accession_based_targets_query = (
            db.query(TargetAccession).join(TargetGene).join(ScoreSet).filter((ScoreSet.published_date.is_not(None)))
        )

        organisms: dict[str, int] = {row[0]: row[1] for row in sequence_based_targets_query.all() if row[0] is not None}
        accessions = accession_based_targets_query.count()

        # NOTE: For now (forever?), all accession based targets are human genomic sequences. It is possible this
        #       assumption changes if we add mouse (or other non-human) genomes to MaveDB.
        if "Homo sapiens" in organisms:
            organisms["Homo sapiens"] += accessions
        elif accessions:
            organisms["Homo sapiens"] = accessions

        return organisms

    elif field is TargetGeneFields.reference:
        sequence_references_by_id_query = (
            db.query(TargetSequence.reference_id, func.count(TargetSequence.reference_id))
            .group_by(TargetSequence.reference_id)
            .join(TargetGene)
            .join(ScoreSet)
            .filter((ScoreSet.published_date.is_not(None)))
        )
        accession_references_by_id_query = (
            db.query(TargetAccession.assembly, func.count(TargetAccession.assembly))
            .group_by(TargetAccession.assembly)
            .join(TargetGene)
            .join(ScoreSet)
            .filter((ScoreSet.published_date.is_not(None)))
        )

        # Sequence based target reference genomes are stored within a separate `ReferenceGenome` object.
        sequence_references = {
            db.query(ReferenceGenome.short_name).filter(ReferenceGenome.id == reference_id).one()[0]: count
            for reference_id, count in sequence_references_by_id_query.all()
            if reference_id is not None
        }
        accession_references = {
            assembly[0]: assembly[1] for assembly in accession_references_by_id_query.all() if assembly[0] is not None
        }

        return {
            k: accession_references.get(k, 0) + sequence_references.get(k, 0)
            for k in set(accession_references) | set(sequence_references)
        }

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
                Table, type[ExperimentPublicationIdentifierAssociation], type[ScoreSetPublicationIdentifierAssociation]
            ],
        ],
    ] = {
        RecordNames.experiment: {
            RecordFields.doiIdentifiers: experiments_doi_identifiers_association_table,
            RecordFields.publicationIdentifiers: ExperimentPublicationIdentifierAssociation,
            RecordFields.rawReadIdentifiers: experiments_raw_read_identifiers_association_table,
            RecordFields.keywords: experiments_keywords_association_table,
        },
        RecordNames.scoreSet: {
            RecordFields.doiIdentifiers: score_sets_doi_identifiers_association_table,
            RecordFields.publicationIdentifiers: ScoreSetPublicationIdentifierAssociation,
            RecordFields.rawReadIdentifiers: score_sets_raw_read_identifiers_association_table,
            RecordFields.keywords: score_sets_keywords_association_table,
        },
    }

    models: dict[RecordNames, Union[type[Experiment], type[ScoreSet]]] = {
        RecordNames.experiment: Experiment,
        RecordNames.scoreSet: ScoreSet,
    }

    # Assumes any identifiers / keywords may not be duplicated within a record.
    if field is RecordFields.doiIdentifiers:
        query = (
            db.query(DoiIdentifier.identifier, func.count(DoiIdentifier.identifier))
            .join(association_tables[model][field])
            .group_by(DoiIdentifier.identifier)
            .join(models[model])
            .filter(getattr(models[model], "published_date").is_not(None))
        )
    elif field is RecordFields.keywords:
        query = (
            db.query(Keyword.text, func.count(Keyword.text))
            .join(association_tables[model][field])
            .group_by(Keyword.text)
            .join(models[model])
            .filter(getattr(models[model], "published_date").is_not(None))
        )
    elif field is RecordFields.rawReadIdentifiers:
        query = (
            db.query(RawReadIdentifier.identifier, func.count(RawReadIdentifier.identifier))
            .join(association_tables[model][field])
            .group_by(RawReadIdentifier.identifier)
            .join(models[model])
            .filter(getattr(models[model], "published_date").is_not(None))
        )
    elif field is RecordFields.createdBy:
        query = (
            db.query(User.username, func.count(User.id))
            .join(ScoreSet, ScoreSet.created_by_id == User.id)
            .group_by(User.id)
            .filter(getattr(models[model], "published_date").is_not(None))
        )

    # Handle publication identifiers separately since they may have duplicated identifiers
    elif field is RecordFields.publicationIdentifiers:
        publication_query = (
            db.query(
                PublicationIdentifier.identifier,
                PublicationIdentifier.db_name,
                func.count(PublicationIdentifier.identifier),
            )
            .join(association_tables[model][field])
            .group_by(PublicationIdentifier.identifier, PublicationIdentifier.db_name)
            .join(models[model])
            .filter((getattr(models[model], "published_date").is_not(None)))
        )

        publication_identifiers: dict[str, dict[str, int]] = {}

        for identifier in publication_query.all():
            db_name = identifier[1]

            # We don't need to worry about overwriting existing identifiers within these internal dictionaries because
            # of the SQL group by clause.
            if db_name in publication_identifiers:
                publication_identifiers[db_name][identifier[0]] = identifier[2]
            else:
                publication_identifiers[db_name] = {identifier[0]: identifier[2]}

        return [(db_name, identifiers) for db_name, identifiers in publication_identifiers.items()]

    # Protection from this case occurs via FastApi/pydantic Enum validation on methods which reference this one.
    else:
        return []

    return query.all()


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
    count_data = _record_from_field_and_model(db, model, field)

    return {row[0]: row[1] for row in count_data if row is not None}
