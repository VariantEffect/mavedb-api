import logging
from typing import Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.contributor import Contributor
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from mavedb.view_models.search import TextSearch

logger = logging.getLogger(__name__)


def find_or_create_target_gene_by_accession(
    db: Session,
    score_set_id: int,
    tg: dict,
    tg_accession: dict,
) -> TargetGene:
    """
    Find or create a target gene for a score set by accession. If the existing target gene or related accession record is modified,
    this function creates a new target gene so that that its id can be used to determine if a score set has changed in a way
    that requires the create variants job to be re-run.

    : param db: Database session
    : param score_set_id: ID of the score set to associate the target gene with
    : param tg: Dictionary with target gene details (name, category, etc.)
    : param tg_accession: Dictionary with target accession details (accession, assembly, gene, etc.)
    : return: The found or newly created TargetGene instance
    """
    target_gene = None
    logger.info(
        msg=f"Searching for existing target gene by accession within score set {score_set_id}.",
        extra=logging_context(),
    )
    if tg_accession is not None and tg_accession.get("accession"):
        target_gene = (
            db.query(TargetGene)
            .filter(
                and_(
                    TargetGene.target_accession.has(
                        and_(
                            TargetAccession.accession == tg_accession["accession"],
                            TargetAccession.assembly == tg_accession["assembly"],
                            TargetAccession.gene == tg_accession["gene"],
                            TargetAccession.is_base_editor == tg_accession.get("is_base_editor", False),
                        )
                    ),
                    TargetGene.name == tg["name"],
                    TargetGene.category == tg["category"],
                    TargetGene.score_set_id == score_set_id,
                )
            )
            .first()
        )

    if target_gene is None:
        target_accession = TargetAccession(**tg_accession)
        target_gene = TargetGene(
            **tg,
            score_set_id=score_set_id,
            target_accession=target_accession,
        )
        db.add(target_gene)
        db.commit()
        db.refresh(target_gene)
        logger.info(
            msg=f"Created new target gene '{target_gene.name}' with ID {target_gene.id}.",
            extra=logging_context(),
        )
    else:
        logger.info(
            msg=f"Found existing target gene '{target_gene.name}' with ID {target_gene.id}.",
            extra=logging_context(),
        )

    return target_gene


def find_or_create_target_gene_by_sequence(
    db: Session,
    score_set_id: int,
    tg: dict,
    tg_sequence: dict,
) -> TargetGene:
    """
    Find or create a target gene for a score set by sequence. If the existing target gene or related sequence record is modified,
    this function creates a new target gene so that that its id can be used to determine if a score set has changed in a way
    that requires the create variants job to be re-run.

    : param db: Database session
    : param score_set_id: ID of the score set to associate the target gene with
    : param tg: Dictionary with target gene details (name, category, etc.)
    : param tg_sequence: Dictionary with target sequence details (sequence, sequence_type, taxonomy, label, etc.)
    : return: The found or newly created TargetGene instance
    """
    target_gene = None
    logger.info(
        msg=f"Searching for existing target gene by sequence within score set {score_set_id}.",
        extra=logging_context(),
    )
    if tg_sequence is not None and tg_sequence.get("sequence"):
        target_gene = (
            db.query(TargetGene)
            .filter(
                and_(
                    TargetGene.target_sequence.has(
                        and_(
                            TargetSequence.sequence == tg_sequence["sequence"],
                            TargetSequence.sequence_type == tg_sequence["sequence_type"],
                            TargetSequence.taxonomy.has(Taxonomy.id == tg_sequence["taxonomy"].id),
                            TargetSequence.label == tg_sequence["label"],
                        )
                    ),
                    TargetGene.name == tg["name"],
                    TargetGene.category == tg["category"],
                    TargetGene.score_set_id == score_set_id,
                )
            )
            .first()
        )

    if target_gene is None:
        target_sequence = TargetSequence(**tg_sequence)
        target_gene = TargetGene(
            **tg,
            score_set_id=score_set_id,
            target_sequence=target_sequence,
        )
        db.add(target_gene)
        db.commit()
        db.refresh(target_gene)
        logger.info(
            msg=f"Created new target gene '{target_gene.name}' with ID {target_gene.id}.",
            extra=logging_context(),
        )
    else:
        logger.info(
            msg=f"Found existing target gene '{target_gene.name}' with ID {target_gene.id}.",
            extra=logging_context(),
        )

    return target_gene


def search_target_genes(
    db: Session,
    owner_or_contributor: Optional[User],
    search: TextSearch,
    limit: Optional[int],
) -> list[TargetGene]:
    save_to_logging_context({"target_gene_search_criteria": search.model_dump()})

    query = db.query(TargetGene)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(TargetGene.name).contains(lower_search_text))
    if owner_or_contributor is not None:
        query = query.filter(
            TargetGene.score_set.has(
                or_(
                    ScoreSet.created_by_id == owner_or_contributor.id,
                    ScoreSet.contributors.any(Contributor.orcid_id == owner_or_contributor.username),
                )
            )
        )

    query = query.order_by(TargetGene.name)
    if limit is not None:
        query = query.limit(limit)

    target_genes = query.all()
    if not target_genes:
        target_genes = []

    save_to_logging_context({"matching_resources": len(target_genes)})
    logger.debug(
        msg=f"Target gene search yielded {len(target_genes)} matching resources.",
        extra=logging_context(),
    )

    return target_genes


def get_target_coding_info(score_set: ScoreSet) -> tuple[bool, Optional[str]]:
    """Extract target coding status and transcript accession for a single-target score set.

    Determines whether the score set target is protein-coding and identifies
    the transcript accession to use for HGVS lookups. For accession-based targets,
    uses the accession if it's an NM or ENST transcript. For sequence-based targets,
    prefers cDNA accession from post-mapped metadata.

    Args:
        score_set: The ScoreSet to analyze.

    Returns:
        Tuple of (target_is_coding, transcript_accession). transcript_accession
        may be None even for coding targets if no transcript could be determined.

    Raises:
        NotImplementedError: If the score set has multiple targets.
        ValueError: If ambiguous cDNA accessions are found in post-mapped metadata.
    """
    # TODO#712: Support multi-target score sets. Each variant's hgvs prefix
    # (e.g. "TARGET_NAME:c.1A>G") identifies which target it belongs to.
    # This function should return a dict[str, tuple[bool, Optional[str]]]
    # keyed by target name, and the job loop should resolve per-variant.
    if len(score_set.target_genes) != 1:
        raise NotImplementedError("Populating mapped HGVS for multi-target score sets is not yet supported.")

    target = score_set.target_genes[0]
    if target.category != "protein_coding":
        return False, None

    transcript_accession: Optional[str] = None

    # Accession-based: use transcript accession if it's an NM or ENST transcript
    if target.target_accession and target.target_accession.accession:
        if target.target_accession.accession.startswith(("NM", "ENST")):
            transcript_accession = target.target_accession.accession

    # Sequence-based: prefer cDNA accession from post-mapped metadata
    if target.post_mapped_metadata:
        assert isinstance(target.post_mapped_metadata, dict)
        cdna_accessions = target.post_mapped_metadata.get("cdna", {}).get("sequence_accessions")
        if cdna_accessions:
            if len(cdna_accessions) == 1:
                transcript_accession = cdna_accessions[0]
            else:
                raise ValueError(
                    f"Multiple cDNA accessions found in post-mapped metadata for target {target.name} "
                    f"in score set {score_set.urn}. Cannot determine which to use."
                )
        else:
            logger.warning(
                f"No cDNA accession found in post-mapped metadata for target {target.name} in score set "
                f"{score_set.urn}. If variants are at the nucleotide level, will assume MANE transcript "
                f"from ClinGen."
            )
    else:
        logger.warning(
            f"No post-mapped metadata for target {target.name} in score set {score_set.urn}. "
            f"Will assume MANE transcript from ClinGen for coding variant."
        )

    return True, transcript_accession
