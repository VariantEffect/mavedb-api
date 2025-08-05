import click
import logging
from typing import Optional

from sqlalchemy.orm import Session

from mavedb.scripts.environment import with_database_session
from mavedb.models.score_set import ScoreSet
from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata

VALID_UNIPROT_DBS = [
    "UniProtKB",
    "UniProtKB_AC-ID",
    "UniProtKB-Swiss-Prot",
    "UniParc",
    "UniRef50",
    "UniRef90",
    "UniRef100",
]

logger = logging.getLogger(__name__)


@click.command()
@with_database_session
@click.option("--score-set-urn", type=str, default=None, help="Score set URN to process. If not provided, process all.")
@click.option("--polling-interval", type=int, default=30, help="Polling interval in seconds for checking job status.")
@click.option("--polling-attempts", type=int, default=5, help="Number of tries to poll for job completion.")
@click.option("--to-db", type=str, default="UniProtKB", help="Target UniProt database for ID mapping.")
def main(db: Session, score_set_urn: Optional[str], polling_interval: int, polling_attempts: int, to_db: str) -> None:
    if to_db not in VALID_UNIPROT_DBS:
        raise ValueError(f"Invalid target database: {to_db}. Must be one of {VALID_UNIPROT_DBS}.")
    if score_set_urn:
        score_sets = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).all()
    else:
        score_sets = db.query(ScoreSet).all()

    api = UniProtIDMappingAPI(polling_interval=polling_interval, polling_tries=polling_attempts)

    logger.info(f"Processing {len(score_sets)} score sets.")
    for score_set in score_sets:
        if not score_set.target_genes:
            logger.warning(f"No target gene for score set {score_set.urn}. Skipped mapping this score set.")
            continue

        for target_gene in score_set.target_genes:
            if not target_gene.post_mapped_metadata:
                logger.warning(
                    f"No post-mapped metadata for target gene {target_gene.id}. Skipped mapping this target."
                )
                continue

            ids = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
            if not ids:
                logger.warning(f"No IDs found in post_mapped_metadata for target gene {target_gene.id}")
                continue

            # Formalize the assumption that we expect exactly one ID in post_mapped_metadata for UniProt ID mapping.
            # It isn't necessarily imply a problem if there are multiple IDs at some later point, but this script assumes
            # there will be only one ID to map.
            assert len(ids) == 1, "Expected exactly one ID in post_mapped_metadata for UniProt ID mapping."
            id_to_map = ids[0]

            from_db = infer_db_name_from_sequence_accession(id_to_map)
            job_id = api.submit_id_mapping(from_db, to_db=to_db, ids=[id_to_map])
            if not job_id:
                logger.warning(f"Failed to submit job for target gene {target_gene.id}")
                continue

            if not api.check_id_mapping_results_ready(job_id):
                logger.warning(f"Job {job_id} not ready for target gene {target_gene.id}")
                continue

            results = api.get_id_mapping_results(job_id)
            uniprot_id = api.extract_uniprot_id_from_results(results)
            if not uniprot_id:
                logger.warning(f"No UniProt ID found for target gene {target_gene.id}")
                continue

            # Same assumption as above.
            assert len(uniprot_id) == 1, "Expected exactly one UniProt ID from mapping results."
            mapping_results = uniprot_id[0]

            target_gene.uniprot_id_from_mapped_metadata = mapping_results[id_to_map]
            db.add(target_gene)

            logger.info(f"Updated target gene {target_gene.id} with UniProt ID {mapping_results[id_to_map]}")

        logger.info(f"Processed score set {score_set.id} with {len(score_set.target_genes)} target genes.")

    logger.info(f"Done processing {len(score_sets)} score sets.")


if __name__ == "__main__":
    main()
