import json
import logging
import requests
from typing import Sequence, Optional

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLINGEN_API_URL = "https://reg.test.genome.network/allele"


def get_gene_symbol_from_clingen(hgvs_string: str) -> Optional[str]:
    response = requests.get(f"{CLINGEN_API_URL}?hgvs={hgvs_string}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {hgvs_string}: {response.status_code}")
        return None

    data = response.json()
    if "aminoAcidAlleles" in data:
        return data["aminoAcidAlleles"][0].get("geneSymbol")
    elif "transcriptAlleles" in data:
        return data["transcriptAlleles"][0].get("geneSymbol")

    return None


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Generate gene mappings for every score set in MaveDB.", is_flag=True)
def generate_gene_mappings(db: Session, urns: Sequence[Optional[str]], all: bool):
    score_set_ids: Sequence[Optional[int]]
    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will generate gene mappings for {len(score_set_ids)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Generating gene mappings for the provided score sets ({len(score_set_ids)}).")

    for ss_id in score_set_ids:
        if not ss_id:
            continue

        score_set = db.scalar(select(ScoreSet).where(ScoreSet.id == ss_id))
        if not score_set:
            logger.warning(f"Could not fetch score set with id={ss_id}.")
            continue

        try:
            mapped_variant = db.scalars(
                select(MappedVariant)
                .join(Variant)
                .where(
                    Variant.score_set_id == ss_id,
                    MappedVariant.current.is_(True),
                    MappedVariant.post_mapped.isnot(None),
                )
                .limit(1)
            ).one_or_none()

            if not mapped_variant:
                logger.info(f"No current mapped variant found for score set {score_set.urn}.")
                continue

            # From line 69, this object must not be None.
            hgvs_string = mapped_variant.post_mapped.get("expressions", {})[0].get("value")  # type: ignore
            if not hgvs_string:
                logger.warning(f"No HGVS string found in post_mapped for variant {mapped_variant.id}.")
                continue

            gene_symbol = get_gene_symbol_from_clingen(hgvs_string)
            if not gene_symbol:
                logger.warning(f"No gene symbol found for HGVS string {hgvs_string}.")
                continue

            # This script has been designed to work prior to the introduction of multi-target mapping.
            # This .one() call reflects those assumptions.
            target_gene = db.scalars(select(TargetGene).where(TargetGene.score_set_id == ss_id)).one()

            if target_gene.post_mapped_metadata is None:
                logger.warning(
                    f"Target gene for score set {score_set.urn} has no post_mapped_metadata despite containing current mapped variants."
                )
                continue

            # Cannot update JSONB fields directly. They can be converted to dictionaries over Mypy's objections.
            post_mapped_metadata = json.loads(json.dumps(dict(target_gene.post_mapped_metadata.copy())))  # type: ignore
            if "genomic" in post_mapped_metadata:
                key = "genomic"
            elif "protein" in post_mapped_metadata:
                key = "protein"
            else:
                logger.warning(f"Unknown post_mapped type for variant {mapped_variant.id}.")

            if "sequence_genes" not in post_mapped_metadata[key]:
                post_mapped_metadata[key]["sequence_genes"] = []
            post_mapped_metadata[key]["sequence_genes"].append(gene_symbol)

            target_gene.post_mapped_metadata = post_mapped_metadata

            db.add(target_gene)
            db.commit()
            logger.info(f"Gene symbol {gene_symbol} added to target gene for score set {score_set.urn}.")

        except Exception as e:
            logger.error(f"Failed to generate gene mappings for score set {score_set.urn}: {str(e)}")
            db.rollback()

    logger.info("Done generating gene mappings.")


if __name__ == "__main__":
    generate_gene_mappings()
