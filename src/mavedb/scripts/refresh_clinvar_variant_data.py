import click
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
import requests
import csv
import time
import logging
import gzip
import random
import io

from typing import Dict, Any, Optional, Sequence
from datetime import date

from sqlalchemy import and_, select, distinct
from sqlalchemy.orm import Session

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.clinical_control import ClinicalControl
from mavedb.scripts.environment import with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def fetch_clinvar_variant_summary_tsv(month: Optional[str], year: Optional[str]) -> bytes:
    if month is None and year is None:
        url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz"
    else:
        url = f"https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/archive/variant_summary_{year}-{month}.txt.gz"

    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.content


def parse_tsv(tsv_content: bytes) -> Dict[int, Dict[str, str]]:
    with gzip.open(filename=io.BytesIO(tsv_content), mode="rt") as f:
        # This readlines object will only be a list of bytes if the file is opened in "rb" mode.
        reader = csv.DictReader(f.readlines(), delimiter="\t")  # type: ignore
        data = {int(row["#AlleleID"]): row for row in reader}

    return data


def query_clingen_allele_api(allele_id: str) -> Dict[str, Any]:
    url = f"https://reg.clinicalgenome.org/allele/{allele_id}"
    retries = 5
    for i in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            if i < retries - 1:
                wait_time = (2**i) + random.uniform(0, 1)
                logger.warning(f"Request failed ({e}), retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retries} attempts: {e}")
                raise

    logger.debug(f"Fetched ClinGen data for allele ID {allele_id}.")
    return response.json()


def refresh_clinvar_variants(db: Session, month: Optional[str], year: Optional[str], urns: Sequence[str]) -> None:
    tsv_content = fetch_clinvar_variant_summary_tsv(month, year)
    tsv_data = parse_tsv(tsv_content)
    version = f"{month}_{year}" if month and year else f"{date.today().month}_{date.today().year}"
    logger.info(f"Fetched TSV variant data for ClinVar for {version}.")

    if urns:
        clingen_ids = db.scalars(
            select(distinct(MappedVariant.clingen_allele_id))
                .join(Variant)
                .join(ScoreSet)
                .where(MappedVariant.current.is_(True), MappedVariant.post_mapped.is_not(None))
                .where(and_(
                    MappedVariant.clingen_allele_id.is_not(None),
                    MappedVariant.current.is_(True),
                    ScoreSet.urn.in_(urns)
                ))
        ).all()
    else:
        clingen_ids = db.scalars(
            select(distinct(MappedVariant.clingen_allele_id)).where(MappedVariant.clingen_allele_id.is_not(None))
        ).all()
    total_variants_with_clingen_ids = len(clingen_ids)

    logger.info(f"Fetching ClinGen data for {total_variants_with_clingen_ids} variants.")
    for index, clingen_id in enumerate(clingen_ids):
        if total_variants_with_clingen_ids > 0 and index % (max(total_variants_with_clingen_ids // 100, 1)) == 0:
            logger.info(f"Progress: {index / total_variants_with_clingen_ids:.0%}")

        # Guaranteed based on our query filters.
        clingen_data = query_clingen_allele_api(clingen_id)  # type: ignore
        clinvar_allele_id = clingen_data.get("externalRecords", {}).get("ClinVarAlleles", [{}])[0].get("alleleId")

        if not clinvar_allele_id or clinvar_allele_id not in tsv_data:
            logger.debug(
                f"No ClinVar variant data found for ClinGen allele ID {clingen_id}. ({index + 1}/{total_variants_with_clingen_ids})."
            )
            continue

        variant_data = tsv_data[clinvar_allele_id]
        clinvar_variant = db.scalars(
            select(ClinicalControl).where(
                ClinicalControl.db_identifier == clinvar_allele_id,
                ClinicalControl.db_version == version,
                ClinicalControl.db_name == "ClinVar",
            )
        ).one_or_none()
        if clinvar_variant:
            clinvar_variant.gene_symbol = variant_data.get("GeneSymbol")
            clinvar_variant.clinical_significance = variant_data.get("ClinicalSignificance")
            clinvar_variant.clinical_review_status = variant_data.get("ReviewStatus")
        else:
            clinvar_variant = ClinicalControl(
                db_identifier=clinvar_allele_id,
                gene_symbol=variant_data.get("GeneSymbol"),
                clinical_significance=variant_data.get("ClinicalSignificance"),
                clinical_review_status=variant_data.get("ReviewStatus"),
                db_version=version,
                db_name="ClinVar",
            )

        db.add(clinvar_variant)

        variants_with_clingen_allele_id = db.scalars(
            select(MappedVariant).where(MappedVariant.clingen_allele_id == clingen_id)
        ).all()
        for mapped_variant in variants_with_clingen_allele_id:
            if clinvar_variant.id in [c.id for c in mapped_variant.clinical_controls]:
                continue
            mapped_variant.clinical_controls.append(clinvar_variant)
            db.add(mapped_variant)

        db.commit()
        logger.debug(
            f"Added ClinVar variant data ({clinvar_allele_id}) for ClinGen allele ID {clingen_id}. ({index + 1}/{total_variants_with_clingen_ids})."
        )


@click.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--month", default=None, help="Populate mapped variants for every score set in MaveDB.")
@click.option("--year", default=None, help="Populate mapped variants for every score set in MaveDB.")
def refresh_clinvar_variants_command(db: Session, month: Optional[str], year: Optional[str], urns: Sequence[str]) -> None:
    refresh_clinvar_variants(db, month, year, urns)


if __name__ == "__main__":
    refresh_clinvar_variants_command()
