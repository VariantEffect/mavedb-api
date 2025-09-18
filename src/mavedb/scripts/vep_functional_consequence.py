import logging
import requests
from datetime import date
from typing import Sequence, Optional

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ENSEMBL_API_URL = "https://rest.ensembl.org"

# List of all possible VEP consequences, in order from most to least severe
VEP_CONSEQUENCES = [
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "transcript_amplification",
    "feature_elongation",
    "feature_truncation",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "splice_donor_5th_base_variant",
    "splice_region_variant",
    "splice_donor_region_variant",
    "splice_polypyrimidine_tract_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "coding_transcript_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "regulatory_region_variant",
    "intergenic_variant",
    "sequence_variant",
]


def get_functional_consequence(hgvs_string: str) -> Optional[str]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    response = requests.get(f"{ENSEMBL_API_URL}/vep/human/hgvs/{hgvs_string}", headers=headers)
    # Nonsense and synonymous protein variants can't be processed directly by VEP, need to be run through Variant Recoder first
    if response.status_code == 400:
        logger.warning(
            f"VEP API could not process HGVS string {hgvs_string}: {response.text}. Retrieving genomic variant(s) from Variant Recoder."
        )
        variant_recoder_response = requests.get(
            f"{ENSEMBL_API_URL}/variant_recoder/human/{hgvs_string}", headers=headers
        )
        if variant_recoder_response.status_code != 200:
            logger.error(
                f"Failed to query Variant Recoder API for {hgvs_string}: {variant_recoder_response.status_code}"
            )
            return None
        recoder_data = variant_recoder_response.json()
        if isinstance(recoder_data, list) and len(recoder_data) == 1:
            variants = []
            for variant, variant_data in recoder_data[0].items():
                genomic_strings = variant_data.get("hgvsg")
                for genomic_hgvs in genomic_strings:
                    if genomic_hgvs.startswith("NC_"):
                        variants.append(genomic_hgvs)
            if not variants:
                logger.error(f"No genomic HGVS strings found in Variant Recoder response for {hgvs_string}.")
                return None
            consequences = []
            for genomic_hgvs in variants:
                vep_response = requests.get(f"{ENSEMBL_API_URL}/vep/human/hgvs/{genomic_hgvs}", headers=headers)
                if vep_response.status_code != 200:
                    logger.error(f"Failed to query VEP API for {genomic_hgvs}: {vep_response.status_code}")
                    # if any of the genomic variants fail, return None, because we don't want to miss the most severe consequence
                    return None
                vep_data = vep_response.json()
                if isinstance(vep_data, list) and len(vep_data) == 1:
                    most_severe_consequence = vep_data[0].get("most_severe_consequence")
                    if most_severe_consequence:
                        consequences.append(most_severe_consequence)
                    else:
                        logger.error(f"No most_severe_consequence found in VEP response for {genomic_hgvs}.")
                        # if any of the genomic variants fail, return None, because we don't want to miss the most severe consequence
                        return None
                else:
                    logger.error(f"Unexpected response format from VEP API for {genomic_hgvs}: {vep_data}")
                    return None
            if consequences:
                # Return the most severe consequence among all genomic variants
                for consequence in VEP_CONSEQUENCES:
                    if consequence in consequences:
                        return consequence
            else:
                logger.error(f"No consequences found for any genomic variants derived from {hgvs_string}.")
                return None
        else:
            logger.error(f"Unexpected response format from Variant Recoder API for {hgvs_string}: {recoder_data}")
            return None

    elif response.status_code != 200:
        logger.error(f"Failed to query VEP API for {hgvs_string}: {response.status_code}")
        return None

    data = response.json()
    if isinstance(data, list) and len(data) == 1:
        most_severe_consequence = data[0].get("most_severe_consequence")
        return most_severe_consequence
    else:
        logger.error(f"Unexpected response format from VEP API for {hgvs_string}: {data}")

    return None


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate functional consequence predictions for every score set in MaveDB.", is_flag=True)
def populate_functional_consequences(db: Session, urns: Sequence[Optional[str]], all: bool):
    score_set_ids: Sequence[Optional[int]]
    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will populate functional consequence predictions for {len(score_set_ids)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(
            f"Populating functional consequence predictions for the provided score sets ({len(score_set_ids)})."
        )

    for ss_id in score_set_ids:
        if not ss_id:
            continue

        score_set = db.scalar(select(ScoreSet).where(ScoreSet.id == ss_id))
        if not score_set:
            logger.warning(f"Could not fetch score set with id={ss_id}.")
            continue

        try:
            mapped_variants = db.scalars(
                select(MappedVariant)
                .join(Variant)
                .where(
                    Variant.score_set_id == ss_id,
                    MappedVariant.current.is_(True),
                    MappedVariant.post_mapped.isnot(None),
                )
            ).all()

            if not mapped_variants:
                logger.info(f"No mapped variant post-mapped objects found for score set {score_set.urn}.")
                continue
            for mapped_variant in mapped_variants:
                hgvs_string = mapped_variant.post_mapped.get("expressions", {})[0].get("value")  # type: ignore
                if not hgvs_string:
                    logger.warning(f"No HGVS string found in post_mapped for variant {mapped_variant.id}.")
                    continue
                functional_consequence = get_functional_consequence(hgvs_string)
                if functional_consequence:
                    mapped_variant.vep_functional_consequence = functional_consequence
                    mapped_variant.vep_access_date = date.today()
                    db.add(mapped_variant)
                    db.commit()
                else:
                    logger.warning(f"Could not retrieve functional consequence for HGVS {hgvs_string}.")

        except Exception as e:
            logger.error(
                f"Failed to populate functional consequence predictions for score set {score_set.urn}: {str(e)}"
            )
            db.rollback()

    logger.info("Done populating functional consequence predictions.")


if __name__ == "__main__":
    populate_functional_consequences()
