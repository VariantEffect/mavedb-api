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


def get_functional_consequence(hgvs_strings: Sequence[str]) -> dict[str, Optional[str]]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    result: dict[str, Optional[str]] = {}

    # Batch POST to VEP
    response = requests.post(
        f"{ENSEMBL_API_URL}/vep/human/hgvs",
        headers=headers,
        json={"hgvs_notations": hgvs_strings},
    )

    missing_hgvs = set(hgvs_strings)
    if response.status_code == 200:
        data = response.json()
        # Map HGVS to consequence
        for entry in data:
            hgvs = entry.get("input")
            most_severe_consequence = entry.get("most_severe_consequence")
            if hgvs:
                result[hgvs] = most_severe_consequence
                missing_hgvs.discard(hgvs)
    else:
        logger.error(f"Failed batch VEP API request: {response.status_code} {response.text}")

    # Fallback for missing HGVS strings: batch POST to Variant Recoder
    if missing_hgvs:
        recoder_response = requests.post(
            f"{ENSEMBL_API_URL}/variant_recoder/human",
            headers=headers,
            json={"variants": list(missing_hgvs)},
        )
        recoder_found = set()
        if recoder_response.status_code == 200:
            recoder_data = recoder_response.json()
            # recoder_data is a list of dicts, each with "input" and variant info
            for entry in recoder_data:
                hgvs_string = entry.get("input")
                if not hgvs_string:
                    continue
                recoder_found.add(hgvs_string)
                variants = []
                for variant, variant_data in entry.items():
                    genomic_strings = variant_data.get("hgvsg")
                    if genomic_strings:
                        for genomic_hgvs in genomic_strings:
                            if genomic_hgvs.startswith("NC_"):
                                variants.append(genomic_hgvs)
                if not variants:
                    logger.error(f"No genomic HGVS found in Variant Recoder response for {hgvs_string}.")
                    result[hgvs_string] = None
                    continue
                consequences = []
                for genomic_hgvs in variants:
                    vep_response = requests.get(f"{ENSEMBL_API_URL}/vep/human/hgvs/{genomic_hgvs}", headers=headers)
                    if vep_response.status_code != 200:
                        logger.error(f"Failed VEP for {genomic_hgvs}: {vep_response.status_code}")
                        # if any of the genomic variants fail, return None, because we don't want to miss the most severe consequence
                        result[hgvs_string] = None
                        break
                    vep_data = vep_response.json()
                    if isinstance(vep_data, list) and len(vep_data) == 1:
                        most_severe_consequence = vep_data[0].get("most_severe_consequence")
                        if most_severe_consequence:
                            consequences.append(most_severe_consequence)
                        else:
                            logger.error(f"No consequence returned from VEP for {genomic_hgvs}")
                            result[hgvs_string] = None
                            break
                    else:
                        logger.error(f"Unexpected VEP format for {genomic_hgvs}: {vep_data}")
                        result[hgvs_string] = None
                        break
                if consequences:
                    # Return the most severe consequence among all genomic variants
                    for consequence in VEP_CONSEQUENCES:
                        if consequence in consequences:
                            result[hgvs_string] = consequence
                            break
                    else:
                        result[hgvs_string] = None
                else:
                    logger.error(f"No consequences found for any genomic variants derived from {hgvs_string}.")
                    result[hgvs_string] = None
        else:
            logger.error(
                f"Failed batch Variant Recoder API request: {recoder_response.status_code} {recoder_response.text}"
            )

        # Assign None for any missing_hgvs not present in recoder_found
        for hgvs_string in missing_hgvs:
            if hgvs_string not in recoder_found:
                result[hgvs_string] = None

    return result


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

            queue = []
            variant_map = {}
            for mapped_variant in mapped_variants:
                hgvs_string = mapped_variant.post_mapped.get("expressions", {})[0].get("value")  # type: ignore
                if not hgvs_string:
                    logger.warning(f"No HGVS string found in post_mapped for variant {mapped_variant.id}.")
                    continue
                queue.append(hgvs_string)
                variant_map[hgvs_string] = mapped_variant

                if len(queue) == 200:
                    consequences = get_functional_consequence(queue)
                    for hgvs, consequence in consequences.items():
                        mapped_variant = variant_map[hgvs]
                        if consequence:
                            mapped_variant.vep_functional_consequence = consequence
                            mapped_variant.vep_access_date = date.today()
                            db.add(mapped_variant)
                        else:
                            logger.warning(f"Could not retrieve functional consequence for HGVS {hgvs}.")
                    db.commit()
                    queue.clear()
                    variant_map.clear()

            # Process any remaining variants in the queue
            if queue:
                consequences = get_functional_consequence(queue)
                for hgvs, consequence in consequences.items():
                    mapped_variant = variant_map[hgvs]
                    if consequence:
                        mapped_variant.vep_functional_consequence = consequence
                        mapped_variant.vep_access_date = date.today()
                        db.add(mapped_variant)
                    else:
                        logger.warning(f"Could not retrieve functional consequence for HGVS {hgvs}.")
                db.commit()

        except Exception as e:
            logger.error(
                f"Failed to populate functional consequence predictions for score set {score_set.urn}: {str(e)}"
            )
            db.rollback()

    logger.info("Done populating functional consequence predictions.")


if __name__ == "__main__":
    populate_functional_consequences()
