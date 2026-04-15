"""VEP (Variant Effect Predictor) library functions for functional consequence prediction."""

import logging
from typing import Optional, Sequence

import requests


logger = logging.getLogger(__name__)

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
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "disruptive_inframe_insertion",
    "disruptive_inframe_deletion",
    "protein_altering_variant",
    "splice_region_variant",
    "incomplete_terminal_codon_variant",
    "start_retained",
    "stop_retained",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_premature_start_codon_gain_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "non_coding_exon_variant",
    "non_coding_transcript_variant",
    "nc_transcript_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "enhancer_ablation",
    "regulatory_region_amplification",
    "enhancer_amplification",
    "regulatory_region_variant",
    "feature_elongation",
    "regulatory_region",
    "TFBS",
    "feature_truncation",
    "exon_variant",
    "disruptive_inframe_deletion",
    "gene_variant",
    "variant_affecting_coding_sequence_conservation",
    "variant_affecting_genome_assembly_quality",
    "variant_of_unknown_significance",
    "sequence_variant",
    "rare_amino_acid_variant",
    "splice_region_variant",
    "downstream_gene_variant",
    "upstream_gene_variant",
    "intron_variant",
    "intergenic_variant",
]


def run_variant_recoder(missing_hgvs: Sequence[str]) -> dict[str, list[str]]:
    """Call the Variant Recoder API and return a mapping from input HGVS strings to genomic HGVS strings.

    Args:
        missing_hgvs (Sequence[str]): List of HGVS strings to recode.

    Returns:
        dict[str, list[str]]: Mapping of input HGVS to list of genomic HGVS strings (hgvsg).

    Raises:
        VEPProcessingError: If the API request fails.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    recoder_response = requests.post(
        f"{ENSEMBL_API_URL}/variant_recoder/human",
        headers=headers,
        json={"ids": list(missing_hgvs)},
    )
    hgvs_to_genomic: dict[str, list[str]] = {}
    if recoder_response.status_code == 200:
        recoder_data = recoder_response.json()
        for entry in recoder_data:
            hgvs_string = entry.get("input")
            if not hgvs_string:
                continue
            genomic_hgvs_list = []
            for variant, variant_data in entry.items():
                if variant == "input":
                    continue
                genomic_strings = variant_data.get("hgvsg") if isinstance(variant_data, dict) else None
                if genomic_strings:
                    for genomic_hgvs in genomic_strings:
                        if genomic_hgvs.startswith("NC_"):
                            genomic_hgvs_list.append(genomic_hgvs)
            if genomic_hgvs_list:
                hgvs_to_genomic[hgvs_string] = genomic_hgvs_list
    else:
        logger.error(
            f"Failed batch Variant Recoder API request: {recoder_response.status_code} {recoder_response.text}"
        )
    return hgvs_to_genomic


def get_functional_consequence(hgvs_strings: Sequence[str]) -> dict[str, Optional[str]]:
    """Get VEP functional consequences for a batch of HGVS strings.

    Submits HGVS strings to the Ensembl VEP API and retrieves functional consequence
    predictions. For any HGVS strings not found in the initial VEP response, attempts
    to recode them using Variant Recoder and retries with VEP.

    Args:
        hgvs_strings (Sequence[str]): List of HGVS strings to process (max 200 per call).

    Returns:
        dict[str, Optional[str]]: Mapping of HGVS string to functional consequence.
                                  If no consequence found, maps to None.

    Raises:
        VEPProcessingError: If VEP API processing fails critically.
    """
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
        for entry in data:
            hgvs = entry.get("input")
            most_severe_consequence = entry.get("most_severe_consequence")
            if hgvs:
                result[hgvs] = most_severe_consequence
                missing_hgvs.discard(hgvs)
    else:
        logger.error(f"Failed batch VEP API request: {response.status_code} {response.text}")
        # raise VEPBatchError(f"Batch VEP API request failed with status {response.status_code}")

    # TODO add in retry logic for transient errors (e.g. 500 or 503) with exponential backoff
    # if batch fails after all retries, add annotation statuses for all variants in that batch as failed

    # Fallback for missing HGVS strings
    if missing_hgvs:
        hgvs_to_genomic = run_variant_recoder(list(missing_hgvs))
        # Assign None for any missing_hgvs not present in recoder response
        for hgvs_string in missing_hgvs:
            if hgvs_string not in hgvs_to_genomic:
                result[hgvs_string] = None

        # Collect all genomic HGVS strings for VEP
        genomic_hgvs_map = {hgvs: hgvs_to_genomic[hgvs] for hgvs in hgvs_to_genomic}
        all_genomic_hgvs = []
        hgvs_genomic_lookup = {}
        for hgvs, genomics in genomic_hgvs_map.items():
            for g in genomics:
                all_genomic_hgvs.append(g)
                hgvs_genomic_lookup.setdefault(hgvs, []).append(g)

        # Run VEP in batches of 200
        vep_results: dict[str, list[str]] = {}
        for i in range(0, len(all_genomic_hgvs), 200):
            batch = all_genomic_hgvs[i : i + 200]
            vep_response = requests.post(
                f"{ENSEMBL_API_URL}/vep/human/hgvs",
                headers=headers,
                json={"hgvs_notations": batch},
            )
            if vep_response.status_code != 200:
                logger.error(f"Failed batch VEP for genomic HGVS: {vep_response.status_code}")
                continue
            vep_data = vep_response.json()
            for entry in vep_data:
                genomic_input = entry.get("input")
                most_severe_consequence = entry.get("most_severe_consequence")
                if genomic_input and most_severe_consequence:
                    vep_results.setdefault(genomic_input, []).append(most_severe_consequence)

        # For each original missing_hgvs, choose the most severe consequence among its genomics
        for hgvs, genomics in hgvs_genomic_lookup.items():
            consequences = []
            for g in genomics:
                consequences.extend(vep_results.get(g, []))
            if consequences:
                for consequence in VEP_CONSEQUENCES:
                    if consequence in consequences:
                        result[hgvs] = consequence
                        break
                else:
                    result[hgvs] = None
            else:
                result[hgvs] = None

    return result
