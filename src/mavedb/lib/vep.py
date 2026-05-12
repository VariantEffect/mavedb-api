"""VEP (Variant Effect Predictor) library functions for functional consequence prediction."""

import asyncio
import functools
import logging
import os
from typing import Optional, Sequence

import requests

from mavedb.lib.utils import request_with_backoff

logger = logging.getLogger(__name__)

ENSEMBL_API_URL = os.environ.get("ENSEMBL_API_URL", "https://rest.ensembl.org")

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


async def run_variant_recoder(missing_hgvs: Sequence[str]) -> dict[str, list[str]]:
    """Call the Variant Recoder API and return a mapping from input HGVS strings to genomic HGVS strings.

    Args:
        missing_hgvs (Sequence[str]): List of HGVS strings to recode.

    Returns:
        dict[str, list[str]]: Mapping of input HGVS to list of genomic HGVS strings (hgvsg).
                              Returns an empty dict if Ensembl rejects the batch (e.g. 400 for
                              unrecognised identifiers) — callers treat missing entries as failures.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    # request_with_backoff is synchronous (requests lib + time.sleep backoff); run_in_executor
    # keeps the event loop free during the full request + any retry wait time.
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(
            None,
            functools.partial(
                request_with_backoff,
                method="POST",
                url=f"{ENSEMBL_API_URL}/variant_recoder/human",
                headers=headers,
                json={"ids": list(missing_hgvs)},
                timeout=600,  # Variant Recoder can be very slow for large batches and 504s are common; generous timeout and backoff retries are needed
            ),
        )
    except requests.exceptions.HTTPError as exc:
        # A 4xx from Ensembl (e.g. 400 for an unrecognised identifier format) means the batch
        # cannot be recoded.  Return empty so callers can handle these missing entries.
        logger.warning(
            f"Variant Recoder returned {exc.response.status_code if exc.response is not None else 'unknown'} "
            f"for batch of {len(missing_hgvs)} HGVS strings — treating as no results.",
            exc_info=exc,
        )
        return {}

    data = response.json()
    # request_with_backoff handles http errors, so no need to check response status
    hgvs_to_genomic: dict[str, list[str]] = {}
    for input_variant in data:
        for variant_str, variant_data in input_variant.items():
            hgvs_string = variant_data.get("input") if isinstance(variant_data, dict) else None
            if variant_str == "input" or not hgvs_string:
                continue
            genomic_strings = variant_data.get("hgvsg") if isinstance(variant_data, dict) else None
            if genomic_strings:
                for genomic_hgvs in genomic_strings:
                    if genomic_hgvs.startswith("NC_"):
                        hgvs_to_genomic.setdefault(hgvs_string, []).append(genomic_hgvs)
    return hgvs_to_genomic


async def get_functional_consequence(hgvs_strings: Sequence[str]) -> dict[str, Optional[str]]:
    """Get VEP functional consequences for a batch of HGVS strings.

    Submits HGVS strings to the Ensembl VEP API and retrieves functional consequence
    predictions. For any HGVS strings not found in the initial VEP response, attempts
    to recode them using Variant Recoder and retries with VEP.

    Args:
        hgvs_strings (Sequence[str]): List of HGVS strings to process (max 200 per call).

    Returns:
        dict[str, Optional[str]]: Mapping of HGVS string to functional consequence.
                                  If no consequence found, maps to None.  Returns an empty dict
                                  if Ensembl rejects the batch (e.g. 400 for unrecognised
                                  identifiers) — callers treat missing entries as needing Recoder
                                  fallback or as failures.
    """
    if len(hgvs_strings) > 200:
        raise ValueError(
            "VEP API can process a maximum of 200 HGVS strings per request. This function does not handle batching."
        )

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    result: dict[str, Optional[str]] = {}

    # request_with_backoff is synchronous (requests lib + time.sleep backoff); run_in_executor
    # keeps the event loop free during the full request + any retry wait time.
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(
            None,
            functools.partial(
                request_with_backoff,
                method="POST",
                url=f"{ENSEMBL_API_URL}/vep/human/hgvs",
                headers=headers,
                json={"hgvs_notations": list(hgvs_strings)},
                timeout=60,  # VEP can be slow for large batches.
            ),
        )
    except requests.exceptions.HTTPError as exc:
        # A 4xx from Ensembl (e.g. 400 for an unrecognised identifier) means the batch cannot
        # be resolved.  Return empty so the callers can handle these missing entries.
        logger.warning(
            f"VEP returned {exc.response.status_code if exc.response is not None else 'unknown'} "
            f"for batch of {len(hgvs_strings)} HGVS strings — treating as no results.",
            exc_info=exc,
        )
        return result

    data = response.json()
    for entry in data:
        hgvs = entry.get("input")
        most_severe_consequence = entry.get("most_severe_consequence")
        if hgvs:
            result[hgvs] = most_severe_consequence

    return result
