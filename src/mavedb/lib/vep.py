"""VEP (Variant Effect Predictor) library functions for functional consequence prediction."""

import asyncio
import functools
import logging
import os
from datetime import date
from enum import Enum
from typing import Mapping, NamedTuple, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.utils import request_with_backoff
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

logger = logging.getLogger(__name__)


ENSEMBL_API_URL = os.environ.get("ENSEMBL_API_URL", "https://rest.ensembl.org")

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
"""
List of all functional consequences VEP can return, in order of severity (most severe first).
"""


class VepLinkVerdict(str, Enum):
    """Per-allele outcome of a VEP linking run, returned for every allele whose status is decided.

    The single source of truth for what happened to an allele's consequence this run — the caller
    derives annotation status from this, never by re-querying consequence state. An allele absent from
    the map had no live consequence and resolved none this run (the caller reads that as "no result").

    - ``CREATED`` — a new or changed consequence was created/superseded this run.
    - ``UNCHANGED`` — a live consequence was retained (value matched, or held against a null run).
    """

    CREATED = "created"
    UNCHANGED = "unchanged"


class VepResolution(NamedTuple):
    """Outcome of resolving a set of HGVS strings, splitting the two kinds of "no consequence".

    This outcome allows us to differentiate between a genuine empty (VEP found nothing) and an
    unknown (VEP failed to answer).

    - ``consequences`` — HGVS that resolved to a most-severe consequence (the hits).
    - ``errored`` — HGVS whose VEP/Recoder request *failed* (HTTP/transport error after retries); the
      result is unknown and the allele should be retried, not treated as a negative.
    - Any queried HGVS in neither set was answered (HTTP 200) with no consequence — a genuine **empty**.
    """

    consequences: dict[str, str]
    errored: set[str]


async def run_variant_recoder(missing_hgvs: Sequence[str]) -> dict[str, list[str]]:
    """Call the Variant Recoder API and return a mapping from input HGVS strings to genomic HGVS strings.

    Args:
        missing_hgvs (Sequence[str]): List of HGVS strings to recode.

    Returns:
        dict[str, list[str]]: Mapping of input HGVS to list of genomic HGVS strings (hgvsg). An input
                              with no recodable genomic mapping is simply absent (a genuine empty).

    Raises:
        requests.exceptions.RequestException: if the Recoder request fails (HTTP/transport error after
            retries). The caller attributes the failure to this batch's inputs so they are reported as
            errored (unknown, retry) rather than silently conflated with a genuine empty.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # request_with_backoff is synchronous (requests lib + time.sleep backoff); run_in_executor
    # keeps the event loop free during the full request + any retry wait time.
    loop = asyncio.get_running_loop()
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
        dict[str, Optional[str]]: Mapping of HGVS string to functional consequence. An HGVS the
                                  successful response carried no consequence for maps to None (a genuine
                                  miss — the caller may try Recoder, else treats it as empty).

    Raises:
        requests.exceptions.RequestException: if the VEP request fails (HTTP/transport error after
            retries). The caller attributes the failure to this batch's inputs so they are reported as
            errored (unknown, retry) rather than silently conflated with a genuine empty.
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

    data = response.json()
    for entry in data:
        hgvs = entry.get("input")
        most_severe_consequence = entry.get("most_severe_consequence")
        if hgvs:
            result[hgvs] = most_severe_consequence

    return result


async def get_ensembl_release() -> str:
    """Return the current Ensembl release the REST API is serving, e.g. ``"116"`` (``/info/software``).

    An Ensembl release is coordinated — software, transcript set, and consequence vocabulary all bump
    together under one number — so this single value version-keys VEP results the way gnomAD keys on its
    data version. The job stamps it on each consequence and skips re-querying alleles already live at the
    current release. Raises on failure: the version is load-bearing for the skip, so a job that cannot
    determine it must abort rather than mis-version its writes.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(
        None,
        functools.partial(
            request_with_backoff,
            method="GET",
            url=f"{ENSEMBL_API_URL}/info/software",
            headers=headers,
            timeout=30,
        ),
    )
    return str(response.json()["release"])


def link_vep_consequences_to_alleles(
    db: Session,
    consequence_by_allele_id: Mapping[int, Optional[str]],
    *,
    source_version: str,
    access_date: date,
) -> dict[int, VepLinkVerdict]:
    """Store VEP consequences against deduplicated alleles, superseding only on change.

    ``consequence_by_allele_id`` maps each queried allele to the consequence VEP resolved this run
    (``None`` when VEP + Variant Recoder found nothing). ``source_version`` is the Ensembl release the
    run resolved against. Each allele holds at most one live :class:`VepAlleleConsequence`, handled per
    allele:

    - **unchanged** (live row already carries this consequence): advance ``source_version`` and
      ``access_date`` in place — no supersede. Supersede is value-keyed, not version-keyed: a new
      release that resolves the same categorical consequence must not fabricate a transaction-time
      boundary, which would churn history every release.
    - **new or changed** (no live row, or a different consequence): supersede keyed on ``allele_id``
      (retire the old, insert the successor stamped with this ``source_version``/``access_date``).
    - **None this run**: leave any live row in place — do not overwrite a held consequence with a null result.
      Log a warning if VEP found no consequence for an allele which previously had a live consequence.

    Does not commit. Returns a verdict per allele whose status is decided this run:
    :attr:`VepLinkVerdict.CREATED` for a created/superseded consequence, :attr:`~VepLinkVerdict.UNCHANGED`
    for a live consequence retained (value matched, or held against a null run). An allele absent from
    the map had no live row and resolved nothing — the caller reads that as "no result". This is the
    single source of truth for per-allele status; callers must not re-derive it from consequence state.
    """
    save_to_logging_context({"num_alleles_to_link_vep": len(consequence_by_allele_id)})
    logger.debug(msg="Linking VEP consequences to alleles", extra=logging_context())

    verdicts: dict[int, VepLinkVerdict] = {}
    for allele_id, consequence in consequence_by_allele_id.items():
        live = db.scalar(
            select(VepAlleleConsequence).where(
                VepAlleleConsequence.allele_id == allele_id,
                VepAlleleConsequence.current,
            )
        )

        # TODO#780 - VEP found nothing this run. Do not overwrite a held consequence with a null result; a retained
        # consequence is UNCHANGED (status preexisting), while no live row at all leaves the allele out of
        # the map (the caller reads that as a no-result).
        if consequence is None:
            if live is not None:
                logger.warning(
                    f"VEP found no consequence for allele {allele_id} this run; leaving prior consequence "
                    f"'{live.functional_consequence}' in place.",
                    extra=logging_context(),
                )
                verdicts[allele_id] = VepLinkVerdict.UNCHANGED

            continue

        # Unchanged: advance version/freshness in place.
        if live is not None and live.functional_consequence == consequence:
            live.source_version = source_version
            live.access_date = access_date
            verdicts[allele_id] = VepLinkVerdict.UNCHANGED
            continue

        # New or changed consequence: retire any live row, insert the successor.
        VepAlleleConsequence.supersede_live_where(
            db,
            [
                VepAlleleConsequence(
                    allele_id=allele_id,
                    functional_consequence=consequence,
                    source_version=source_version,
                    access_date=access_date,
                )
            ],
            VepAlleleConsequence.allele_id == allele_id,
        )
        verdicts[allele_id] = VepLinkVerdict.CREATED

    changed_allele_count = sum(1 for v in verdicts.values() if v is VepLinkVerdict.CREATED)
    save_to_logging_context({"changed_allele_count": changed_allele_count})
    logger.info(
        msg=f"Created or superseded {changed_allele_count} VEP allele consequences this run.",
        extra=logging_context(),
    )
    return verdicts
