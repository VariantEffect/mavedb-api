import asyncio
import logging
from typing import Optional

import requests
from aiocache import cached

from mavedb.lib.clingen.cache import CACHE_CLASS, CACHE_CONFIG, CACHE_TTL_SECONDS, clingen_cache_key_builder

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLINGEN_API_URL = "https://reg.genome.network/allele"


@cached(ttl=CACHE_TTL_SECONDS, key_builder=clingen_cache_key_builder, cache=CACHE_CLASS, **CACHE_CONFIG)
async def get_clingen_allele_data(clingen_allele_id: str) -> Optional[dict]:
    """Retrieve full allele data from the ClinGen Allele Registry.

    Results are automatically cached for 24 hours using aiocache with configurable backend.
    If the cache backend is unavailable, aiocache falls through to the real API call rather
    than raising — cache read failures are treated as misses, write failures are logged and
    ignored. The function always returns a result as long as the ClinGen API itself is reachable.

    Args:
        clingen_allele_id: ClinGen allele ID to query (e.g., CA123456 or PA123456).

    Returns:
        Full JSON response from the ClinGen API, or None if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns None).
    """
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, requests.get, f"{CLINGEN_API_URL}/{clingen_allele_id}")

    if response.status_code == 404:
        return None

    if response.status_code != 200:
        response.raise_for_status()

    return response.json()


async def get_canonical_pa_ids(clingen_allele_id: str) -> list[str]:
    """Retrieve canonical PA IDs from the ClinGen API for a given ClinGen allele ID.

    Uses the cached allele data from `get_clingen_allele_data` to avoid redundant API calls.

    Args:
        clingen_allele_id: ClinGen allele ID to query (e.g., CA123456)

    Returns:
        List of canonical PA IDs associated with the allele. Returns empty list if
        the allele has no MANE transcripts or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty list).
    """
    data = await get_clingen_allele_data(clingen_allele_id)
    if data is None:
        return []

    pa_ids = []
    if data.get("transcriptAlleles"):
        for allele in data["transcriptAlleles"]:
            if allele.get("MANE") and allele.get("@id"):
                # @id field returns url; the last component is the PA ID
                pa_ids.append(allele["@id"].split("/")[-1])

    return pa_ids


async def get_matching_registered_ca_ids(clingen_pa_id: str) -> list[str]:
    """Retrieve matching registered transcript CA IDs for a given PA ID from the ClinGen API.

    Uses the cached allele data from `get_clingen_allele_data` to avoid redundant API calls.

    Args:
        clingen_pa_id: ClinGen protein allele ID to query (e.g., PA123456)

    Returns:
        List of matching registered transcript CA IDs. Returns empty list if no
        matching transcripts are found or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty list).
    """
    data = await get_clingen_allele_data(clingen_pa_id)
    if data is None:
        return []

    ca_ids = []
    if data.get("aminoAcidAlleles"):
        for allele in data["aminoAcidAlleles"]:
            if allele.get("matchingRegisteredTranscripts"):
                # @id field returns URL; the last component is the transcript CA ID
                ca_ids.extend(
                    [transcript["@id"].split("/")[-1] for transcript in allele["matchingRegisteredTranscripts"]]
                )

    return ca_ids


async def get_associated_clinvar_allele_id(clingen_allele_id: str) -> str:
    """Retrieve the associated ClinVar Allele ID for a given ClinGen Allele ID.

    Uses the cached allele data from `get_clingen_allele_data` to avoid redundant API calls.

    Returns empty string when no ClinVar association exists or when the allele doesn't exist
    in ClinGen's registry (404).

    Args:
        clingen_allele_id: ClinGen allele ID to query (e.g., CA123456)

    Returns:
        Associated ClinVar allele ID as a string, or empty string if no association exists
        or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty string).
    """
    data = await get_clingen_allele_data(clingen_allele_id)
    if data is None:
        return ""

    clinvar_allele_id = data.get("externalRecords", {}).get("ClinVarAlleles", [{}])[0].get("alleleId")
    if clinvar_allele_id:
        return str(clinvar_allele_id)

    return ""


def extract_hgvs_from_ca_allele_data(
    data: dict,
    target_is_coding: bool,
    transcript_accession: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract HGVS strings from ClinGen allele data for a CA (canonical allele) ID.

    Parses the ClinGen API response to find GRCh38 genomic HGVS, coding HGVS
    matching the target transcript (or MANE fallback), and protein HGVS.

    Args:
        data: Parsed JSON response from the ClinGen Allele Registry API.
        target_is_coding: Whether the score set target is protein-coding.
        transcript_accession: Specific transcript accession to match, or None to use MANE.

    Returns:
        Tuple of (hgvs_g, hgvs_c, hgvs_p), any of which may be None.
    """
    hgvs_g: Optional[str] = None
    hgvs_c: Optional[str] = None
    hgvs_p: Optional[str] = None

    if data.get("genomicAlleles"):
        for allele in data["genomicAlleles"]:
            if allele.get("referenceGenome") == "GRCh38" and allele.get("hgvs"):
                hgvs_g = allele["hgvs"][0]
                break

    if target_is_coding and data.get("transcriptAlleles"):
        if transcript_accession:
            for allele in data["transcriptAlleles"]:
                if allele.get("hgvs"):
                    for hgvs_string in allele["hgvs"]:
                        hgvs_reference_sequence = hgvs_string.split(":")[0]
                        if transcript_accession == hgvs_reference_sequence:
                            hgvs_c = hgvs_string
                            break
                if hgvs_c:
                    if allele.get("proteinEffect"):
                        hgvs_p = allele["proteinEffect"].get("hgvs")
                    break
        else:
            # No transcript specified; use MANE if available
            for allele in data["transcriptAlleles"]:
                if allele.get("MANE"):
                    hgvs_c = allele["MANE"].get("nucleotide", {}).get("RefSeq", {}).get("hgvs")
                    hgvs_p = allele["MANE"].get("protein", {}).get("RefSeq", {}).get("hgvs")
                    break

    return hgvs_g, hgvs_c, hgvs_p


def extract_hgvs_from_pa_allele_data(data: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract HGVS strings from ClinGen allele data for a PA (protein allele) ID.

    For PA alleles, only hgvs_p is extracted from aminoAcidAlleles.

    Args:
        data: Parsed JSON response from the ClinGen Allele Registry API.

    Returns:
        Tuple of (None, None, hgvs_p), where hgvs_p may be None.
    """
    hgvs_p: Optional[str] = None

    if data.get("aminoAcidAlleles"):
        for allele in data["aminoAcidAlleles"]:
            if allele.get("hgvs"):
                hgvs_p = allele["hgvs"][0]
                break

    return None, None, hgvs_p


def expand_allele_ids(clingen_allele_ids: list[Optional[str]]) -> set[str]:
    """Expand comma-separated multi-variant ClinGen allele IDs into individual IDs.

    Multi-variant alleles may contain multiple comma-separated ClinGen IDs.
    This function normalizes them into individual IDs for independent processing.
    """
    expanded: set[str] = set()
    for allele_id in clingen_allele_ids:
        if not allele_id:
            continue
        if "," in allele_id:
            expanded.update(single_id.strip() for single_id in allele_id.split(","))
        else:
            expanded.add(allele_id)
    return expanded
