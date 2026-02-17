import asyncio
import logging

import requests
from aiocache import cached

from mavedb.lib.clingen.cache import CACHE_CLASS, CACHE_CONFIG, CACHE_TTL_SECONDS, clingen_cache_key_builder

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLINGEN_API_URL = "https://reg.genome.network/allele"


@cached(ttl=CACHE_TTL_SECONDS, key_builder=clingen_cache_key_builder, cache=CACHE_CLASS, **CACHE_CONFIG)
async def get_canonical_pa_ids(clingen_allele_id: str) -> list[str]:
    """Retrieve canonical PA IDs from the ClinGen API for a given ClinGen allele ID.

    Results are automatically cached for 24 hours using aiocache with configurable backend.
    This significantly reduces repeated API calls when processing multiple ClinVar control
    versions or running jobs that query the same alleles. Cache backend can be switched
    between Redis (production) and in-memory (testing) via CLINGEN_CACHE_BACKEND env var.

    Args:
        clingen_allele_id: ClinGen allele ID to query (e.g., CA123456)

    Returns:
        List of canonical PA IDs associated with the allele. Returns empty list if
        the allele has no MANE transcripts or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty list).
    """
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, requests.get, f"{CLINGEN_API_URL}/{clingen_allele_id}")

    # 404 means the allele doesn't exist in ClinGen's registry - treat as "no data" (cacheable)
    if response.status_code == 404:
        return []

    # All other non-2xx status codes raise exceptions (400, 429, 5xx, etc.)
    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()

    pa_ids = []
    if data.get("transcriptAlleles"):
        for allele in data["transcriptAlleles"]:
            if allele.get("MANE") and allele.get("@id"):
                # @id field returns url; the last component is the PA ID
                pa_ids.append(allele["@id"].split("/")[-1])

    return pa_ids


@cached(ttl=CACHE_TTL_SECONDS, key_builder=clingen_cache_key_builder, cache=CACHE_CLASS, **CACHE_CONFIG)
async def get_matching_registered_ca_ids(clingen_pa_id: str) -> list[str]:
    """Retrieve matching registered transcript CA IDs for a given PA ID from the ClinGen API.

    Results are automatically cached for 24 hours using aiocache with configurable backend.
    This significantly reduces repeated API calls when processing variant translations or
    running jobs that query the same protein alleles. Cache backend can be switched
    between Redis (production) and in-memory (testing) via CLINGEN_CACHE_BACKEND env var.

    Args:
        clingen_pa_id: ClinGen protein allele ID to query (e.g., PA123456)

    Returns:
        List of matching registered transcript CA IDs. Returns empty list if no
        matching transcripts are found or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty list).
    """
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, requests.get, f"{CLINGEN_API_URL}/{clingen_pa_id}")

    # 404 means the allele doesn't exist in ClinGen's registry - treat as "no data" (cacheable)
    if response.status_code == 404:
        return []

    # All other non-2xx status codes raise exceptions (400, 429, 5xx, etc.)
    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()

    ca_ids = []
    if data.get("aminoAcidAlleles"):
        for allele in data["aminoAcidAlleles"]:
            if allele.get("matchingRegisteredTranscripts"):
                # @id field returns URL; the last component is the transcript CA ID
                ca_ids.extend(
                    [transcript["@id"].split("/")[-1] for transcript in allele["matchingRegisteredTranscripts"]]
                )

    return ca_ids


@cached(ttl=CACHE_TTL_SECONDS, key_builder=clingen_cache_key_builder, cache=CACHE_CLASS, **CACHE_CONFIG)
async def get_associated_clinvar_allele_id(clingen_allele_id: str) -> str:
    """Retrieve the associated ClinVar Allele ID for a given ClinGen Allele ID.

    Results are automatically cached for 24 hours using aiocache with configurable backend.
    This significantly reduces repeated API calls when refreshing ClinVar controls across
    multiple months/years, as each job queries the same ClinGen allele IDs. Cache backend
    can be switched between Redis (production) and in-memory (testing) via the
    CLINGEN_CACHE_BACKEND environment variable.

    Note: Returns empty string when the API call succeeds but no ClinVar association exists,
    or when the allele doesn't exist in ClinGen's registry (404). This ensures successful
    negative results are cached, which is important since most ClinGen alleles don't have
    ClinVar associations. Other API errors (400, 429, 5xx) raise HTTPError, which prevents
    caching and allows retries for transient failures or surfaces issues like rate limiting.

    Args:
        clingen_allele_id: ClinGen allele ID to query (e.g., CA123456)

    Returns:
        Associated ClinVar allele ID as a string, or empty string if no association exists
        or if the allele doesn't exist (404).

    Raises:
        requests.exceptions.HTTPError: If the API request fails with non-2xx status code
            (excluding 404, which returns empty string).
    """
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, requests.get, f"{CLINGEN_API_URL}/{clingen_allele_id}")

    # 404 means the allele doesn't exist in ClinGen's registry - treat as "no data" (cacheable)
    if response.status_code == 404:
        return ""

    # All other non-2xx status codes raise exceptions (400, 429, 5xx, etc.)
    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()
    clinvar_allele_id = data.get("externalRecords", {}).get("ClinVarAlleles", [{}])[0].get("alleleId")
    if clinvar_allele_id:
        return str(clinvar_allele_id)

    return ""
