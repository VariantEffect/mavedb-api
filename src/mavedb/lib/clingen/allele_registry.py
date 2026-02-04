import logging

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLINGEN_API_URL = "https://reg.genome.network/allele"


def get_canonical_pa_ids(clingen_allele_id: str) -> list[str]:
    """ "Retrieve any canonical PA IDs from the ClinGen API for a given clingen allele ID."""
    response = requests.get(f"{CLINGEN_API_URL}/{clingen_allele_id}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {clingen_allele_id}: {response.status_code}")
        return []

    data = response.json()

    pa_ids = []
    if data.get("transcriptAlleles"):
        for allele in data["transcriptAlleles"]:
            if allele.get("MANE") and allele.get("@id"):
                # @id field returns url; the last component is the PA ID
                pa_ids.append(allele["@id"].split("/")[-1])

    return pa_ids


def get_matching_registered_ca_ids(clingen_pa_id: str) -> list[str]:
    """Retrieve all matching registered transcript CA IDs for a given PA ID from the ClinGen API."""
    response = requests.get(f"{CLINGEN_API_URL}/{clingen_pa_id}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {clingen_pa_id}: {response.status_code}")
        return []

    data = response.json()

    ca_ids = []
    if data.get("aminoAcidAlleles"):
        for allele in data["aminoAcidAlleles"]:
            if allele.get("matchingRegisteredTranscripts"):
                # @id field returns url; the last component is the PA ID
                ca_ids.extend([allele["@id"].split("/")[-1] for allele in allele["matchingRegisteredTranscripts"]])

    return ca_ids


def get_associated_clinvar_allele_id(clingen_allele_id: str) -> str | None:
    """Retrieve the associated ClinVar Allele ID for a given ClinGen Allele ID from the ClinGen API."""
    response = requests.get(f"{CLINGEN_API_URL}/{clingen_allele_id}")
    if response.status_code != 200:
        logger.error(f"Failed to query ClinGen API for {clingen_allele_id}: {response.status_code}")
        return None

    data = response.json()
    clinvar_allele_id = data.get("externalRecords", {}).get("ClinVarAlleles", [{}])[0].get("alleleId")
    if clinvar_allele_id:
        return str(clinvar_allele_id)

    return None
