from urllib.parse import quote

from mavedb.lib.clingen.constants import CLINGEN_ALLELE_REGISTRY_PAGE_URL


def clingen_allele_url(clingen_allele_id: str) -> str:
    """Allele Registry page for a CAID or PAID, in the form the ga4gh/cat-vrs examples use."""
    return f"{CLINGEN_ALLELE_REGISTRY_PAGE_URL}?caid={quote(clingen_allele_id, safe='')}"
