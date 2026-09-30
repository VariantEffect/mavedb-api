import os

CLIN_GEN_SUBMISSION_ENABLED = os.getenv("CLIN_GEN_SUBMISSION_ENABLED", "false").lower() == "true"

GENBOREE_ACCOUNT_NAME = os.getenv("GENBOREE_ACCOUNT_NAME")
GENBOREE_ACCOUNT_PASSWORD = os.getenv("GENBOREE_ACCOUNT_PASSWORD")

CLIN_GEN_TENANT = os.getenv("CLIN_GEN_TENANT")

CAR_SUBMISSION_ENDPOINT = os.getenv("CAR_SUBMISSION_ENDPOINT")

LDH_SUBMISSION_TYPE = "cg-ldh-ld-submission"
LDH_ENTITY_NAME = "MaveDBMapping"
LDH_ENTITY_ENDPOINT = "maveDb"  # for some reason, not the same :/

DEFAULT_LDH_SUBMISSION_BATCH_SIZE = 100
DEFAULT_CAR_SUBMISSION_BATCH_SIZE = int(os.getenv("CAR_SUBMISSION_BATCH_SIZE", "10000"))
"""Number of HGVS strings sent to the ClinGen Allele Registry per PUT. Reverse translation can
produce very large allele counts, so submissions are chunked rather than sent as a single payload."""
CLINGEN_CACHE_WARMING_CONCURRENCY = int(os.getenv("CLINGEN_CACHE_WARMING_CONCURRENCY", "5"))
"""Maximum number of concurrent requests to make to the ClinGen API when pre-warming the cache for mapped variants."""

CLINGEN_CONNECT_TIMEOUT = float(os.getenv("CLINGEN_CONNECT_TIMEOUT", "5"))
CLINGEN_READ_TIMEOUT = float(os.getenv("CLINGEN_READ_TIMEOUT", "300"))
CLINGEN_HTTP_TIMEOUT = (CLINGEN_CONNECT_TIMEOUT, CLINGEN_READ_TIMEOUT)
"""(connect, read) timeout for ClinGen HTTP calls. Fail fast on connect, with a generous read 
budget because a large batch can take CAR a while to respond."""
LDH_SUBMISSION_ENDPOINT = f"https://genboree.org/mq/brdg/pulsar/{CLIN_GEN_TENANT}/ldh/submissions/{LDH_ENTITY_ENDPOINT}"
LDH_ACCESS_ENDPOINT = os.getenv("LDH_ACCESS_ENDPOINT", "https://genboree.org/ldh")
LDH_MAVE_ACCESS_ENDPOINT = f"{LDH_ACCESS_ENDPOINT}/{LDH_ENTITY_NAME}/id"
