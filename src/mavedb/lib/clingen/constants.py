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
CLINGEN_CACHE_WARMING_CONCURRENCY = int(os.getenv("CLINGEN_CACHE_WARMING_CONCURRENCY", "5"))
"""Maximum number of concurrent requests to make to the ClinGen API when pre-warming the cache for mapped variants."""
LDH_SUBMISSION_ENDPOINT = f"https://genboree.org/mq/brdg/pulsar/{CLIN_GEN_TENANT}/ldh/submissions/{LDH_ENTITY_ENDPOINT}"
LDH_ACCESS_ENDPOINT = os.getenv("LDH_ACCESS_ENDPOINT", "https://genboree.org/ldh")
LDH_MAVE_ACCESS_ENDPOINT = f"{LDH_ACCESS_ENDPOINT}/{LDH_ENTITY_NAME}/id"
