import os

GENBOREE_ACCOUNT_NAME = os.getenv("GENBOREE_ACCOUNT_NAME")
GENBOREE_ACCOUNT_PASSWORD = os.getenv("GENBOREE_ACCOUNT_PASSWORD")

CLIN_GEN_TENANT = os.getenv("CLIN_GEN_TENANT")

LDH_SUBMISSION_TYPE = "cg-ldh-ld-submission"
LDH_ENTITY_NAME = "MaveDBMapping"
LDH_ENTITY_ENDPOINT = "maveDb"  # for some reason, not the same :/

MAVEDB_BASE_GIT = "https://github.com/VariantEffect/mavedb-api"

DEFAULT_LDH_SUBMISSION_BATCH_SIZE = 100
LDH_SUBMISSION_URL = f"https://genboree.org/mq/brdg/pulsar/{CLIN_GEN_TENANT}/ldh/submissions/{LDH_ENTITY_ENDPOINT}"
LDH_LINKED_DATA_URL = f"https://genboree.org/{CLIN_GEN_TENANT}/{LDH_ENTITY_NAME}/id"
