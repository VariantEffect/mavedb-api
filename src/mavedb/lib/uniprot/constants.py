import os

UNIPROT_ID_MAPPING_ENABLED = os.getenv("UNIPROT_ID_MAPPING_ENABLED", "false").lower() == "true"
UNIPROT_ID_MAPPING_API_URL = "https://rest.uniprot.org"
