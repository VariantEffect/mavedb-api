import os

UNIPROT_ID_MAPPING_ENABLED = os.getenv("UNIPROT_ID_MAPPING_ENABLED", "false").lower() == "true"
UNIPROT_ID_MAPPING_API_URL = "https://rest.uniprot.org"

SWISS_PROT_ENTRY_TYPE = "UniProtKB reviewed (Swiss-Prot)"
