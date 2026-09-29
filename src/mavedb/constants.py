import os

MAVEDB_BASE_GIT = "https://github.com/VariantEffect/mavedb-api"
MAVEDB_FRONTEND_URL = os.getenv("MAVE_FRONTEND_URL", "https://mavedb.org")
# Concept DOI of the public data archive on Zenodo; it always resolves to the latest release.
MAVEDB_BULK_DOWNLOAD_URL = "https://doi.org/10.5281/zenodo.11201736"
