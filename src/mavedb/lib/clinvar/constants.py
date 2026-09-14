import os
from pathlib import Path

from urllib3.util.retry import Retry

TSV_VARIANT_ARCHIVE_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/archive"

NCBI_REQUEST_HEADERS = {
    "User-Agent": "MaveDB/1.0 (https://mavedb.org)",
}
"""NCBI's FTP servers aggressively throttle requests with non-descriptive User-Agent headers, returning 503.
MaveDB sets a custom User-Agent to identify itself and avoid unnecessary throttling when fetching ClinVar data.
"""

CLINVAR_CACHE_DIR = Path(os.getenv("CLINVAR_CACHE_DIR", Path.home() / ".cache" / "mavedb" / "clinvar"))
"""File-based cache directory for ClinVar TSV files. These files are large (5-50+ MB) so we store them on disk instead of Redis. By default, this is set to a user-specific cache directory under the home directory, but it can be overridden by setting the CLINVAR_CACHE_DIR environment variable.
"""

NCBI_RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
"""Retries protect against transient NCBI failures (brief 500s, connection resets).
Now that ClinVar versions are fetched sequentially (not concurrently), aggressive
backoff for throttling is unnecessary — a modest retry with short backoff suffices.
"""

CLINVAR_FIELDS_TO_KEEP = ("GeneSymbol", "ClinicalSignificance", "ReviewStatus", "VariationID")
"""Only these fields are extracted from each ClinVar TSV row and cached. The full TSV has ~30 columns; trimming to only what we need shrinks the cached pickle from hundreds of MB to tens of MB and speeds up load times.

VariationID is ClinVar's canonical public identifier (anchors the web UI / variation links); we keep it
alongside the AlleleID (the row key) so the link record can carry both. A row missing the column on an
older archival TSV degrades to None rather than failing the whole version's parse.
"""
