import asyncio
import csv
import gzip
import hashlib
import io
import logging
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import requests
from filelock import FileLock
from requests.adapters import HTTPAdapter

from mavedb.lib.clinvar.constants import (
    CLINVAR_CACHE_DIR,
    CLINVAR_FIELDS_TO_KEEP,
    CLINVAR_NS_PATTERN,
    NCBI_REQUEST_HEADERS,
    NCBI_RETRY_STRATEGY,
    TSV_VARIANT_ARCHIVE_BASE_URL,
)

_FIELDS_HASH = hashlib.sha256("|".join(CLINVAR_FIELDS_TO_KEEP).encode()).hexdigest()[:8]
"""Short hash of the kept fields, embedded in the cache filename so that adding/removing fields automatically invalidates stale caches. This ensures that if we change which fields we keep from the ClinVar TSV, we won't accidentally use old cached data that doesn't have the new fields."""

logger = logging.getLogger(__name__)


def parse_clinvar_namespace(ns: str) -> Optional[str]:
    """Parse a ClinVar-versioned namespace into its db_version string.

    Namespaces are of the form ``"clinvar.YEAR_MONTH"`` (e.g. ``"clinvar.2024_01"``
    for January 2024). The corresponding ``db_version`` stored in
    ``clinical_controls`` is ``"MONTH_YEAR"`` (e.g. ``"01_2024"``).

    Returns ``None`` if *ns* does not match the expected pattern.
    """
    m = CLINVAR_NS_PATTERN.match(ns)
    if not m:
        return None
    year, month = m.group(1), m.group(2)
    return f"{month}_{year}"


def _ncbi_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NCBI_REQUEST_HEADERS)
    adapter = HTTPAdapter(max_retries=NCBI_RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def validate_clinvar_variant_summary_date(month: int, year: int) -> None:
    """
    Validates the provided month and year for fetching ClinVar variant summary data.

    Ensures that:
    - The year is not earlier than 2015 (ClinVar archived data is only available from 2015 onwards).
    - The year is not in the future.
    - If the year is the current year, the month is not in the future.

    Raises:
        ValueError: If the provided year is before 2015, in the future, or if the month is in the future for the current year.

    Args:
        month (int): The month to validate (1-12).
        year (int): The year to validate.
    """
    current_year = datetime.now().year
    current_month = datetime.now().month

    if month < 1 or month > 12:
        raise ValueError("Month must be an integer between 1 and 12.")

    if year < 2015 or (year == 2015 and month < 2):
        raise ValueError("ClinVar archived data is only available from February 2015 onwards.")
    elif year > current_year:
        raise ValueError("Cannot fetch ClinVar data for future years.")
    elif year == current_year and month > current_month:
        raise ValueError("Cannot fetch ClinVar data for future months.")


async def fetch_clinvar_variant_data(month: int, year: int) -> Dict[str, Dict[str, str]]:
    """
    Fetch, parse, and cache ClinVar variant summary data for a given month/year.

    Downloads the gzipped TSV from NCBI (with retry), parses it, trims each row
    to only the fields we need (see ``CLINVAR_FIELDS_TO_KEEP``), and caches the
    resulting dict as a pickle file on disk.  Both download and parse run in an
    executor to avoid blocking the event loop — the modern 350 MB+ files take
    significant CPU time to decompress and parse.

    On subsequent calls the cached pickle is loaded directly (also in an executor),
    skipping both the network fetch and the expensive parse.

    Args:
        month: The month for which to fetch the variant summary (1-12).
        year: The year for which to fetch the variant summary.

    Returns:
        A dict mapping AlleleID (str) to a dict of the kept fields, e.g.
        ``{"VCV123": {"GeneSymbol": "BRCA1", "ClinicalSignificance": "Pathogenic", "ReviewStatus": "..."}}``.

    Raises:
        requests.RequestException: If the file cannot be downloaded from either location.
        ValueError: If the provided month or year is invalid.
    """
    validate_clinvar_variant_summary_date(month, year)

    cache_file = CLINVAR_CACHE_DIR / f"variant_summary_{year}-{month:02d}.parsed.{_FIELDS_HASH}.pkl"

    # Archival ClinVar files are immutable — cache never expires.
    if cache_file.exists():
        logger.debug(f"Cache hit for parsed ClinVar {year}-{month:02d}")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _load_parsed_cache, cache_file)

    logger.debug(f"Cache miss — fetching and parsing ClinVar {year}-{month:02d}")

    # ClinVar stores recent files at the top level and older files in
    # year-based subdirectories.  The cadence at which files are moved is not
    # documented, so we try both locations with a preference for the top-level.
    url_top_level = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/variant_summary_{year}-{month:02d}.txt.gz"
    url_archive = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/{year}/variant_summary_{year}-{month:02d}.txt.gz"

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_parse_and_cache, url_top_level, url_archive, cache_file, year, month)


def _load_parsed_cache(cache_file: Path) -> Dict[str, Dict[str, str]]:
    """Load a previously cached parsed dict from a pickle file."""
    with open(cache_file, "rb") as f:
        return pickle.load(f)  # noqa: S301 — trusted local cache written by _fetch_parse_and_cache


def _fetch_parse_and_cache(
    url_top_level: str,
    url_archive: str,
    cache_file: Path,
    year: int,
    month: int,
) -> Dict[str, Dict[str, str]]:
    """Download ClinVar TSV, parse to a trimmed dict, and cache as pickle.

    Runs in an executor — all operations here are blocking (network I/O + CPU).

    A per-version file lock prevents two concurrent workers from downloading
    and parsing the same version simultaneously, which would double peak memory
    usage. The second worker acquires the lock after the first finishes and
    writes the cache, then finds the cache file already present and returns
    early without re-downloading.
    """
    CLINVAR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lock_file = CLINVAR_CACHE_DIR / f"variant_summary_{year}-{month:02d}.lock"

    with FileLock(lock_file):
        # Re-check cache inside the lock — another worker may have populated it
        # while we were waiting.
        if cache_file.exists():
            logger.debug(f"Cache hit (post-lock) for parsed ClinVar {year}-{month:02d}")
            return _load_parsed_cache(cache_file)

        session = _ncbi_session()
        try:
            response = session.get(url_top_level, stream=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            response = session.get(url_archive, stream=True)
            response.raise_for_status()

        # Stream the compressed response directly into the gzip decompressor
        # rather than loading all bytes into memory first.  On recent ClinVar
        # files the compressed payload is 50–350 MB; buffering it as bytes and
        # then calling readlines() on the decompressed stream would peak at
        # 2–3 GB per job.  Streaming + lazy CSV iteration keeps peak memory to
        # the size of the trimmed output dict (tens of MB).
        buf = io.BytesIO()
        for chunk in response.iter_content(chunk_size=1 << 20):  # 1 MB chunks
            buf.write(chunk)
        buf.seek(0)

        # Parse the gzipped TSV, keeping only the fields we actually use.
        # Some old ClinVar files have fields larger than the default csv limit.
        default_csv_field_size_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(sys.maxsize)
            # Iterate lazily — avoids materialising all decompressed lines
            # as a list (which would be 1.5–2 GB for a modern TSV).
            with gzip.open(filename=buf, mode="rt") as f:
                reader = csv.DictReader(f, delimiter="\t")  # type: ignore
                data: Dict[str, Dict[str, str]] = {
                    str(row["#AlleleID"]): {field: row[field] for field in CLINVAR_FIELDS_TO_KEEP} for row in reader
                }
        finally:
            csv.field_size_limit(default_csv_field_size_limit)

        # Cache the parsed + trimmed dict to disk so subsequent calls skip both
        # the network fetch and the expensive parse.
        with open(cache_file, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

        logger.info(f"Cached parsed ClinVar {year}-{month:02d} to {cache_file} ({len(data)} alleles)")

        # Remove stale cache files for this month/year with a different fields hash.
        stale_prefix = f"variant_summary_{year}-{month:02d}.parsed."
        for stale in CLINVAR_CACHE_DIR.glob(f"{stale_prefix}*.pkl"):
            if stale != cache_file:
                stale.unlink(missing_ok=True)
                logger.debug(f"Removed stale cache file {stale}")

        return data
