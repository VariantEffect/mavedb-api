import asyncio
import csv
import gzip
import io
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import requests

from mavedb.lib.clinvar.constants import TSV_VARIANT_ARCHIVE_BASE_URL

logger = logging.getLogger(__name__)

# ClinVar TSV files are archival and never change once released
# Use 90-day TTL (7776000 seconds) for file-based caching
# Since these files are immutable and stored on disk (not Redis), a long TTL
# reduces unnecessary re-downloads and bandwidth usage
CLINVAR_TSV_CACHE_TTL = 7776000

# File-based cache directory for ClinVar TSV files
# These files are large (5-50+ MB) so we store them on disk instead of Redis
# Defaults to a user-specific cache directory under the home directory unless CLINVAR_CACHE_DIR is set
CLINVAR_CACHE_DIR = Path(os.getenv("CLINVAR_CACHE_DIR", Path.home() / ".cache" / "mavedb" / "clinvar"))


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


async def fetch_clinvar_variant_summary_tsv(month: int, year: int) -> bytes:
    """
    Fetches the ClinVar variant summary TSV file for a specified month and year.

    This function attempts to download the variant summary file from the ClinVar FTP archive.
    It first tries the top-level directory for recent files, and if not found, falls back to the year-based subdirectory.
    The function validates the provided month and year before attempting the download.

    Results are cached to disk for 90 days since archival ClinVar data is immutable.
    File-based caching is used instead of Redis because these files are large (5-50+ MB).

    Args:
        month (int): The month for which to fetch the variant summary (as an integer).
        year (int): The year for which to fetch the variant summary.

    Returns:
        bytes: The contents of the downloaded variant summary TSV file (gzipped).

    Raises:
        requests.RequestException: If the file cannot be downloaded from either location.
        ValueError: If the provided month or year is invalid.
    """
    validate_clinvar_variant_summary_date(month, year)

    # Check file-based cache first
    cache_file = CLINVAR_CACHE_DIR / f"variant_summary_{year}-{month:02d}.txt.gz"

    if cache_file.exists():
        file_age = time.time() - cache_file.stat().st_mtime
        if file_age < CLINVAR_TSV_CACHE_TTL:
            logger.debug(
                f"Cache hit for ClinVar {year}-{month:02d} (age: {file_age:.0f}s, TTL: {CLINVAR_TSV_CACHE_TTL}s)"
            )
            return cache_file.read_bytes()
        else:
            logger.debug(
                f"Cache expired for ClinVar {year}-{month:02d} (age: {file_age:.0f}s, TTL: {CLINVAR_TSV_CACHE_TTL}s)"
            )

    logger.debug(f"Cache miss or expired - fetching ClinVar {year}-{month:02d} from remote server")
    # Construct URLs for the variant summary TSV file. ClinVar stores recent files at the top level and older files in year-based subdirectories.
    # The cadence at which files are moved is not documented, so we try both locations with a preference for the top-level URL.
    url_top_level = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/variant_summary_{year}-{month:02d}.txt.gz"
    url_archive = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/{year}/variant_summary_{year}-{month:02d}.txt.gz"

    # Execute HTTP request in executor to avoid blocking the event loop
    loop = asyncio.get_running_loop()

    def _fetch_and_cache_tsv():
        try:
            response = requests.get(url_top_level, stream=True)
            response.raise_for_status()
            content = response.content
        except requests.exceptions.HTTPError:
            response = requests.get(url_archive, stream=True)
            response.raise_for_status()
            content = response.content

        # Store in file cache
        CLINVAR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file.write_bytes(content)
        logger.info(f"Cached ClinVar {year}-{month:02d} to {cache_file} ({len(content)} bytes)")

        return content

    return await loop.run_in_executor(None, _fetch_and_cache_tsv)


def parse_clinvar_variant_summary(tsv_content: bytes) -> Dict[str, Dict[str, str]]:
    """
    Parses a gzipped TSV file content and returns a dictionary mapping Allele IDs to row data.

    Args:
        tsv_content (bytes): The gzipped TSV file content as bytes.

    Returns:
        Dict[str, Dict[str, str]]: A dictionary where each key is a string Allele ID (from the '#AlleleID' column),
        and each value is a dictionary representing the corresponding row with column names as keys.

    Raises:
        KeyError: If the '#AlleleID' column is missing in any row.
        ValueError: If the '#AlleleID' value cannot be converted to an integer.
        csv.Error: If there is an error parsing the TSV content.

    Note:
        The function temporarily increases the CSV field size limit to handle large fields in the TSV file. Some old ClinVar
        variant summary files may have fields larger than the default limit.
    """
    default_csv_field_size_limit = csv.field_size_limit()

    try:
        csv.field_size_limit(sys.maxsize)

        with gzip.open(filename=io.BytesIO(tsv_content), mode="rt") as f:
            # This readlines object will only be a list of bytes if the file is opened in "rb" mode.
            reader = csv.DictReader(f.readlines(), delimiter="\t")  # type: ignore
            data = {str(row["#AlleleID"]): row for row in reader}

    finally:
        csv.field_size_limit(default_csv_field_size_limit)

    return data
