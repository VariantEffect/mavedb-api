import csv
import gzip
import io
import sys
from datetime import datetime
from typing import Dict

import requests

from mavedb.lib.clinvar.constants import TSV_VARIANT_ARCHIVE_BASE_URL


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


def fetch_clinvar_variant_summary_tsv(month: int, year: int) -> bytes:
    """
    Fetches the ClinVar variant summary TSV file for a specified month and year.

    This function attempts to download the variant summary file from the ClinVar FTP archive.
    It first tries the top-level directory for recent files, and if not found, falls back to the year-based subdirectory.
    The function validates the provided month and year before attempting the download.

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

    # Construct URLs for the variant summary TSV file. ClinVar stores recent files at the top level and older files in year-based subdirectories.
    # The cadence at which files are moved is not documented, so we try both locations with a preference for the top-level URL.
    url_top_level = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/variant_summary_{year}-{month:02d}.txt.gz"
    url_archive = f"{TSV_VARIANT_ARCHIVE_BASE_URL}/{year}/variant_summary_{year}-{month:02d}.txt.gz"

    try:
        response = requests.get(url_top_level, stream=True)
        response.raise_for_status()
        return response.content
    except requests.exceptions.HTTPError:
        response = requests.get(url_archive, stream=True)
        response.raise_for_status()
        return response.content


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
