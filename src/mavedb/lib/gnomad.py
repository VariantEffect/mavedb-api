import os
import re
from typing import Union

GNOMAD_DB_NAME = "gnomAD"
GNOMAD_DATA_VERSION = os.getenv("GNOMAD_DATA_VERSION")


def gnomad_identifier(contig: str, position: Union[str, int], alleles: list[str]) -> str:
    """
    Generate a gnomAD variant identifier based on contig, position, and alleles.
    """
    contig = contig.replace("chr", "")
    position = str(position)

    # Ensure alleles are sorted to maintain consistency
    alleles = sorted(alleles)

    if len(alleles) < 2:
        raise ValueError("At least two alleles are required to create a gnomAD identifier.")

    # Create the identifier in the format: contig-position-allele1-allele2
    return f"{contig}-{position}-{'-'.join(alleles)}"


def gnomad_table_name() -> str:
    """
    Generate the gnomAD table name based on the data version.
    """
    if not GNOMAD_DATA_VERSION:
        raise ValueError("GNOMAD_DATA_VERSION environment variable is not set.")

    return GNOMAD_DATA_VERSION.replace(".", "_")


def allele_list_from_list_like_string(alleles_string: str) -> list[str]:
    """
    Convert a list-like string representation of alleles into a Python list.

    eg:
    "[A, T]" -> ["A", "T"]
    "[A, TG]" -> ["A", "TG"]
    "" -> []
    "[A, T, C]" -> ValueError: "Invalid format for alleles string."
    """
    if not alleles_string:
        return []

    if not re.match(r"\[[(AGTC)+\, (AGTC)+]", alleles_string):
        raise ValueError("Invalid format for alleles string.")

    alleles_string = alleles_string.strip().strip("[]")  # Remove square brackets if present

    # Remove whitespace and split by comma
    alleles = [allele.strip() for allele in alleles_string.split(",")]

    return alleles
