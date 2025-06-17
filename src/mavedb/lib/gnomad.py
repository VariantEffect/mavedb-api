import os
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
