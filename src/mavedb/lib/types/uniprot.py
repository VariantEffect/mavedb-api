from typing import TypedDict


class MappingEntry(TypedDict):
    """
    Represents a single mapping result from the UniProt ID mapping API.

    Attributes:
        uniprot_id (str): The mapped UniProt identifier.
        entry_type (str): The type of UniProt entry (e.g., Swiss-Prot, TrEMBL).
    """

    uniprot_id: str
    entry_type: str


MappingEntries = list[dict[str, MappingEntry]]
