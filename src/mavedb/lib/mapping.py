from datetime import date
from typing import Any, Optional, TypedDict, Union

import requests

ANNOTATION_LAYERS = {
    "g": "genomic",
    "p": "protein",
    "c": "cdna",
}


class VRSMap:
    url: str

    class GeneInfo(TypedDict):
        hgnc_symbol: str
        selection_method: str

    class TargetAnnotation(TypedDict):
        gene_info: "VRSMap.GeneInfo"
        layers: dict[str, dict[str, dict[str, dict[str, Union[str, list[str]]]]]]

    class ScoreSetMappingResults(TypedDict):
        metadata: Optional[dict[str, str]]
        dcd_mapping_version: str
        mapped_date_utc: date
        reference_sequences: Optional[dict[str, "VRSMap.TargetAnnotation"]]
        mapped_scores: Optional[list[dict]]
        error_message: Optional[str]

    def __init__(self, url: str) -> None:
        self.url = url

    def map_score_set(self, score_set_urn: str) -> ScoreSetMappingResults:
        uri = f"{self.url}/api/v1/map/{score_set_urn}"
        response = requests.post(uri)
        response.raise_for_status()
        return response.json()


def extract_ids_from_post_mapped_metadata(post_mapped_metadata: dict[str, Any]) -> Optional[list[str]]:
    """
    Extracts sequence accession IDs from post-mapped metadata.

    This function checks the provided metadata dictionary for either "protein" or "cdna" keys,
    and attempts to retrieve the "sequence_accessions" field from the corresponding sub-dictionary.
    If neither key is present or the input is empty, returns None.

    Args:
        post_mapped_metadata (Dict[str, Any]): Metadata dictionary potentially containing
            "protein" or "cdna" keys with "sequence_accessions" lists.

    Returns:
        Optional[List[str]]: List of sequence accession IDs if found, otherwise None.
    """
    if not post_mapped_metadata:
        return None

    if "protein" in post_mapped_metadata:
        return post_mapped_metadata["protein"].get("sequence_accessions")
    if "cdna" in post_mapped_metadata:
        return post_mapped_metadata["cdna"].get("sequence_accessions")

    return None
