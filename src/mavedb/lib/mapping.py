from datetime import date
from typing import Optional, TypedDict, Union

import requests

ANNOTATION_LAYERS = {
    "g": "genomic",
    "p": "protein",
    "c": "cdna",
}


class VRSMap:
    url: str

    class ScoreSetMappingResults(TypedDict):
        metadata: Optional[dict[str, str]]
        dcd_mapping_version: str
        mapped_date_utc: date
        reference_sequences: Optional[dict[str, dict[str, dict[str, dict[str, Union[str, list[str]]]]]]]
        mapped_scores: Optional[list[dict]]
        error_message: Optional[str]

    def __init__(self, url: str) -> None:
        self.url = url

    def map_score_set(self, score_set_urn: str) -> ScoreSetMappingResults:
        uri = f"{self.url}/api/v1/map/{score_set_urn}"
        response = requests.post(uri)
        response.raise_for_status()
        return response.json()
