"""HTTP client for the external dcd-mapping VRS mapping service."""

import requests

from mavedb.lib.mapping.schema import ScoreSetMappingResults


class VRSMap:
    url: str

    def __init__(self, url: str) -> None:
        self.url = url

    def map_score_set(self, score_set_urn: str) -> ScoreSetMappingResults:
        uri = f"{self.url}/api/v1/map/{score_set_urn}"
        response = requests.post(uri)
        response.raise_for_status()
        return response.json()
