from fastapi import APIRouter, HTTPException
import httpx
import xml.etree.ElementTree as ET
import re

from mavedb.lib.logging.logged_route import LoggedRoute

ALPHAFOLD_BASE = "https://alphafold.ebi.ac.uk/files/"

router = APIRouter(
    prefix="/api/v1",
    tags=["alphafold files"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

@router.get("/alphafold-files/version")
async def proxy_alphafold_index():
    """
    Proxy the AlphaFold files index (XML document).
    """
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        resp = await client.get(ALPHAFOLD_BASE, headers={"Accept": "application/xml"})
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Upstream error fetching AlphaFold files index")

    # parse XML response
    try:
        root = ET.fromstring(resp.content)

        # Detect default namespace
        if root.tag.startswith("{"):
            ns_uri = root.tag.split("}", 1)[0][1:]
            ns = {"x": ns_uri}
            next_marker_tag = "x:NextMarker"
        else:
            ns = {}
            next_marker_tag = "NextMarker"

        next_marker_el = root.find(next_marker_tag, ns)
        next_marker = next_marker_el.text if next_marker_el is not None else None

        match = re.search(r"model_(v\d+)\.pdb$", next_marker, re.IGNORECASE)
        if not match:
            raise HTTPException(status_code=500, detail="Malformed AlphaFold PDB ID in XML")
        version = match.group(1)
        return {"version": version.lower()}

    except ET.ParseError as e:
        raise HTTPException(status_code=502, detail=f"Failed to parse upstream XML: {e}")
