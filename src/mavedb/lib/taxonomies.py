from typing import Any

import httpx
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session

from mavedb.models.taxonomy import Taxonomy
from mavedb.view_models.taxonomy import TaxonomyCreate


async def find_or_create_taxonomy(db: Session, taxonomy: TaxonomyCreate):
    """
    Find an existing taxonomy ID record with the specified tax_id int, or create a new one.

    :param db: An active database session
    :param taxonomy: A TaxonomyCreate object containing the taxonomy details to search for or create.
    :return: An existing Taxonomy containing the specified taxonomy ID, or a new, unsaved Taxonomy
    tax_id: A valid taxonomy ID from NCBI
    """
    taxonomy_record = db.query(Taxonomy).filter(Taxonomy.tax_id == taxonomy.tax_id).one_or_none()
    if not taxonomy_record:
        taxonomy_record = await search_NCBI_taxonomy(db, str(taxonomy.tax_id))
    return taxonomy_record


async def search_NCBI_taxonomy(db: Session, search: str) -> Any:
    """
    Check whether this taxonomy exists in NCBI website.
    If it exists, saving it in local database and return it to users.
    """
    if search and len(search.strip()) > 0:
        if search.isnumeric() is False:
            search_text = search.strip().lower()
        else:
            search_text = str(search)

        url = f"https://api.ncbi.nlm.nih.gov/datasets/v2alpha/taxonomy/taxon/{search_text}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()

                # response status code is 200 even no match data in NCBI.
                if "errors" in data.get("taxonomy_nodes", [{}])[0]:
                    return None

                # Process the retrieved data as needed
                ncbi_taxonomy = data["taxonomy_nodes"][0]["taxonomy"]
                ncbi_taxonomy.setdefault("organism_name", "NULL")
                ncbi_taxonomy.setdefault("common_name", "NULL")
                ncbi_taxonomy.setdefault("rank", "NULL")
                ncbi_taxonomy.setdefault("has_described_species_name", False)
                taxonomy_record = Taxonomy(
                    tax_id=ncbi_taxonomy["tax_id"],
                    organism_name=ncbi_taxonomy["organism_name"],
                    common_name=ncbi_taxonomy["common_name"],
                    rank=ncbi_taxonomy["rank"],
                    has_described_species_name=ncbi_taxonomy["has_described_species_name"],
                    url="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id="
                    + str(ncbi_taxonomy["tax_id"]),
                    article_reference="NCBI:txid" + str(ncbi_taxonomy["tax_id"]),
                )
                db.add(taxonomy_record)
                db.commit()
                db.refresh(taxonomy_record)
            else:
                raise HTTPException(status_code=404, detail=f"Taxonomy with search {search_text} not found in NCBI")
    else:
        raise HTTPException(status_code=404, detail="Please enter valid searching words")

    return taxonomy_record
