from datetime import date
from humps import camelize

TEST_USER = {
    "username": "0000-1111-2222-3333",
    "first_name": "First",
    "last_name": "Last",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
}

EXTRA_USER = {
    "username": "1234-5678-8765-4321",
    "first_name": "Extra",
    "last_name": "User",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
}

TEST_MINIMAL_EXPERIMENT = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
}

TEST_MINIMAL_EXPERIMENT_RESPONSE = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "scoreSetUrns": [],
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
}

TEST_TAXONOMY = {
    "id": 1,
    "tax_id": 9606,
    "organism_name": "Organism name",
    "common_name": "Common name",
    "rank": "Rank",
    "has_described_species_name": True,
    "article_reference": "NCBI:txid9606",
    "genome_identifier_id": None,
    "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606"
}

TEST_LICENSE = {
    "id": 1,
    "short_name": "Short",
    "long_name": "Long",
    "text": "Don't be evil.",
    "link": "localhost",
    "version": "1.0",
}

TEST_MINIMAL_SCORE_SET = {
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "licenseId": 1,
    "targetGenes":[ {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "targetSequence": {
            "sequenceType": "dna",
            "sequence": "ACGTTT",
            "taxonomy": {
                      "taxId": TEST_TAXONOMY["tax_id"],
                      "organismName": TEST_TAXONOMY["organism_name"],
                      "commonName": TEST_TAXONOMY["common_name"],
                      "rank": TEST_TAXONOMY["rank"],
                      "hasDescribedSpeciesName": TEST_TAXONOMY["has_described_species_name"],
                      "articleReference": TEST_TAXONOMY["article_reference"],
                      "id": TEST_TAXONOMY["id"],
                      "url": TEST_TAXONOMY["url"]
            }
        },
    } ],
}

TEST_MINIMAL_SCORE_SET_RESPONSE = {
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "license": {camelize(k): v for k, v in TEST_LICENSE.items() if k not in ("text",)},
    "numVariants": 0,
    "targetGenes": [ {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "id": 1,
        "targetSequence": {
            "sequenceType": "dna",
            "sequence": "ACGTTT",
            "taxonomy": {
                      "taxId": TEST_TAXONOMY["tax_id"],
                      "organismName": TEST_TAXONOMY["organism_name"],
                      "commonName": TEST_TAXONOMY["common_name"],
                      "rank": TEST_TAXONOMY["rank"],
                      "hasDescribedSpeciesName": TEST_TAXONOMY["has_described_species_name"],
                      "articleReference": TEST_TAXONOMY["article_reference"],
                      "id": TEST_TAXONOMY["id"],
                      "url": TEST_TAXONOMY["url"]
            }
        },
    } ],
    "metaAnalyzesScoreSetUrns": [],
    "metaAnalyzedByScoreSetUrns": [],
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {},
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
}
