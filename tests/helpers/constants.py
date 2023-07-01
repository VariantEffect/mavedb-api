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

TEST_REFERENCE_GENOME = {
    "id": 1,
    "short_name": "Name",
    "organism_name": "Organism",
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
    "targetGene": {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "referenceMaps": [{"genomeId": TEST_REFERENCE_GENOME["id"]}],
        "wtSequence": {
            "sequenceType": "dna",
            "sequence": "ACGTTT",
        },
    },
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
    "targetGene": {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "referenceMaps": [
            {
                "creationDate": date.today().isoformat(),
                "modificationDate": date.today().isoformat(),
                "genomeId": TEST_REFERENCE_GENOME["id"],
                "id": 1,
                "targetId": 1,
                "isPrimary": False,
                "genome": {camelize(k): v for k, v in TEST_REFERENCE_GENOME.items()}
                | {
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                },
            }
        ],
        "wtSequence": {
            "sequenceType": "dna",
            "sequence": "ACGTTT",
        },
    },
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
