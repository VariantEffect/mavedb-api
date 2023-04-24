import os
from datetime import date
import json
import re

from mavedb.models.reference_genome import ReferenceGenome
from tests.conftest import client, TestingSessionLocal

"""
#Test one. Can ignore it.
def test_get_scoreset(test_empty_db):
    response = client.get("/api/v1/scoresets/")
    assert response.status_code == 200
    response_data = response.json()
    scoreset_urn = response_data['urn']
    assert scoreset_urn is not None
    assert re.match(r'tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', scoreset_urn)
"""


def create_an_experiment():
    experiment_to_create = {
        "title": "Test Experiment Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test experiment",
        "extraMetadata": {"key": "value"},
        "keywords": [],
    }
    response = client.post("/api/v1/experiments/", json=experiment_to_create)
    experiment = response.json()
    # experiment_urn = response_data['urn']
    # experiment_set_urn = response_data['experimentSetUrn']
    return experiment


def create_reference_genome():
    reference_genome = ReferenceGenome(id=1, short_name="Name", organism_name="Organism")
    db = TestingSessionLocal()
    db.add(reference_genome)
    db.commit()


def create_scoreset():
    experiment = create_an_experiment()
    scoreset_to_create = {
        "experimentUrn": experiment["urn"],
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [],
        "pubmedIdentifiers": [],
        "dataUsagePolicy": None,
        "datasetColumns": {},
        "supersededScoresetUrn": None,
        "metaAnalysisSourceScoresetUrns": [],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    scoreset = response.json()
    return scoreset


def test_create_scoreset_with_some_none_values(test_empty_db):
    experiment = create_an_experiment()
    create_reference_genome()
    scoreset_to_create = {
        "experimentUrn": experiment["urn"],
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [],
        "pubmedIdentifiers": [],
        "dataUsagePolicy": None,
        "datasetColumns": {},
        "supersededScoresetUrn": None,
        "metaAnalysisSourceScoresetUrns": [],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    assert response.status_code == 200
    response_data = response.json()
    scoreset_urn = response_data["urn"]
    experiment_urn = experiment["urn"]
    assert scoreset_urn is not None
    assert experiment_urn is not None
    assert re.match(r"tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", scoreset_urn)
    assert re.match(r"tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", experiment_urn)
    # Don't use same way as test_create_experiment because some keys in submitted scoreset are different from the
    # response_data. For example, in submitted scoreset experiment urn is called experimentUrn while in response_data
    # it's in experiment{} and called urn.
    # More data are added in different sub-dict such as externalIdentifiers, referenceMaps
    expected_response = {
        "urn": scoreset_urn,
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "dataUsagePolicy": None,
        "licenceId": None,
        "keywords": [],
        "numVariants": 0,
        "experiment": {
            "title": "Test Experiment Title",
            "shortDescription": "Test experiment",
            "abstractText": "Abstract",
            "methodText": "Methods",
            "extraMetadata": {"key": "value"},
            "keywords": [],
            "urn": experiment_urn,
            "numScoresets": 0,
            "createdBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
            "modifiedBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
            "creationDate": date.today().isoformat(),
            "modificationDate": date.today().isoformat(),
            "publishedDate": None,
            "experimentSetUrn": experiment["experimentSetUrn"],
            "doiIdentifiers": [],
            "pubmedIdentifiers": [],
            "rawReadIdentifiers": [],
            "processingState": None,
        },
        "supersededScoreset": None,
        "supersedingScoreset": None,
        "metaAnalysisSourceScoresets": [],
        "metaAnalyses": [],
        "doiIdentifiers": [],
        "pubmedIdentifiers": [],
        "publishedDate": None,
        "creationDate": date.today().isoformat(),
        "modificationDate": date.today().isoformat(),
        "createdBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
        "modifiedBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {
                    "identifier": {
                        "dbName": "Ensembl",
                        "identifier": "ENSG00000103275",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 0,
                },
                {
                    "identifier": {
                        "dbName": "RefSeq",
                        "identifier": "NM_003345",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 159,
                },
                {
                    "identifier": {
                        "dbName": "UniProt",
                        "identifier": "P63279",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 0,
                },
            ],
            "referenceMaps": [
                {
                    "id": 1,
                    "genomeId": 1,
                    "targetId": 1,
                    "isPrimary": False,
                    "genome": {
                        "shortName": "Name",
                        "organismName": "Organism",
                        "genomeId": None,
                        "creationDate": date.today().isoformat(),
                        "modificationDate": date.today().isoformat(),
                        "id": 1,
                    },
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                }
            ],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
        "datasetColumns": {},
        "private": True,
    }
    assert json.dumps(response_data, sort_keys=True) == json.dumps(expected_response, sort_keys=True)


def test_create_scoreset_with_valid_values(test_empty_db):
    experiment = create_an_experiment()
    create_reference_genome()
    superseded_scoreset = create_scoreset()
    meta_analysis_scoreset = create_scoreset()
    scoreset_to_create = {
        "experimentUrn": experiment["urn"],
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [{"identifier": "10.1016/j.cels.2018.01.015"}, {"identifier": "10.1016/j.cels.2018.01.011"}],
        "pubmedIdentifiers": [{"identifier": "20711194"}, {"identifier": "19502423"}],
        "dataUsagePolicy": "data usage",
        "datasetColumns": {},
        "supersededScoresetUrn": superseded_scoreset["urn"],
        "metaAnalysisSourceScoresetUrns": [meta_analysis_scoreset["urn"]],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    assert response.status_code == 200
    response_data = response.json()
    scoreset_urn = response_data["urn"]
    experiment_urn = experiment["urn"]
    assert scoreset_urn is not None
    assert experiment_urn is not None
    assert re.match(r"tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", scoreset_urn)
    assert re.match(r"tmp:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", experiment_urn)
    expected_response = {
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "dataUsagePolicy": "data usage",
        "licenceId": None,
        "keywords": [],
        "urn": scoreset_urn,
        "numVariants": 0,
        "experiment": {
            "title": "Test Experiment Title",
            "shortDescription": "Test experiment",
            "abstractText": "Abstract",
            "methodText": "Methods",
            "extraMetadata": {"key": "value"},
            "keywords": [],
            "urn": experiment_urn,
            "numScoresets": 0,
            "createdBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
            "modifiedBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
            "creationDate": date.today().isoformat(),
            "modificationDate": date.today().isoformat(),
            "publishedDate": None,
            "experimentSetUrn": experiment["experimentSetUrn"],
            "doiIdentifiers": [],
            "pubmedIdentifiers": [],
            "rawReadIdentifiers": [],
            "processingState": None,
        },
        "supersededScoreset": {
            "urn": superseded_scoreset["urn"],
            "title": "Test Scoreset Title",
            "shortDescription": "Test scoreset",
            "publishedDate": None,
            "replacesId": None,
            "numVariants": 0,
            "experiment": {
                "title": "Test Experiment Title",
                "shortDescription": "Test experiment",
                "abstractText": "Abstract",
                "methodText": "Methods",
                "extraMetadata": {"key": "value"},
                "keywords": [],
                "urn": superseded_scoreset["experiment"]["urn"],
                "numScoresets": 0,
                "createdBy": {
                    "orcidId": "someuser",
                    "firstName": "First",
                    "lastName": "Last",
                    "email": None,
                    "roles": [],
                },
                "modifiedBy": {
                    "orcidId": "someuser",
                    "firstName": "First",
                    "lastName": "Last",
                    "email": None,
                    "roles": [],
                },
                "creationDate": date.today().isoformat(),
                "modificationDate": date.today().isoformat(),
                "publishedDate": None,
                "experimentSetUrn": superseded_scoreset["experiment"]["experimentSetUrn"],
                "doiIdentifiers": [],
                "pubmedIdentifiers": [],
                "rawReadIdentifiers": [],
                "processingState": None,
            },
            "creationDate": date.today().isoformat(),
            "modificationDate": date.today().isoformat(),
            "targetGene": {
                "name": "UBE2I",
                "category": "Protein coding",
                "externalIdentifiers": [
                    {
                        "identifier": {
                            "dbName": "Ensembl",
                            "identifier": "ENSG00000103275",
                            "dbVersion": None,
                            "url": None,
                            "referenceHtml": None,
                        },
                        "offset": 0,
                    },
                    {
                        "identifier": {
                            "dbName": "RefSeq",
                            "identifier": "NM_003345",
                            "dbVersion": None,
                            "url": None,
                            "referenceHtml": None,
                        },
                        "offset": 159,
                    },
                    {
                        "identifier": {
                            "dbName": "UniProt",
                            "identifier": "P63279",
                            "dbVersion": None,
                            "url": None,
                            "referenceHtml": None,
                        },
                        "offset": 0,
                    },
                ],
                "referenceMaps": [
                    {
                        "id": 1,
                        "genomeId": 1,
                        "targetId": 1,
                        "isPrimary": False,
                        "genome": {
                            "shortName": "Name",
                            "organismName": "Organism",
                            "genomeId": None,
                            "creationDate": date.today().isoformat(),
                            "modificationDate": date.today().isoformat(),
                            "id": 1,
                        },
                        "creationDate": date.today().isoformat(),
                        "modificationDate": date.today().isoformat(),
                    }
                ],
            },
            "private": True,
        },
        "supersedingScoreset": None,
        "metaAnalysisSourceScoresets": [
            {
                "urn": meta_analysis_scoreset["urn"],
                "title": "Test Scoreset Title",
                "shortDescription": "Test scoreset",
                "publishedDate": None,
                "replacesId": None,
                "numVariants": 0,
                "experiment": {
                    "title": "Test Experiment Title",
                    "shortDescription": "Test experiment",
                    "abstractText": "Abstract",
                    "methodText": "Methods",
                    "extraMetadata": {"key": "value"},
                    "keywords": [],
                    "urn": meta_analysis_scoreset["experiment"]["urn"],
                    "numScoresets": 0,
                    "createdBy": {
                        "orcidId": "someuser",
                        "firstName": "First",
                        "lastName": "Last",
                        "email": None,
                        "roles": [],
                    },
                    "modifiedBy": {
                        "orcidId": "someuser",
                        "firstName": "First",
                        "lastName": "Last",
                        "email": None,
                        "roles": [],
                    },
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                    "publishedDate": None,
                    "experimentSetUrn": meta_analysis_scoreset["experiment"]["experimentSetUrn"],
                    "doiIdentifiers": [],
                    "pubmedIdentifiers": [],
                    "rawReadIdentifiers": [],
                    "processingState": None,
                },
                "creationDate": date.today().isoformat(),
                "modificationDate": date.today().isoformat(),
                "targetGene": {
                    "name": "UBE2I",
                    "category": "Protein coding",
                    "externalIdentifiers": [
                        {
                            "identifier": {
                                "dbName": "Ensembl",
                                "identifier": "ENSG00000103275",
                                "dbVersion": None,
                                "url": None,
                                "referenceHtml": None,
                            },
                            "offset": 0,
                        },
                        {
                            "identifier": {
                                "dbName": "RefSeq",
                                "identifier": "NM_003345",
                                "dbVersion": None,
                                "url": None,
                                "referenceHtml": None,
                            },
                            "offset": 159,
                        },
                        {
                            "identifier": {
                                "dbName": "UniProt",
                                "identifier": "P63279",
                                "dbVersion": None,
                                "url": None,
                                "referenceHtml": None,
                            },
                            "offset": 0,
                        },
                    ],
                    "referenceMaps": [
                        {
                            "id": 2,
                            "genomeId": 1,
                            "targetId": 2,
                            "isPrimary": False,
                            "genome": {
                                "shortName": "Name",
                                "organismName": "Organism",
                                "genomeId": None,
                                "creationDate": date.today().isoformat(),
                                "modificationDate": date.today().isoformat(),
                                "id": 1,
                            },
                            "creationDate": date.today().isoformat(),
                            "modificationDate": date.today().isoformat(),
                        }
                    ],
                },
                "private": True,
            }
        ],
        "metaAnalyses": [],
        "doiIdentifiers": [
            {"identifier": "10.1016/j.cels.2018.01.015", "id": 1, "url": "https://doi.org/10.1016/j.cels.2018.01.015"},
            {"identifier": "10.1016/j.cels.2018.01.011", "id": 2, "url": "https://doi.org/10.1016/j.cels.2018.01.011"},
        ],
        "pubmedIdentifiers": [
            {
                "identifier": "20711194",
                "id": 1,
                "url": "http://www.ncbi.nlm.nih.gov/pubmed/20711194",
                "referenceHtml": "Fowler DM, <i>et al</i>. High-resolution mapping of protein sequence-function relationships. High-resolution mapping of protein sequence-function relationships. 2010; 7:741-6. doi: 10.1038/nmeth.1492",
            },
            {
                "identifier": "19502423",
                "id": 2,
                "url": "http://www.ncbi.nlm.nih.gov/pubmed/19502423",
                "referenceHtml": "Sohka T, <i>et al</i>. An externally tunable bacterial band-pass filter. An externally tunable bacterial band-pass filter. 2009; 106:10135-40. doi: 10.1073/pnas.0901246106",
            },
        ],
        "publishedDate": None,
        "creationDate": date.today().isoformat(),
        "modificationDate": date.today().isoformat(),
        "createdBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
        "modifiedBy": {"orcidId": "someuser", "firstName": "First", "lastName": "Last", "email": None, "roles": []},
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {
                    "identifier": {
                        "dbName": "Ensembl",
                        "identifier": "ENSG00000103275",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 0,
                },
                {
                    "identifier": {
                        "dbName": "RefSeq",
                        "identifier": "NM_003345",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 159,
                },
                {
                    "identifier": {
                        "dbName": "UniProt",
                        "identifier": "P63279",
                        "dbVersion": None,
                        "url": None,
                        "referenceHtml": None,
                    },
                    "offset": 0,
                },
            ],
            "referenceMaps": [
                {
                    "id": 3,
                    "genomeId": 1,
                    "targetId": 3,
                    "isPrimary": False,
                    "genome": {
                        "shortName": "Name",
                        "organismName": "Organism",
                        "genomeId": None,
                        "creationDate": date.today().isoformat(),
                        "modificationDate": date.today().isoformat(),
                        "id": 1,
                    },
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                }
            ],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
        "datasetColumns": {},
        "private": True,
    }
    assert json.dumps(response_data, sort_keys=True) == json.dumps(expected_response, sort_keys=True)


def test_create_scoreset_with_invalid_doi(test_empty_db):
    experiment = create_an_experiment()
    create_reference_genome()
    scoreset_to_create = {
        "experimentUrn": experiment["urn"],
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [{"identifier": "abab"}],
        "pubmedIdentifiers": [],
        "dataUsagePolicy": None,
        "datasetColumns": {},
        "supersededScoresetUrn": None,
        "metaAnalysisSourceScoresetUrns": [],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    assert response.status_code == 422


def test_create_scoreset_with_invalid_pubmed(test_empty_db):
    experiment = create_an_experiment()
    create_reference_genome()
    scoreset_to_create = {
        "experimentUrn": experiment["urn"],
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [],
        "pubmedIdentifiers": [{"identifier": "abcc"}],
        "dataUsagePolicy": None,
        "datasetColumns": {},
        "supersededScoresetUrn": None,
        "metaAnalysisSourceScoresetUrns": [],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    assert response.status_code == 422


def test_create_scoreset_with_invalid_experiment(test_empty_db):
    create_reference_genome()
    scoreset_to_create = {
        "experimentUrn": "invalid_urn",
        "title": "Test Scoreset Title",
        "methodText": "Methods",
        "abstractText": "Abstract",
        "shortDescription": "Test scoreset",
        "extraMetadata": {"key": "value"},
        "keywords": [],
        "doiIdentifiers": [],
        "pubmedIdentifiers": [],
        "dataUsagePolicy": None,
        "datasetColumns": {},
        "supersededScoresetUrn": None,
        "metaAnalysisSourceScoresetUrns": [],
        "targetGene": {
            "name": "UBE2I",
            "category": "Protein coding",
            "externalIdentifiers": [
                {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0},
                {"identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}, "offset": 159},
                {"identifier": {"dbName": "UniProt", "identifier": "P63279"}, "offset": 0},
            ],
            "referenceMaps": [{"genomeId": 1}],
            "wtSequence": {
                "sequenceType": "dna",
                "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
            },
        },
    }
    response = client.post("/api/v1/scoresets/", json=scoreset_to_create)
    assert response.status_code == 422


def test_show_scoreset(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    response = client.get(f"/api/v1/scoresets/{urn}")
    assert response.status_code == 200


# Can't pass
def test_valid_score_file(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores.csv")
    count_csv_path = os.path.join(current_directory, "counts.csv")
    # csv_path = os.path.join("/Users/da.e/Desktop/Estelle Computer/Programming Work/mavedb-api/app/tests/", "scores.csv")
    with open(score_csv_path, "rb") as f1, open(count_csv_path, "rb") as f2:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores.csv", f1, "text/csv"), "counts_file": ("counts.csv", f2, "text/csv")},
        )
    # check the response status code and data
    print(response.json())
    assert response.status_code == 200


"""
# This one can't work cause duplicate column names will be processed automatically
def test_score_file_with_duplicate_columns(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_duplicate_columns.csv")
    count_csv_path = os.path.join(current_directory, "counts.csv")
    # csv_path = os.path.join("/Users/da.e/Desktop/Estelle Computer/Programming Work/mavedb-api/app/tests/", "scores.csv")
    with open(score_csv_path, "rb") as f1, open(count_csv_path, "rb") as f2:
        response = client.post(f"/api/v1/scoresets/{urn}/variants/data", files={'scores_file': ('scores.csv', f1, "text/csv"),
                                                                                'counts_file': ('counts.csv', f2, "text/csv")})
    # check the response status code and data
    print(response.json())
    assert response.status_code == 400
    assert response.json() == {"detail": "There cannot be duplicate column names."}
"""


# Is it a ccorect way?
def test_score_file_without_score_column(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_without_score_column.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_without_score_column.csv", f, "text/csv")},
        )
    assert response.status_code == 400
    assert response.json() == {"detail": "score dataframe must have a 'score' column"}


# Show other problem. Can't pass.
def test_scores_and_counts_define_different_variants(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores.csv")
    count_csv_path = os.path.join(current_directory, "counts_with_different_variants.csv")
    with open(score_csv_path, "rb") as f1, open(count_csv_path, "rb") as f2:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={
                "scores_file": ("scores.csv", f1, "text/csv"),
                "counts_file": ("counts_with_different_variants.csv", f2, "text/csv"),
            },
        )
    assert response.status_code == 400
    assert response.json() == {
        "detail": "both score and count dataframes must define matching variants, discrepancy found in 'hgvs_pro'"
    }


def test_score_file_with_letter(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_string.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data", files={"scores_file": ("scores_with_string.csv", f, "text/csv")}
        )
    assert response.status_code == 400
    assert response.json() == {"detail": "data column 'score' has mixed string and numeric types"}


# can't catch error
def test_score_file_without_hgvs_columns(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_without_hgvs_column.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_without_hgvs_column.csv", f, "text/csv")},
        )
    assert response.status_code == 400
    # I'm confused. There're two validations about this. This one is in validate_column_names
    # The other one is in validate_values_by_column. It raises ValidationError("Missing required hgvs and/or score columns.")
    # And then there's one more ValidationError("Must include either hgvs_nt or hgvs_pro column.")
    assert response.json() == {"detail": "failed to find valid HGVS variant column"}


def test_score_file_hgvs_pro_has_same_values(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_hgvs_pro_has_same_values.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_hgvs_pro_has_same_values.csv", f, "text/csv")},
        )
    assert response.status_code == 400
    assert response.json() == {"detail": "primary variant column 'hgvs_pro' must contain unique values"}


def test_score_and_count_files_have_hgvs_nt_and_pro(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_hgvs_nt_and_pro.csv")
    count_csv_path = os.path.join(current_directory, "counts_with_hgvs_nt_and_pro.csv")
    with open(score_csv_path, "rb") as f1, open(count_csv_path, "rb") as f2:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={
                "scores_file": ("scores_with_hgvs_nt_and_pro.csv", f1, "text/csv"),
                "counts_file": ("counts_with_hgvs_nt_and_pro.csv", f2, "text/csv"),
            },
        )
    assert response.status_code == 200


# Can't catch error.
def test_score_file_has_not_match_hgvs_nt_and_pro(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_hgvs_nt_not_match_pro.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_hgvs_nt_not_match_pro.csv", f, "text/csv")},
        )
    assert response.status_code == 400
    # assert response.json() == {"detail": "Each value in hgvs_pro column must be unique."}


def test_score_file_has_invalid_hgvs_pro_prefix(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_invalid_hgvs_pro_prefix.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_with_invalid_hgvs_pro_prefix.csv", f, "text/csv")},
        )
    assert response.status_code == 400


def test_score_file_has_invalid_hgvs_nt_prefix(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_invalid_hgvs_nt_prefix.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_with_invalid_hgvs_nt_prefix.csv", f, "text/csv")},
        )
    assert response.status_code == 400


def test_count_file_has_score_column(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores.csv")
    count_csv_path = os.path.join(current_directory, "counts_with_score.csv")
    with open(score_csv_path, "rb") as f1, open(count_csv_path, "rb") as f2:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={
                "scores_file": ("scores.csv", f1, "text/csv"),
                "counts_file": ("counts_with_score.csv", f2, "text/csv"),
            },
        )
    assert response.status_code == 400
    assert response.json() == {"detail": "counts dataframe must not have a 'score' column"}


def test_score_file_column_name_has_nan(test_empty_db):
    create_reference_genome()
    scoreset = create_scoreset()
    urn = scoreset["urn"]
    current_directory = os.path.dirname(os.path.abspath(__file__))
    score_csv_path = os.path.join(current_directory, "scores_with_nan_column_name.csv")
    with open(score_csv_path, "rb") as f:
        response = client.post(
            f"/api/v1/scoresets/{urn}/variants/data",
            files={"scores_file": ("scores_with_nan_column_name.csv", f, "text/csv")},
        )
    assert response.status_code == 400
    assert response.json() == {"detail": "column names cannot be empty or whitespace"}
