from datetime import date, datetime

from humps import camelize

from mavedb.models.enums.processing_state import ProcessingState

TEST_PUBMED_IDENTIFIER = "20711194"
TEST_PUBMED_URL_IDENTIFIER = "https://pubmed.ncbi.nlm.nih.gov/37162834/"
TEST_BIORXIV_IDENTIFIER = "2021.06.21.212592"
TEST_MEDRXIV_IDENTIFIER = "2021.06.22.21259265"
TEST_CROSSREF_IDENTIFIER = "10.1371/2021.06.22.21259265"
TEST_ORCID_ID = "1111-1111-1111-1111"

VALID_ACCESSION = "NM_001637.3"
VALID_GENE = "BRCA1"

SAVED_PUBMED_PUBLICATION = {
    "recordType": "PublicationIdentifier",
    "identifier": "20711194",
    "dbName": "PubMed",
    "title": "None",
    "authors": [],
    "abstract": "test",
    "doi": "test",
    "publicationYear": 1999,
    "publicationJournal": "test",
    "url": "http://www.ncbi.nlm.nih.gov/pubmed/20711194",
    "referenceHtml": ". None. test. 1999; (Unknown volume):(Unknown pages). test",
    "id": 1,
}

SAVED_DOI_IDENTIFIER = {
    "recordType": "DoiIdentifier",
    "identifier": TEST_CROSSREF_IDENTIFIER,
    "url": f"https://doi.org/{TEST_CROSSREF_IDENTIFIER}",
    "id": 1,
}

TEST_USER = {
    "username": "0000-1111-2222-3333",
    "first_name": "First",
    "last_name": "Last",
    "email": "test_user@test.com",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
    "is_first_login": True,
}

CONTRIBUTOR = {
    "orcid_id": TEST_USER["username"],
    "given_name": TEST_USER["first_name"],
    "family_name": TEST_USER["last_name"],
}

SAVED_CONTRIBUTOR = {
    "recordType": "Contributor",
    "orcidId": TEST_USER["username"],
    "givenName": TEST_USER["first_name"],
    "familyName": TEST_USER["last_name"],
}

TEST_USER_DECODED_JWT = {
    "sub": TEST_USER["username"],
    "given_name": TEST_USER["first_name"],
    "family_name": TEST_USER["last_name"],
}

EXTRA_USER = {
    "username": "1234-5678-8765-4321",
    "first_name": "Extra",
    "last_name": "User",
    "email": "extra_user@test.com",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
    "is_first_login": True,
}

EXTRA_CONTRIBUTOR = {
    "orcid_id": EXTRA_USER["username"],
    "given_name": EXTRA_USER["first_name"],
    "family_name": EXTRA_USER["last_name"],
}

SAVED_EXTRA_CONTRIBUTOR = {
    "recordType": "Contributor",
    "orcidId": EXTRA_USER["username"],
    "givenName": EXTRA_USER["first_name"],
    "familyName": EXTRA_USER["last_name"],
}

EXTRA_USER_DECODED_JWT = {
    "sub": EXTRA_USER["username"],
    "given_name": EXTRA_USER["first_name"],
    "family_name": EXTRA_USER["last_name"],
}

ADMIN_USER = {
    "username": "9999-9999-9999-9999",
    "first_name": "Admin",
    "last_name": "User",
    "email": "admin_user@test.com",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
    "is_first_login": True,
}

TEST_DESCRIPTION = "description"

ADMIN_USER_DECODED_JWT = {
    "sub": ADMIN_USER["username"],
    "given_name": ADMIN_USER["first_name"],
    "family_name": ADMIN_USER["last_name"],
}

TEST_EXPERIMENT = {
    "title": "Test Title",
    "short_description": "Test experiment",
    "abstract_text": "Abstract",
    "method_text": "Methods",
}

# Add to db for testing.
TEST_DB_KEYWORDS = [
    {
        "key": "Variant Library Creation Method",
        "value": "Endogenous locus library method",
        "special": False,
        "description": "Description",
    },
    {
        "key": "Variant Library Creation Method",
        "value": "In vitro construct library method",
        "special": False,
        "description": "Description",
    },
    {"key": "Variant Library Creation Method", "value": "Other", "special": False, "description": "Description"},
    {
        "key": "Endogenous Locus Library Method System",
        "value": "SaCas9",
        "special": False,
        "description": "Description",
    },
    {
        "key": "Endogenous Locus Library Method Mechanism",
        "value": "Base editor",
        "special": False,
        "description": "Description",
    },
    {
        "key": "In Vitro Construct Library Method System",
        "value": "Oligo-directed mutagenic PCR",
        "special": False,
        "description": "Description",
    },
    {
        "key": "In Vitro Construct Library Method Mechanism",
        "value": "Native locus replacement",
        "special": False,
        "description": "Description",
    },
    {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
]

TEST_KEYWORDS = [
    {
        "keyword": {
            "key": "Variant Library Creation Method",
            "value": "Endogenous locus library method",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {
            "key": "Endogenous Locus Library Method System",
            "value": "SaCas9",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {
            "key": "Endogenous Locus Library Method Mechanism",
            "value": "Base editor",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
        "description": "Details of delivery method",
    },
]

TEST_EXPERIMENT_WITH_KEYWORD = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "keywords": [
        {
            "keyword": {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
            "description": "Details of delivery method",
        },
    ],
}

TEST_MINIMAL_EXPERIMENT = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
}

TEST_MINIMAL_EXPERIMENT_RESPONSE = {
    "recordType": "Experiment",
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "scoreSetUrns": [],
    "contributors": [],
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
    "officialCollections": [],
}

TEST_EXPERIMENT_WITH_KEYWORD_RESPONSE = {
    "recordType": "Experiment",
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "scoreSetUrns": [],
    "contributors": [],
    "keywords": [
        {
            "recordType": "ExperimentControlledKeyword",
            "keyword": {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
            "description": "Details of delivery method",
        },
    ],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
    "officialCollections": [],
}

TEST_EXPERIMENT_WITH_KEYWORD_HAS_DUPLICATE_OTHERS_RESPONSE = {
    "recordType": "Experiment",
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "scoreSetUrns": [],
    "contributors": [],
    "keywords": [
        {
            "recordType": "ExperimentControlledKeyword",
            "keyword": {
                "key": "Variant Library Creation Method",
                "value": "Other",
                "special": False,
                "description": "Description",
            },
            "description": "Description",
        },
        {
            "recordType": "ExperimentControlledKeyword",
            "keyword": {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
            "description": "Description",
        },
    ],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
    "officialCollections": [],
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
    "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
}

TEST_LICENSE = {
    "id": 1,
    "short_name": "Short",
    "long_name": "Long",
    "text": "Don't be evil.",
    "link": "localhost",
    "version": "1.0",
    "active": True,
}

SAVED_SHORT_TEST_LICENSE = {
    "recordType": "ShortLicense",
    "id": TEST_LICENSE["id"],
    "shortName": TEST_LICENSE["short_name"],
    "longName": TEST_LICENSE["long_name"],
    "link": TEST_LICENSE["link"],
    "version": TEST_LICENSE["version"],
    "active": TEST_LICENSE["active"],
}

EXTRA_LICENSE = {
    "id": 2,
    "short_name": "Extra",
    "long_name": "License",
    "text": "Don't be tooooo evil.",
    "link": "localhost",
    "version": "1.0",
    "active": True,
}

SAVED_SHORT_EXTRA_LICENSE = {
    "recordType": "ShortLicense",
    "id": EXTRA_LICENSE["id"],
    "shortName": EXTRA_LICENSE["short_name"],
    "longName": EXTRA_LICENSE["long_name"],
    "link": EXTRA_LICENSE["link"],
    "version": EXTRA_LICENSE["version"],
    "active": EXTRA_LICENSE["active"],
}

TEST_INACTIVE_LICENSE = {
    "id": 3,
    "short_name": "Long",
    "long_name": "Short",
    "text": "Be evil.",
    "link": "localhost",
    "version": "1.0",
    "active": False,
}

SAVED_SHORT_INACTIVE_LICENSE = {
    "recordType": "ShortLicense",
    "id": TEST_INACTIVE_LICENSE["id"],
    "shortName": TEST_INACTIVE_LICENSE["short_name"],
    "longName": TEST_INACTIVE_LICENSE["long_name"],
    "link": TEST_INACTIVE_LICENSE["link"],
    "version": TEST_INACTIVE_LICENSE["version"],
    "active": TEST_INACTIVE_LICENSE["active"],
}

TEST_SEQ_SCORESET = {
    "title": "Test Score Set Title",
    "short_description": "Test score set",
    "abstract_text": "Abstract",
    "method_text": "Methods",
    "target_genes": [
        {
            "name": "TEST1",
            "category": "protein_coding",
            "target_sequence": {
                "sequence_type": "dna",
                "sequence": "ACGTTT",
                "taxonomy": {
                    "tax_id": TEST_TAXONOMY["tax_id"],
                    "organism_name": TEST_TAXONOMY["organism_name"],
                    "common_name": TEST_TAXONOMY["common_name"],
                    "rank": TEST_TAXONOMY["rank"],
                    "id": TEST_TAXONOMY["id"],
                    "url": TEST_TAXONOMY["url"],
                },
            },
        }
    ],
}

TEST_MINIMAL_SEQ_SCORESET = {
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "licenseId": 1,
    "targetGenes": [
        {
            "name": "TEST1",
            "category": "protein_coding",
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
                    "url": TEST_TAXONOMY["url"],
                },
            },
        }
    ],
}

TEST_MINIMAL_SEQ_SCORESET_RESPONSE = {
    "recordType": "ScoreSet",
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "license": {
        "recordType": "ShortLicense",
        **{camelize(k): v for k, v in TEST_LICENSE.items() if k not in ("text",)},
    },
    "numVariants": 0,
    "targetGenes": [
        {
            "recordType": "TargetGene",
            "name": "TEST1",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "id": 1,
            "targetSequence": {
                "recordType": "TargetSequence",
                "sequenceType": "dna",
                "sequence": "ACGTTT",
                "label": "TEST1",
                "taxonomy": {
                    "recordType": "Taxonomy",
                    "taxId": TEST_TAXONOMY["tax_id"],
                    "organismName": TEST_TAXONOMY["organism_name"],
                    "commonName": TEST_TAXONOMY["common_name"],
                    "rank": TEST_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_TAXONOMY["article_reference"],
                    "id": TEST_TAXONOMY["id"],
                    "url": TEST_TAXONOMY["url"],
                },
            },
        }
    ],
    "metaAnalyzesScoreSetUrns": [],
    "metaAnalyzedByScoreSetUrns": [],
    "contributors": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {},
    "externalLinks": {},
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
    "processingState": ProcessingState.incomplete.name,
    "officialCollections": [],
}

TEST_MINIMAL_ACC_SCORESET = {
    "title": "Test Score Set Acc Title",
    "shortDescription": "Test accession score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "licenseId": 1,
    "targetGenes": [
        {
            "name": "TEST2",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "targetAccession": {
                "accession": VALID_ACCESSION,
                "assembly": "GRCh37",
                "gene": VALID_GENE,
                "isBaseEditor": False,
            },
        }
    ],
}

TEST_ACC_SCORESET = {
    "title": "Test Score Set Acc Title",
    "short_description": "Test accession score set",
    "abstract_text": "Abstract",
    "method_text": "Methods",
    "target_genes": [
        {
            "name": "TEST2",
            "category": "protein_coding",
            "target_accession": {
                "accession": VALID_ACCESSION,
                "assembly": "GRCh37",
                "gene": VALID_GENE,
                "isBaseEditor": False,
            },
        }
    ],
}

TEST_BASE_EDITOR_SCORESET = {
    "title": "Test Score Set Acc Title",
    "short_description": "Test accession score set",
    "abstract_text": "Abstract",
    "method_text": "Methods",
    "target_genes": [
        {
            "name": "TEST2",
            "category": "protein_coding",
            "target_accession": {
                "accession": VALID_ACCESSION,
                "assembly": "GRCh37",
                "gene": VALID_GENE,
                "isBaseEditor": False,
            },
        }
    ],
}

TEST_MINIMAL_ACC_SCORESET_RESPONSE = {
    "recordType": "ScoreSet",
    "title": "Test Score Set Acc Title",
    "shortDescription": "Test accession score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "license": {
        "recordType": "ShortLicense",
        **{camelize(k): v for k, v in TEST_LICENSE.items() if k not in ("text",)},
    },
    "numVariants": 0,
    "targetGenes": [
        {
            "recordType": "TargetGene",
            "name": "TEST2",
            "id": 2,
            "category": "protein_coding",
            "externalIdentifiers": [],
            "targetAccession": {
                "recordType": "TargetAccession",
                "accession": VALID_ACCESSION,
                "assembly": "GRCh37",
                "gene": VALID_GENE,
                "isBaseEditor": False,
            },
        }
    ],
    "metaAnalyzesScoreSetUrns": [],
    "metaAnalyzedByScoreSetUrns": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {},
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
    "processingState": ProcessingState.incomplete.name,
    "officialCollections": [],
}

TEST_CDOT_TRANSCRIPT = {
    "start_codon": 0,
    "stop_codon": 18,
    "id": VALID_ACCESSION,
    "gene_version": "313",
    "gene_name": VALID_GENE,
    "biotype": ["protein_coding"],
    "protein": "NP_001628.1",
    "genome_builds": {
        "GRCh37": {
            "cds_end": 1,
            "cds_start": 18,
            "contig": "NC_000007.13",
            # The exons are non-sense but it doesn't really matter for the tests.
            "exons": [[1, 12, 20, 2001, 2440, "M196 I1 M61 I1 M181"], [12, 18, 19, 1924, 2000, None]],
            "start": 1,
            "stop": 18,
            "strand": "+",
        }
    },
}


TEST_MINIMAL_PRE_MAPPED_METADATA = {
    "genomic": {"sequence_id": "ga4gh:SQ.em9khDCUYXrVWBfWr9r8fjBUrTjj1aig", "sequence_type": "dna"}
}


TEST_MINIMAL_POST_MAPPED_METADATA = {
    "genomic": {
        "sequence_id": "ga4gh:SQ.em9khDCUYXrVWBfWr9r8fjBUrTjj1aig",
        "sequence_type": "dna",
        "sequence_accessions": [VALID_ACCESSION],
        "sequence_genes": [VALID_GENE],
    }
}

TEST_VARIANT_MAPPING_SCAFFOLD = {
    "metadata": {},
    "computed_genomic_reference_sequence": {
        "sequence_type": "dna",
        "sequence_id": "ga4gh:SQ.ref_test",
        "sequence": "ACGTTT",
    },
    "mapped_genomic_reference_sequence": {
        "sequence_type": "dna",
        "sequence_id": "ga4gh:SQ.map_test",
        "sequence_accessions": ["NC_000001.11"],
    },
    "mapped_scores": [],
    "vrs_version": "2.0",
    "dcd_mapping_version": "pytest.0.0",
    "mapped_date_utc": datetime.isoformat(datetime.now()),
}


TEST_MINIMAL_MAPPED_VARIANT = {
    "pre_mapped": {},
    "post_mapped": {},
    "modification_date": datetime.isoformat(datetime.now()),
    "mapped_date": datetime.isoformat(datetime.now()),
    "current": True,
    "vrs_version": "2.0",
    "mapping_api_version": "pytest.0.0",
}


TEST_SCORESET_RANGE = {
    "wt_score": 1.0,
    "ranges": [
        {"label": "test1", "classification": "normal", "range": (0, 2.0)},
        {"label": "test2", "classification": "abnormal", "range": (-2.0, 0)},
    ],
}


TEST_SAVED_SCORESET_RANGE = {
    "wtScore": 1.0,
    "ranges": [
        {"label": "test1", "classification": "normal", "range": [0.0, 2.0]},
        {"label": "test2", "classification": "abnormal", "range": [-2.0, 0.0]},
    ],
}


TEST_SCORE_CALIBRATION = {
    "parameter_sets": [
        {
            "functionally_altering": {"skew": 1.15, "location": -2.20, "scale": 1.20},
            "functionally_normal": {"skew": -1.5, "location": 2.25, "scale": 0.8},
            "fraction_functionally_altering": 0.20,
        },
    ],
    "evidence_strengths": [3, 2, 1, -1],
    "thresholds": [1.25, 2.5, 3, 5.5],
    "positive_likelihood_ratios": [100, 10, 1, 0.1],
    "prior_probability_pathogenicity": 0.20,
}


TEST_SAVED_SCORE_CALIBRATION = {
    "parameterSets": [
        {
            "functionallyAltering": {"skew": 1.15, "location": -2.20, "scale": 1.20},
            "functionallyNormal": {"skew": -1.5, "location": 2.25, "scale": 0.8},
            "fractionFunctionallyAltering": 0.20,
        },
    ],
    "evidenceStrengths": [3, 2, 1, -1],
    "thresholds": [1.25, 2.5, 3, 5.5],
    "positiveLikelihoodRatios": [100, 10, 1, 0.1],
    "priorProbabilityPathogenicity": 0.20,
}


TEST_COLLECTION = {"name": "Test collection", "description": None, "private": True}


TEST_COLLECTION_RESPONSE = {
    "recordType": "Collection",
    "name": "Test collection",
    # "description": None,
    "private": True,
    "createdBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "recordType": "User",
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "experimentUrns": [],
    "scoreSetUrns": [],
    "admins": [
        {
            "recordType": "User",
            "firstName": TEST_USER["first_name"],
            "lastName": TEST_USER["last_name"],
            "orcidId": TEST_USER["username"],
        }
    ],
    "editors": [],
    "viewers": [],
}
