from datetime import date, datetime

from humps import camelize

from mavedb.models.enums.processing_state import ProcessingState

# TODO#373 - Cleanup test constants

VALID_EXPERIMENT_SET_URN = "urn:mavedb:01234567"
VALID_EXPERIMENT_URN = "urn:mavedb:01234567-abcd"
VALID_SCORE_SET_URN = "urn:mavedb:01234567-abcd-0123"

TEST_PUBMED_IDENTIFIER = "20711194"
TEST_BIORXIV_IDENTIFIER = "2021.06.21.212592"
TEST_MEDRXIV_IDENTIFIER = "2021.06.22.21259265"
TEST_CROSSREF_IDENTIFIER = "10.1371/2021.06.22.21259265"
TEST_ORCID_ID = "1111-1111-1111-1111"

TEST_GA4GH_IDENTIFIER = "ga4gh:SQ.test"
# ^[0-9A-Za-z_\-]{32}$
TEST_GA4GH_DIGEST = "ga4ghtest_ga4ghtest_ga4ghtest_dg"
# ^SQ.[0-9A-Za-z_\-]{32}$
TEST_REFGET_ACCESSION = "SQ.ga4ghtest_ga4ghtest_ga4ghtest_rg"
TEST_SEQUENCE_LOCATION_ACCESSION = "ga4gh:SL.test"

TEST_REFSEQ_IDENTIFIER = "NM_003345"
TEST_UNIPROT_IDENTIFIER = "P63279"
TEST_ENSEMBL_IDENTIFIER = "ENSG00000103275"

TEST_REFSEQ_EXTERNAL_IDENTIFIER = {"identifier": TEST_REFSEQ_IDENTIFIER, "db_name": "RefSeq"}
TEST_UNIPROT_EXTERNAL_IDENTIFIER = {"identifier": TEST_UNIPROT_IDENTIFIER, "db_name": "Uniprot"}
TEST_ENSEMBLE_EXTERNAL_IDENTIFIER = {"identifier": TEST_ENSEMBL_IDENTIFIER, "db_name": "Ensembl"}

VALID_ACCESSION = "NM_001637.3"
VALID_GENE = "BRCA1"

TEST_VALID_PRE_MAPPED_VRS_ALLELE = {
    "id": TEST_GA4GH_IDENTIFIER,
    "type": "Allele",
    "state": {"type": "LiteralSequenceExpression", "sequence": "V"},
    "digest": TEST_GA4GH_DIGEST,
    "location": {
        "id": TEST_SEQUENCE_LOCATION_ACCESSION,
        "end": 2,
        "type": "SequenceLocation",
        "start": 1,
        "digest": TEST_GA4GH_DIGEST,
        "sequenceReference": {
            "type": "SequenceReference",
            "refgetAccession": TEST_REFGET_ACCESSION,
        },
    },
    "extensions": [{"name": "vrs_ref_allele_seq", "type": "Extension", "value": "W"}],
}

TEST_VALID_POST_MAPPED_VRS_ALLELE = {
    "id": TEST_GA4GH_IDENTIFIER,
    "type": "Allele",
    "state": {"type": "LiteralSequenceExpression", "sequence": "F"},
    "digest": TEST_GA4GH_DIGEST,
    "location": {
        "id": TEST_SEQUENCE_LOCATION_ACCESSION,
        "end": 6,
        "type": "SequenceLocation",
        "start": 5,
        "digest": TEST_GA4GH_DIGEST,
        "sequenceReference": {
            "type": "SequenceReference",
            "label": TEST_REFSEQ_IDENTIFIER,
            "refgetAccession": TEST_REFGET_ACCESSION,
        },
    },
    "extensions": [{"name": "vrs_ref_allele_seq", "type": "Extension", "value": "D"}],
    "expressions": [{"value": f"{TEST_REFSEQ_IDENTIFIER}:p.Asp5Phe", "syntax": "hgvs.p"}],
}

TEST_VALID_PRE_MAPPED_VRS_HAPLOTYPE = {
    "type": "Haplotype",
    "members": [TEST_VALID_PRE_MAPPED_VRS_ALLELE, TEST_VALID_PRE_MAPPED_VRS_ALLELE],
}

TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE = {
    "type": "Haplotype",
    "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE, TEST_VALID_POST_MAPPED_VRS_ALLELE],
}

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
    "recordType": "SavedContributor",
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

TEST_MINIMAL_TAXONOMY = {
    "tax_id": 9606,
}

TEST_POPULATED_TAXONOMY = {
    **TEST_MINIMAL_TAXONOMY,
    "organism_name": "Organism name",
    "common_name": "Common name",
    "rank": "Rank",
    "has_described_species_name": True,
    "article_reference": "NCBI:txid9606",
    "genome_identifier_id": None,
}

TEST_SAVED_TAXONOMY = {
    **TEST_POPULATED_TAXONOMY,
    "id": 1,
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
            "external_identifiers": [],
            "target_sequence": {
                "sequence_type": "dna",
                "sequence": "ACGTTT",
                "reference": {
                    "id": 1,
                    "short_name": "Name",
                    "organism_name": "Organism",
                    "creation_date": date.today().isoformat(),
                    "modification_date": date.today().isoformat(),
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
                    "taxId": TEST_SAVED_TAXONOMY["tax_id"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
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
                    "taxId": TEST_SAVED_TAXONOMY["tax_id"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
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
            "targetAccession": {"accession": VALID_ACCESSION, "assembly": "GRCh37", "gene": VALID_GENE},
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
            "external_identifiers": [],
            "target_accession": {"accession": VALID_ACCESSION, "assembly": "GRCh37", "gene": VALID_GENE},
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


TEST_SCORE_SET_RANGE = {
    "wt_score": 1.0,
    "ranges": [
        {"label": "test1", "classification": "normal", "range": [0, 2.0]},
        {"label": "test2", "classification": "abnormal", "range": [-2.0, 0]},
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

SEQUENCE = (
    "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCT"
    "GAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCT"
    "GCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTA"
    "CGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTG"
    "CACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCG"
    "CAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGT"
    "TTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCA"
    "ACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA"
)


TEST_MINIMAL_MAPPED_VARIANT = {
    "variant_id": 1,
    "modification_date": date.today(),
    "vrs_version": "2.0",
    "mapped_date": date.today(),
    "mapping_api_version": "pytest.0.0",
    "current": True,
}


TEST_MINIMAL_ORCID_AUTH_TOKEN_REQUEST = {
    "code": "xxx.test.xxx",
    "redirect_uri": "https://www.fake.orcid.org/redirect_uri",
}


TEST_MINIMAL_ORCID_AUTH_TOKEN_RESPONSE = {
    "access_token": "yyy.test.yyy",
    "expires_in": 30,
    "id_token": "zzz.test.zzz",
    "token_type": "bearer",
}

TEST_MINIMAL_ORCID_USER = {"orcid_id": TEST_ORCID_ID}

TEST_MINIMAL_RAW_READ_IDENTIFIER = {"identifier": "test_raw_read"}

TEST_SAVED_MINIMAL_RAW_READ_IDENTIFIER = {
    **TEST_MINIMAL_RAW_READ_IDENTIFIER,
    "id": 1,
    "url": "https://www.test.rawreadidentifiers.org",
}

TEST_POPULATED_EXPERIMENT_SEARCH = {
    "published": True,
    "authors": ["last-name"],
    "databases": ["uniprot"],
    "journals": ["biomed"],
    "publication_identifiers": ["12345678"],
    "keywords": ["keyword"],
    "text": "testtesttest",
}

TEST_POPULATED_SCORE_SET_SEARCH = {
    "published": True,
    "targets": ["BRCA1"],
    "target_organism_names": ["homo sapiens"],
    "target_types": ["protein_coding"],
    "target_accessions": ["NC_12345.1"],
    "authors": ["last-name"],
    "databases": ["uniprot"],
    "journals": ["biomed"],
    "publication_identifiers": ["12345678"],
    "keywords": ["keyword"],
    "text": "testtesttest",
}

TEST_POPULATED_TEXT_SEARCH = {"text": "testtesttest"}

TEST_MINIMAL_VARIANT = {
    "data": {},
    "score_set_id": 1,
    "creation_date": date.today(),
    "modification_date": date.today(),
}

TEST_POPULATED_VARIANT = {
    **TEST_MINIMAL_VARIANT,
    "urn": f"{VALID_SCORE_SET_URN}#1",
    "hgvs_nt": "c.1A>T",
    "hgvs_pro": "p.1M>T",
    "hgvs_splice": "c.1A>T",
}

TEST_SAVED_VARIANT = {
    **TEST_POPULATED_VARIANT,
    "id": 1,
}
