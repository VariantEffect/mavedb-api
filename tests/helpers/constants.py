from datetime import date, datetime

from humps import camelize

from mavedb.models.enums.processing_state import ProcessingState


VALID_EXPERIMENT_SET_URN = "urn:mavedb:01234567"
VALID_EXPERIMENT_URN = f"{VALID_EXPERIMENT_SET_URN}-abcd"
VALID_SCORE_SET_URN = f"{VALID_EXPERIMENT_URN}-0123"
VALID_VARIANT_URN = f"{VALID_SCORE_SET_URN}#1"

TEST_PUBMED_IDENTIFIER = "20711194"
TEST_PUBMED_URL_IDENTIFIER = "https://pubmed.ncbi.nlm.nih.gov/37162834/"
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
TEST_HGVS_IDENTIFIER = f"{TEST_REFSEQ_IDENTIFIER}:p.Asp5Phe"

VALID_ACCESSION = "NM_001637.3"
VALID_NT_ACCESSION = "NM_001637.3"
VALID_PRO_ACCESSION = "NP_001637.4"
VALID_GENE = "BRCA1"

VALID_CLINGEN_PA_ID = "PA2579908752"
VALID_CLINGEN_CA_ID = "CA341478553"
VALID_CLINGEN_LDH_ID = "2786738861"

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

# VRS 1.X
TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS1_X = {
    "id": TEST_GA4GH_IDENTIFIER,
    "type": "Allele",
    "variation": {
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
    },
}

TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X = {
    "id": TEST_GA4GH_IDENTIFIER,
    "type": "Allele",
    "variation": {
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
        "expressions": [{"value": TEST_HGVS_IDENTIFIER, "syntax": "hgvs.p"}],
    },
}

# VRS 2.X
TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X = {
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

TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X = {
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
    "expressions": [{"value": TEST_HGVS_IDENTIFIER, "syntax": "hgvs.p"}],
}

# VRS 1.X
TEST_VALID_PRE_MAPPED_VRS_HAPLOTYPE = {
    "type": "Haplotype",
    "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X],
}

TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE = {
    "type": "Haplotype",
    "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X],
}

# VRS 2.X
TEST_VALID_PRE_MAPPED_VRS_CIS_PHASED_BLOCK = {
    "type": "Haplotype",
    "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X],
}

TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK = {
    "type": "Haplotype",
    "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X],
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
                "accession": VALID_NT_ACCESSION,
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
                "accession": VALID_NT_ACCESSION,
                "assembly": "GRCh37",
                "gene": VALID_GENE,
                "is_base_editor": False,
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
                "accession": VALID_NT_ACCESSION,
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
                "accession": VALID_NT_ACCESSION,
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

TEST_NT_CDOT_TRANSCRIPT = {
    "start_codon": 0,
    "stop_codon": 18,
    "id": VALID_NT_ACCESSION,
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

TEST_PRO_CDOT_TRANSCRIPT = {
    "start_codon": 0,
    "stop_codon": 18,
    "id": VALID_PRO_ACCESSION,
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


TEST_SCORE_SET_RANGE = {
    "wt_score": 1.0,
    "ranges": [
        {"label": "test1", "classification": "normal", "range": (0, 2.0)},
        {"label": "test2", "classification": "abnormal", "range": (-2.0, 0)},
    ],
}


TEST_SAVED_SCORE_SET_RANGE = {
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

TEST_CLINVAR_CONTROL = {
    "db_identifier": "183058",
    "gene_symbol": "PTEN",
    "clinical_significance": "Likely benign",
    "clinical_review_status": "criteria provided, multiple submitters, no conflicts",
    "db_name": "ClinVar",
    "db_version": "11_2024",
}


TEST_SAVED_CLINVAR_CONTROL = {
    "recordType": "ClinicalControlWithMappedVariants",
    "dbIdentifier": "183058",
    "geneSymbol": "PTEN",
    "clinicalSignificance": "Likely benign",
    "clinicalReviewStatus": "criteria provided, multiple submitters, no conflicts",
    "dbName": "ClinVar",
    "dbVersion": "11_2024",
    "mappedVariants": [],
}


TEST_GENERIC_CLINICAL_CONTROL = {
    "db_identifier": "ABC123",
    "gene_symbol": "BRCA1",
    "clinical_significance": "benign",
    "clinical_review_status": "lots of convincing evidence",
    "db_name": "GenDB",
    "db_version": "2024",
}


TEST_SAVED_GENERIC_CLINICAL_CONTROL = {
    "recordType": "ClinicalControlWithMappedVariants",
    "dbIdentifier": "ABC123",
    "geneSymbol": "BRCA1",
    "clinicalSignificance": "benign",
    "clinicalReviewStatus": "lots of convincing evidence",
    "dbName": "GenDB",
    "dbVersion": "2024",
    "mappedVariants": [],
}


TEST_CLINGEN_SUBMISSION_RESPONSE = {
    "data": {"msg": "Data sent successfully", "msgIds": ["(148894,0,-1,0)"]},
    "metadata": {"rendered": {"by": "https://genboree.org/mq/brdg/srvc", "when": datetime.now().isoformat()}},
    "status": {"code": 200, "name": "OK"},
}


TEST_CLINGEN_SUBMISSION_UNAUTHORIZED_RESPONSE = {
    "metadata": {"rendered": {"when": datetime.now().isoformat()}},
    "status": {"code": 403, "msg": "Bad Auth Info - jwt malformed", "name": "Forbidden"},
}

TEST_CLINGEN_SUBMISSION_BAD_RESQUEST_RESPONSE = {
    "metadata": {"rendered": {"when": datetime.now().isoformat()}},
    "status": {
        "code": 400,
        "msg": "Put Failed - Error! Submission was an empty object. Submission must consist of valid, non-Empty JSON objects",
        "name": "Bad Request",
    },
}


TEST_CLINGEN_LDH_LINKING_RESPONSE = {
    "data": {
        "created": datetime.now().isoformat(),
        "creator": "brl_clingen",
        "entContent": {
            "mapping_api_version": "pytest.mapping.1.0",
            "mavedb_id": VALID_VARIANT_URN,
            "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
            "pre_mapped": TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
            "score": 1.0,
        },
        "entId": VALID_VARIANT_URN,
        "entIri": f"https://staging.mavedb.org/score-sets/{VALID_VARIANT_URN}",
        "entType": "MaveDBMapping",
        "ldFor": {
            "Variant": [
                {
                    "created": datetime.now().isoformat(),
                    "creator": "brl_clingen",
                    "entId": VALID_CLINGEN_PA_ID,
                    "entIri": f"http://reg.genome.network/allele/{VALID_CLINGEN_PA_ID}",
                    "entType": "Variant",
                    "ldhId": VALID_CLINGEN_LDH_ID,
                    "ldhIri": f"https://10.15.55.128/ldh-stg/Variant/id/{VALID_CLINGEN_LDH_ID}",
                    "modified": datetime.now().isoformat(),
                    "modifier": "brl_clingen",
                    "rev": "_hLpznbC-A-",
                }
            ]
        },
        "ldhId": VALID_CLINGEN_LDH_ID,
        "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/{VALID_CLINGEN_LDH_ID}",
        "modified": datetime.now().isoformat(),
        "modifier": "brl_clingen",
        "rev": "_jj3a99K---",
    },
    "metadata": {"rendered": {"by": "https://10.15.55.128/ldh-stg/srvc", "when": datetime.now().isoformat()}},
    "status": {"code": 200, "name": "OK"},
}


TEST_CLINGEN_LDH_LINKING_RESPONSE_NOT_FOUND = {
    "metadata": {"rendered": {"by": "https://10.15.55.128/ldh-stg/srvc", "when": datetime.now().isoformat()}},
    "status": {
        "code": 404,
        "msg": f"Bad Entity - No 'MaveDBMapping' entity found with identifier {VALID_VARIANT_URN}",
        "name": "Not Found",
    },
}


TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST = {
    "errCode": 400,
    "errMsg": "INVALID URL - Your request is invalid. Specifically, the URL path you provided ('/ldh-stg/MaveDBMapping/i/urn%3Amavedb%3A00000050-a-1%231') is not valid for HTTP 'GET' requests to the CG-LDH API service.",
    "errName": "Bad Request",
    "errCat": "INVALID URL",
}
