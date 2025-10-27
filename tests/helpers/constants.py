from datetime import date, datetime

from humps import camelize

from mavedb.models.enums.processing_state import ProcessingState

VALID_EXPERIMENT_SET_URN = "urn:mavedb:01234567"
VALID_EXPERIMENT_URN = f"{VALID_EXPERIMENT_SET_URN}-a"
VALID_SCORE_SET_URN = f"{VALID_EXPERIMENT_URN}-1"
VALID_TMP_URN = "tmp:79471b5b-2dbd-4a96-833c-c33023862437"
VALID_VARIANT_URN = f"{VALID_SCORE_SET_URN}#1"
VALID_COLLECTION_URN = "urn:mavedb:collection-79471b5b-2dbd-4a96-833c-c33023862437"
VALID_CALIBRATION_URN = "urn:mavedb:calibration-79471b5b-2dbd-4a96-833c-c33023862437"

TEST_PUBMED_IDENTIFIER = "20711194"
TEST_PUBMED_URL_IDENTIFIER = "https://pubmed.ncbi.nlm.nih.gov/37162834/"
TEST_BIORXIV_IDENTIFIER = "2021.06.21.212592"
TEST_MEDRXIV_IDENTIFIER = "2021.06.22.21259265"
TEST_CROSSREF_IDENTIFIER = "10.1371/2021.06.22.21259265"
TEST_ORCID_ID = "1111-1111-1111-1111"

GA4GH_SEQUENCE_DIGEST = "SQ.TestTestTestTestTestTestTestTest"
# "^ga4gh:([^.]+)\.([0-9A-Za-z_\-]{32})$"
TEST_GA4GH_IDENTIFIER = f"ga4gh:{GA4GH_SEQUENCE_DIGEST}"
# ^[0-9A-Za-z_\-]{32}$
TEST_GA4GH_DIGEST = "ga4ghtest_ga4ghtest_ga4ghtest_dg"
# ^SQ.[0-9A-Za-z_\-]{32}$
TEST_REFGET_ACCESSION = "SQ.ga4ghtest_ga4ghtest_ga4ghtest_rg"
TEST_SEQUENCE_LOCATION_ACCESSION = "ga4gh:SL.test"

TEST_REFSEQ_IDENTIFIER = "NM_003345"
TEST_HGVS_IDENTIFIER = f"{TEST_REFSEQ_IDENTIFIER}:p.Asp5Phe"
TEST_UNIPROT_IDENTIFIER = "P63279"
TEST_ENSEMBL_IDENTIFIER = "ENSG00000103275"

TEST_REFSEQ_EXTERNAL_IDENTIFIER = {"identifier": TEST_REFSEQ_IDENTIFIER, "db_name": "RefSeq"}
TEST_UNIPROT_EXTERNAL_IDENTIFIER = {"identifier": TEST_UNIPROT_IDENTIFIER, "db_name": "Uniprot"}
TEST_ENSEMBLE_EXTERNAL_IDENTIFIER = {"identifier": TEST_ENSEMBL_IDENTIFIER, "db_name": "Ensembl"}

VALID_CHR_ACCESSION = "NC_000001.11"
VALID_ACCESSION = "NM_001637.3"
VALID_NT_ACCESSION = "NM_001637.3"
VALID_PRO_ACCESSION = "NP_001637.4"
VALID_GENE = "BRCA1"
VALID_UNIPROT_ACCESSION = "P05067"

VALID_ENSEMBL_IDENTIFIER = "ENST00000530893.6"

VALID_CLINGEN_PA_ID = "PA2579908752"
VALID_CLINGEN_CA_ID = "CA341478553"
VALID_CLINGEN_LDH_ID = "2786738861"

VALID_MD5_DIGEST = "01234abcde%"
VALID_VMC_DIGEST = "GS_ASNKvN4=%"

TEST_SEQREPO_INITIAL_STATE = [
    {f"refseq:{VALID_ACCESSION}": {"seq_id": "seq1", "seq": "AAAA", "namespace": "refseq", "alias": VALID_ACCESSION}},
    {f"MD5:{VALID_MD5_DIGEST}": {"seq_id": "seq2", "seq": "CCCC", "namespace": "MD5", "alias": VALID_MD5_DIGEST}},
    {
        f"ensembl:{VALID_ENSEMBL_IDENTIFIER}": {
            "seq_id": "seq3",
            "seq": "GGGG",
            "namespace": "ensembl",
            "alias": VALID_ENSEMBL_IDENTIFIER,
        }
    },
    {
        f"ga4gh:{GA4GH_SEQUENCE_DIGEST}": {
            "seq_id": "seq4",
            "seq": "EEEE",
            "namespace": "ga4gh",
            "alias": GA4GH_SEQUENCE_DIGEST,
        }
    },
]

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

TEST_PUBMED_PUBLICATION = {
    "identifier": TEST_PUBMED_IDENTIFIER,
    "db_name": "PubMed",
    "title": "None",
    "authors": [],
    "abstract": "test",
    "doi": "test",
    "publication_year": 1999,
    "publication_journal": "test",
    "url": "http://www.ncbi.nlm.nih.gov/pubmed/20711194",
    "reference_html": ". None. test. 1999; (Unknown volume):(Unknown pages). test",
}

SAVED_PUBMED_PUBLICATION = {
    "recordType": "PublicationIdentifier",
    "identifier": TEST_PUBMED_IDENTIFIER,
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

TEST_BIORXIV_PUBLICATION = {
    "identifier": TEST_BIORXIV_IDENTIFIER,
    "db_name": "bioRxiv",
    "title": "test biorxiv",
    "authors": [{"name": "", "primary": True}],
    "abstract": "test abstract",
    "doi": "test:test:test",
    "publication_year": 1999,
    "publication_journal": "Preprint",
    "url": "https://www.biorxiv.org/content/10.1101/2021.06.21.212592",
    "reference_html": ". test biorxiv. (None). 1999; (Unknown volume):(Unknown pages). test:test:test",
}

SAVED_BIORXIV_PUBLICATION = {
    "recordType": "PublicationIdentifier",
    "id": 2,
    **{camelize(k): v for k, v in TEST_BIORXIV_PUBLICATION.items()},
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
    "expressions": [{"value": TEST_HGVS_IDENTIFIER, "syntax": "hgvs.p", "type": "Expression", "syntax_version": None}],
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

TEST_SAVED_USER = {
    "recordType": "SavedUser",
    **{camelize(k): v for k, v in TEST_USER.items()},
}

TEST_USER2 = {
    "username": "1111-2222-3333-4444",
    "first_name": "First",
    "last_name": "Last",
    "email": "test_user2@test.com",
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
        "label": "Endogenous locus library method",
        "special": False,
        "description": "Description",
    },
    {
        "key": "Variant Library Creation Method",
        "label": "In vitro construct library method",
        "special": False,
        "description": "Description",
    },
    {"key": "Variant Library Creation Method", "label": "Other", "special": False, "description": "Description"},
    {
        "key": "Endogenous Locus Library Method System",
        "label": "SaCas9",
        "special": False,
        "description": "Description",
    },
    {
        "key": "Endogenous Locus Library Method Mechanism",
        "label": "Base editor",
        "special": False,
        "description": "Description",
    },
    {
        "key": "In Vitro Construct Library Method System",
        "label": "Oligo-directed mutagenic PCR",
        "special": False,
        "description": "Description",
    },
    {
        "key": "In Vitro Construct Library Method Mechanism",
        "label": "Native locus replacement",
        "special": False,
        "description": "Description",
    },
    {"key": "Delivery method", "label": "Other", "special": False, "description": "Description"},
    {
        "key": "Phenotypic Assay Mechanism",
        "label": "Other",
        "code": None,
        "special": False,
        "description": "Description",
    },
    {
        "key": "Phenotypic Assay Mechanism",
        "label": "Label",
        "code": "GO:1234567",
        "special": False,
        "description": "Description",
    },
]

TEST_KEYWORDS = [
    {
        "keyword": {
            "key": "Variant Library Creation Method",
            "label": "Endogenous locus library method",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {
            "key": "Endogenous Locus Library Method System",
            "label": "SaCas9",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {
            "key": "Endogenous Locus Library Method Mechanism",
            "label": "Base editor",
            "special": False,
            "description": "Description",
        },
    },
    {
        "keyword": {"key": "Delivery method", "label": "Other", "special": False, "description": "Description"},
        "description": "Details of delivery method",
    },
]

TEST_EXPERIMENT_SET = {
    "extra_metadata": {},
    "approved": False,
}

TEST_MINIMAL_EXPERIMENT_SET = {
    "extraMetadata": {},
    "approved": False,
}

TEST_EXPERIMENT_WITH_KEYWORD = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "keywords": [
        {
            "keyword": {"key": "Delivery method", "label": "Other", "special": False, "description": "Description"},
            "description": "Details of delivery method",
        },
    ],
}

TEST_EXPERIMENT = {
    "title": "Test Experiment Title",
    "short_description": "Test experiment",
    "abstract_text": "Abstract",
    "method_text": "Methods",
    "extra_metadata": {},
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
    "numScoreSets": 0,
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
            "keyword": {"key": "Delivery method", "label": "Other", "special": False, "description": "Description"},
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
    "numScoreSets": 0,  # NOTE: This is context-dependent and may need overriding per test
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
                "label": "Other",
                "special": False,
                "description": "Description",
            },
            "description": "Description",
        },
        {
            "recordType": "ExperimentControlledKeyword",
            "keyword": {"key": "Delivery method", "label": "Other", "special": False, "description": "Description"},
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
    "numScoreSets": 0,  # NOTE: This is context-dependent and may need overriding per test
}


TEST_MINIMAL_TAXONOMY = {
    "code": 9606,
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

SAVED_MINIMAL_DATASET_COLUMNS = {
    "recordType": "DatasetColumns",
    "countColumns": [],
    "scoreColumns": ["score", "s_0", "s_1"],
}

TEST_SEQ_SCORESET = {
    "title": "Test Score Set Title",
    "short_description": "Test score set",
    "abstract_text": "Abstract",
    "method_text": "Methods",
    "extra_metadata": {},
    "target_genes": [
        {
            "name": "TEST1",
            "category": "protein_coding",
            "target_sequence": {
                "sequence_type": "dna",
                "sequence": "ACGTTT",
                "taxonomy": {
                    "code": TEST_SAVED_TAXONOMY["code"],
                    "organism_name": TEST_SAVED_TAXONOMY["organism_name"],
                    "common_name": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
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
                    "code": TEST_SAVED_TAXONOMY["code"],
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
                    "code": TEST_SAVED_TAXONOMY["code"],
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
    "scoreCalibrations": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {
        "recordType": "DatasetColumns",
    },
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
    "datasetColumns": {
        "recordType": "DatasetColumns",
    },
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
    "processingState": ProcessingState.incomplete.name,
    "officialCollections": [],
}

TEST_MINIMAL_MULTI_TARGET_SCORESET = {
    "title": "Test Multi Target Score Set Title",
    "shortDescription": "Test multi target score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "licenseId": 1,
    "targetGenes": [
        {
            "name": "TEST3",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "targetSequence": {
                "sequenceType": "dna",
                "sequence": "ACGTTT",
                "label": "TEST3",
                "taxonomy": {
                    "code": TEST_SAVED_TAXONOMY["code"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
                },
            },
        },
        {
            "name": "TEST4",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "targetSequence": {
                "sequenceType": "dna",
                "sequence": "TAATGCC",
                "label": "TEST4",
                "taxonomy": {
                    "code": TEST_SAVED_TAXONOMY["code"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
                },
            },
        },
    ],
}

TEST_MINIMAL_MULTI_TARGET_SCORESET_RESPONSE = {
    "recordType": "ScoreSet",
    "title": "Test Multi Target Score Set Title",
    "shortDescription": "Test multi target score set",
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
            "name": "TEST3",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "id": 1,
            "targetSequence": {
                "recordType": "TargetSequence",
                "sequenceType": "dna",
                "sequence": "ACGTTT",
                "label": "TEST3",
                "taxonomy": {
                    "recordType": "Taxonomy",
                    "code": TEST_SAVED_TAXONOMY["code"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
                },
            },
        },
        {
            "recordType": "TargetGene",
            "name": "TEST4",
            "category": "protein_coding",
            "externalIdentifiers": [],
            "id": 1,
            "targetSequence": {
                "recordType": "TargetSequence",
                "sequenceType": "dna",
                "sequence": "TAATGCC",
                "label": "TEST4",
                "taxonomy": {
                    "recordType": "Taxonomy",
                    "code": TEST_SAVED_TAXONOMY["code"],
                    "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                    "commonName": TEST_SAVED_TAXONOMY["common_name"],
                    "rank": TEST_SAVED_TAXONOMY["rank"],
                    "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                    "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                    "id": TEST_SAVED_TAXONOMY["id"],
                    "url": TEST_SAVED_TAXONOMY["url"],
                },
            },
        },
    ],
    "metaAnalyzesScoreSetUrns": [],
    "metaAnalyzedByScoreSetUrns": [],
    "contributors": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {
        "recordType": "DatasetColumns",
    },
    "externalLinks": {},
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
    "processingState": ProcessingState.incomplete.name,
    "officialCollections": [],
}

TEST_SCORE_SET_DATASET_COLUMNS = {
    "score_columns": ["score", "s_0", "s_1"],
    "count_columns": ["c_0", "c_1"],
    "score_columns_metadata": {
        "s_0": {"description": "s_0 description", "details": "s_0 details"},
        "s_1": {"description": "s_1 description", "details": "s_1 details"},
    },
    "count_columns_metadata": {
        "c_0": {"description": "c_0 description", "details": "c_0 details"},
        "c_1": {"description": "c_1 description", "details": "c_1 details"},
    },
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
        "sequence_accessions": [VALID_NT_ACCESSION],
        "sequence_genes": [VALID_GENE],
    }
}

TEST_POST_MAPPED_METADATA_WITH_EXPRESSION = {
    "genomic": {
        "sequence_id": "ga4gh:SQ.em9khDCUYXrVWBfWr9r8fjBUrTjj1aig",
        "sequence_type": "dna",
        "sequence_accessions": [VALID_NT_ACCESSION],
        "sequence_genes": [VALID_GENE],
    }
}

TEST_SEQ_SCORESET_VARIANT_MAPPING_SCAFFOLD = {
    "metadata": {},
    "reference_sequences": {
        "TEST1": {
            "g": {
                "computed_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.ref_test",
                    "sequence": "ACGTTT",
                },
                "mapped_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.map_test",
                    "sequence_accessions": [VALID_CHR_ACCESSION],
                },
            },
            "c": {
                "mapped_reference_sequence": {
                    "sequence_accessions": [VALID_NT_ACCESSION],
                },
            },
        }
    },
    "mapped_scores": [],
    "vrs_version": "2.0",
    "dcd_mapping_version": "pytest.0.0",
    "mapped_date_utc": datetime.isoformat(datetime.now()),
}

TEST_ACC_SCORESET_VARIANT_MAPPING_SCAFFOLD = {
    "metadata": {},
    "reference_sequences": {
        "TEST2": {
            "g": {
                "computed_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.ref_test",
                    "sequence": "ACGTTT",
                },
                "mapped_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.map_test",
                    "sequence_accessions": [VALID_CHR_ACCESSION],
                },
            },
            "c": {
                "mapped_reference_sequence": {
                    "sequence_accessions": [VALID_NT_ACCESSION],
                },
            },
        }
    },
    "mapped_scores": [],
    "vrs_version": "2.0",
    "dcd_mapping_version": "pytest.0.0",
    "mapped_date_utc": datetime.isoformat(datetime.now()),
}

TEST_MULTI_TARGET_SCORESET_VARIANT_MAPPING_SCAFFOLD = {
    "metadata": {},
    "reference_sequences": {
        "TEST3": {
            "g": {
                "computed_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.ref_test3",
                    "sequence": "ACGTTT",
                },
                "mapped_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.map_test",
                    "sequence_accessions": [VALID_CHR_ACCESSION],
                },
            },
            "c": {
                "mapped_reference_sequence": {
                    "sequence_accessions": [VALID_NT_ACCESSION],
                },
            },
        },
        "TEST4": {
            "g": {
                "computed_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.ref_test4",
                    "sequence": "TAATGCC",
                },
                "mapped_reference_sequence": {
                    "sequence_type": "dna",
                    "sequence_id": "ga4gh:SQ.map_test",
                    "sequence_accessions": [VALID_CHR_ACCESSION],
                },
            },
            "c": {
                "mapped_reference_sequence": {
                    "sequence_accessions": [VALID_NT_ACCESSION],
                },
            },
        },
    },
    "mapped_scores": [],
    "vrs_version": "2.0",
    "dcd_mapping_version": "pytest.0.0",
    "mapped_date_utc": datetime.isoformat(datetime.now()),
}


TEST_MINIMAL_VARIANT = {
    "data": {
        "count_data": {},
        "score_data": {"sd": 0.100412839533719, "se": 0.0409933700802629, "score": 0.406972991738182},
    },
    "hgvs_nt": "c.[197A>G;472T>C]",
    "creation_date": datetime.date(datetime.now()),
    "modification_date": datetime.date(datetime.now()),
}


TEST_MINIMAL_MAPPED_VARIANT = {
    "pre_mapped": {},
    "post_mapped": {},
    "modification_date": datetime.date(datetime.now()),
    "mapped_date": datetime.date(datetime.now()),
    "current": True,
    "vrs_version": "2.0",
    "mapping_api_version": "pytest.0.0",
}


TEST_MINIMAL_MAPPED_VARIANT_CREATE = {**TEST_MINIMAL_MAPPED_VARIANT, "clinical_controls": [], "gnomad_variants": []}

TEST_POST_MAPPED_VRS_WITH_HGVS_G_EXPRESSION = {
    "id": "ga4gh:VA.fRW7u-kBQnAKitu1PoDMLvlECWZTHCos",
    "type": "Allele",
    "state": {"type": "LiteralSequenceExpression", "sequence": "G"},
    "digest": "fRW7u-kBQnAKitu1PoDMLvlECWZTHCos",
    "location": {
        "id": "ga4gh:SL.99b3WBaSSmaSTs6YmJfIhl1ZDCV07VZY",
        "end": 23536836,
        "type": "SequenceLocation",
        "start": 23536835,
        "digest": "99b3WBaSSmaSTs6YmJfIhl1ZDCV07VZY",
        "sequenceReference": {
            "type": "SequenceReference",
            "label": "NC_000018.10",
            "refgetAccession": "SQ.vWwFhJ5lQDMhh-czg06YtlWqu0lvFAZV",
        },
    },
    "extensions": [{"name": "vrs_ref_allele_seq", "type": "Extension", "value": "C"}],
    "expressions": [{"value": "NC_000018.10:g.23536836C>G", "syntax": "hgvs.g"}],
}

TEST_POST_MAPPED_VRS_WITH_HGVS_P_EXPRESSION = {
    "id": "ga4gh:VA.zkOAzZK5qG0D0mkJUfXlK1aS075OGSjh",
    "type": "Allele",
    "state": {"type": "LiteralSequenceExpression", "sequence": "R"},
    "digest": "zkOAzZK5qG0D0mkJUfXlK1aS075OGSjh",
    "location": {
        "id": "ga4gh:SL.uUyRpJbrPttRThL7A2zeWAnTcb_7f1R2",
        "end": 116,
        "type": "SequenceLocation",
        "start": 115,
        "digest": "uUyRpJbrPttRThL7A2zeWAnTcb_7f1R2",
        "sequenceReference": {"type": "SequenceReference", "refgetAccession": "SQ.StlJo3M4b8cS253ufe9nPpWqQHBDOSPs"},
    },
    "extensions": [{"name": "vrs_ref_allele_seq", "type": "Extension", "value": "Q"}],
    "expressions": [{"value": "NP_002746.1:p.Gln116Arg", "syntax": "hgvs.p"}],
}

TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION = {
    "pre_mapped": {},
    "post_mapped": TEST_POST_MAPPED_VRS_WITH_HGVS_G_EXPRESSION,
    "vep_functional_consequence": "missense_variant",
    "modification_date": datetime.isoformat(datetime.now()),
    "mapped_date": datetime.isoformat(datetime.now()),
    "current": True,
    "vrs_version": "2.0",
    "mapping_api_version": "pytest.0.0",
}

TEST_MAPPED_VARIANT_WITH_HGVS_P_EXPRESSION = {
    "pre_mapped": {},
    "post_mapped": TEST_POST_MAPPED_VRS_WITH_HGVS_P_EXPRESSION,
    "vep_functional_consequence": "missense_variant",
    "modification_date": datetime.isoformat(datetime.now()),
    "mapped_date": datetime.isoformat(datetime.now()),
    "current": True,
    "vrs_version": "2.0",
    "mapping_api_version": "pytest.0.0",
}

TEST_BASELINE_SCORE = 1.0


TEST_ACMG_BS3_STRONG_CLASSIFICATION = {
    "criterion": "BS3",
    "evidence_strength": "strong",
}

TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION = {
    "recordType": "ACMGClassification",
    **{camelize(k): v for k, v in TEST_ACMG_BS3_STRONG_CLASSIFICATION.items()},
}

TEST_ACMG_PS3_STRONG_CLASSIFICATION = {
    "criterion": "PS3",
    "evidence_strength": "strong",
}

TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION = {
    "recordType": "ACMGClassification",
    **{camelize(k): v for k, v in TEST_ACMG_PS3_STRONG_CLASSIFICATION.items()},
}


TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS = {
    "criterion": "BS3",
    "evidence_strength": "strong",
    "points": -4,
}

TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS = {
    "recordType": "ACMGClassification",
    **{camelize(k): v for k, v in TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS.items()},
}

TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS = {
    "criterion": "PS3",
    "evidence_strength": "strong",
    "points": 4,
}

TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS = {
    "recordType": "ACMGClassification",
    **{camelize(k): v for k, v in TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS.items()},
}

TEST_BS3_STRONG_ODDS_PATH_RATIO = 0.052
TEST_PS3_STRONG_ODDS_PATH_RATIO = 18.7


TEST_FUNCTIONAL_RANGE_NORMAL = {
    "label": "test normal functional range",
    "description": "A normal functional range",
    "classification": "normal",
    "range": [1.0, 5.0],
    "acmg_classification": TEST_ACMG_BS3_STRONG_CLASSIFICATION,
    "oddspaths_ratio": TEST_BS3_STRONG_ODDS_PATH_RATIO,
    "inclusive_lower_bound": True,
    "inclusive_upper_bound": False,
}


TEST_SAVED_FUNCTIONAL_RANGE_NORMAL = {
    "recordType": "FunctionalRange",
    **{camelize(k): v for k, v in TEST_FUNCTIONAL_RANGE_NORMAL.items() if k not in ("acmg_classification",)},
    "acmgClassification": TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION,
}


TEST_FUNCTIONAL_RANGE_ABNORMAL = {
    "label": "test abnormal functional range",
    "description": "An abnormal functional range",
    "classification": "abnormal",
    "range": [-5.0, -1.0],
    "acmg_classification": TEST_ACMG_PS3_STRONG_CLASSIFICATION,
    "oddspaths_ratio": TEST_PS3_STRONG_ODDS_PATH_RATIO,
    "inclusive_lower_bound": True,
    "inclusive_upper_bound": False,
}


TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL = {
    "recordType": "FunctionalRange",
    **{camelize(k): v for k, v in TEST_FUNCTIONAL_RANGE_ABNORMAL.items() if k not in ("acmg_classification",)},
    "acmgClassification": TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION,
}


TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED = {
    "label": "test not specified functional range",
    "classification": "not_specified",
    "range": [-1.0, 1.0],
    "inclusive_lower_bound": True,
    "inclusive_upper_bound": False,
}


TEST_SAVED_FUNCTIONAL_RANGE_NOT_SPECIFIED = {
    "recordType": "FunctionalRange",
    **{camelize(k): v for k, v in TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED.items()},
}


TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY = {
    "label": "test functional range including negative infinity",
    "description": "A functional range including negative infinity",
    "classification": "not_specified",
    "range": [None, 0.0],
    "inclusive_lower_bound": False,
    "inclusive_upper_bound": False,
}


TEST_SAVED_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY = {
    "recordType": "FunctionalRange",
    **{camelize(k): v for k, v in TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY.items()},
}


TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY = {
    "label": "test functional range including positive infinity",
    "description": "A functional range including positive infinity",
    "classification": "not_specified",
    "range": [0.0, None],
    "inclusive_lower_bound": False,
    "inclusive_upper_bound": False,
}


TEST_MINIMAL_CALIBRATION = {
    "title": "Test BRNICH Score Calibration",
    "research_use_only": False,
    "investigator_provided": False,
    "functional_ranges": [
        TEST_FUNCTIONAL_RANGE_NORMAL,
        TEST_FUNCTIONAL_RANGE_ABNORMAL,
        TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
    ],
    "threshold_sources": [],
    "classification_sources": [],
    "method_sources": [],
    "calibration_metadata": {},
}


TEST_BRNICH_SCORE_CALIBRATION = {
    "title": "Test BRNICH Score Calibration",
    "research_use_only": False,
    "baseline_score": TEST_BASELINE_SCORE,
    "baseline_score_description": "Test baseline score description",
    "functional_ranges": [
        TEST_FUNCTIONAL_RANGE_NORMAL,
        TEST_FUNCTIONAL_RANGE_ABNORMAL,
        TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
    ],
    "threshold_sources": [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}],
    "classification_sources": [
        {"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"},
        {"identifier": TEST_BIORXIV_IDENTIFIER, "db_name": "bioRxiv"},
    ],
    "method_sources": [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}],
    "calibration_metadata": {},
}

TEST_SAVED_BRNICH_SCORE_CALIBRATION = {
    "recordType": "ScoreCalibration",
    **{
        camelize(k): v
        for k, v in TEST_BRNICH_SCORE_CALIBRATION.items()
        if k not in ("functional_ranges", "classification_sources", "threshold_sources", "method_sources")
    },
    "functionalRanges": [
        TEST_SAVED_FUNCTIONAL_RANGE_NORMAL,
        TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL,
        TEST_SAVED_FUNCTIONAL_RANGE_NOT_SPECIFIED,
    ],
    "thresholdSources": [SAVED_PUBMED_PUBLICATION],
    "classificationSources": [SAVED_PUBMED_PUBLICATION, SAVED_BIORXIV_PUBLICATION],
    "methodSources": [SAVED_PUBMED_PUBLICATION],
    "id": 1,
    "urn": VALID_CALIBRATION_URN,
    "investigatorProvided": True,
    "primary": True,
    "private": False,
    "scoreSetId": 1,
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
}

TEST_PATHOGENICITY_SCORE_CALIBRATION = {
    "title": "Test Pathogenicity Score Calibration",
    "research_use_only": False,
    "baseline_score": TEST_BASELINE_SCORE,
    "baseline_score_description": "Test baseline score description",
    "functional_ranges": [
        TEST_FUNCTIONAL_RANGE_NORMAL,
        TEST_FUNCTIONAL_RANGE_ABNORMAL,
    ],
    "threshold_sources": [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}],
    "classification_sources": None,
    "method_sources": None,
    "calibration_metadata": {},
}

TEST_SAVED_PATHOGENICITY_SCORE_CALIBRATION = {
    "recordType": "ScoreCalibration",
    **{
        camelize(k): v
        for k, v in TEST_PATHOGENICITY_SCORE_CALIBRATION.items()
        if k not in ("functional_ranges", "classification_sources", "threshold_sources", "method_sources")
    },
    "functionalRanges": [
        TEST_SAVED_FUNCTIONAL_RANGE_NORMAL,
        TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL,
    ],
    "thresholdSources": [SAVED_PUBMED_PUBLICATION],
    "classificationSources": None,
    "methodSources": None,
    "id": 2,
    "investigatorProvided": True,
    "primary": False,
    "private": False,
    "urn": VALID_CALIBRATION_URN,
    "scoreSetId": 1,
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


TEST_CLINGEN_ALLELE_OBJECT = {
    "@context": "http://reg.genome.network/schema/allele.jsonld",
    "@id": "http://reg.genome.network/allele/CA341478553",
    "communityStandardTitle": ["NM_001408.3(CELSR2):c.1A>G (p.Met1Val)"],
    "externalRecords": {
        "MyVariantInfo_hg19": [
            {"@id": "http://myvariant.info/v1/variant/chr1:g.109792702A>G?assembly=hg19", "id": "chr1:g.109792702A>G"}
        ],
        "MyVariantInfo_hg38": [
            {"@id": "http://myvariant.info/v1/variant/chr1:g.109250080A>G?assembly=hg38", "id": "chr1:g.109250080A>G"}
        ],
        "gnomAD_4": [
            {
                "@id": "http://gnomad.broadinstitute.org/variant/1-109250080-A-G?dataset=gnomad_r4",
                "id": "1-109250080-A-G",
                "variant": "1:109250080 A / G",
            }
        ],
    },
    "genomicAlleles": [
        {
            "chromosome": "1",
            "coordinates": [{"allele": "G", "end": 109250080, "referenceAllele": "A", "start": 109250079}],
            "hgvs": ["NC_000001.11:g.109250080A>G", "CM000663.2:g.109250080A>G"],
            "referenceGenome": "GRCh38",
            "referenceSequence": "http://reg.genome.network/refseq/RS000049",
        },
        {
            "chromosome": "1",
            "coordinates": [{"allele": "G", "end": 109792702, "referenceAllele": "A", "start": 109792701}],
            "hgvs": ["NC_000001.10:g.109792702A>G", "CM000663.1:g.109792702A>G"],
            "referenceGenome": "GRCh37",
            "referenceSequence": "http://reg.genome.network/refseq/RS000025",
        },
        {
            "chromosome": "1",
            "coordinates": [{"allele": "G", "end": 109594225, "referenceAllele": "A", "start": 109594224}],
            "hgvs": ["NC_000001.9:g.109594225A>G"],
            "referenceGenome": "NCBI36",
            "referenceSequence": "http://reg.genome.network/refseq/RS000001",
        },
        {
            "coordinates": [{"allele": "G", "end": 5376, "referenceAllele": "A", "start": 5375}],
            "hgvs": ["NG_052669.1:g.5376A>G"],
            "referenceSequence": "http://reg.genome.network/refseq/RS617150",
        },
    ],
    "transcriptAlleles": [
        {
            "coordinates": [{"allele": "G", "end": 542, "referenceAllele": "A", "start": 541}],
            "gene": "http://reg.genome.network/gene/GN003231",
            "geneNCBI_id": 1952,
            "geneSymbol": "CELSR2",
            "hgvs": ["ENST00000271332.4:c.1A>G", TEST_HGVS_IDENTIFIER],
            "proteinEffect": {"hgvs": "ENSP00000271332.3:p.Met1Val", "hgvsWellDefined": "ENSP00000271332.3:p.Met1Val"},
            "referenceSequence": "http://reg.genome.network/refseq/RS743193",
            "MANE": {
                "maneVersion": "1.3",
                "maneStatus": "MANE Select",
                "nucleotide": {
                    "Ensembl": {"hgvs": "ENST00000271332.4:c.1A>G"},
                    "RefSeq": {"hgvs": "NM_001408.3:c.1A>G"},
                },
                "protein": {
                    "Ensembl": {"hgvs": "ENSP00000271332.3:p.Met1Val"},
                    "RefSeq": {"hgvs": "NP_001399.1:p.Met1Val"},
                },
            },
        },
        {
            "coordinates": [{"allele": "G", "end": 62, "referenceAllele": "A", "start": 61}],
            "gene": "http://reg.genome.network/gene/GN003231",
            "geneNCBI_id": 1952,
            "geneSymbol": "CELSR2",
            "hgvs": ["ENST00000271332.3:c.1A>G"],
            "proteinEffect": {"hgvs": "ENSP00000271332.3:p.Met1Val", "hgvsWellDefined": "ENSP00000271332.3:p.Met1Val"},
            "referenceSequence": "http://reg.genome.network/refseq/RS252294",
        },
        {
            "coordinates": [{"allele": "G", "end": 62, "referenceAllele": "A", "start": 61}],
            "gene": "http://reg.genome.network/gene/GN003231",
            "geneNCBI_id": 1952,
            "geneSymbol": "CELSR2",
            "hgvs": ["NM_001408.2:c.1A>G"],
            "proteinEffect": {"hgvs": "NP_001399.1:p.Met1Val", "hgvsWellDefined": "NP_001399.1:p.Met1Val"},
            "referenceSequence": "http://reg.genome.network/refseq/RS026793",
        },
        {
            "coordinates": [{"allele": "G", "end": 366, "referenceAllele": "A", "start": 365}],
            "gene": "http://reg.genome.network/gene/GN003231",
            "geneNCBI_id": 1952,
            "geneSymbol": "CELSR2",
            "hgvs": ["XM_005270580.3:c.1A>G"],
            "proteinEffect": {"hgvs": "XP_005270637.1:p.Met1Val", "hgvsWellDefined": "XP_005270637.1:p.Met1Val"},
            "referenceSequence": "http://reg.genome.network/refseq/RS066576",
        },
        {
            "coordinates": [{"allele": "G", "end": 542, "referenceAllele": "A", "start": 541}],
            "gene": "http://reg.genome.network/gene/GN003231",
            "geneNCBI_id": 1952,
            "geneSymbol": "CELSR2",
            "hgvs": ["NM_001408.3:c.1A>G"],
            "proteinEffect": {"hgvs": "NP_001399.1:p.Met1Val", "hgvsWellDefined": "NP_001399.1:p.Met1Val"},
            "referenceSequence": "http://reg.genome.network/refseq/RS664974",
            "MANE": {
                "maneVersion": "1.3",
                "maneStatus": "MANE Select",
                "nucleotide": {
                    "Ensembl": {"hgvs": "ENST00000271332.4:c.1A>G"},
                    "RefSeq": {"hgvs": "NM_001408.3:c.1A>G"},
                },
                "protein": {
                    "Ensembl": {"hgvs": "ENSP00000271332.3:p.Met1Val"},
                    "RefSeq": {"hgvs": "NP_001399.1:p.Met1Val"},
                },
            },
        },
    ],
    "type": "nucleotide",
}


TEST_UNIPROT_SWISS_PROT_TYPE = "UniProtKB reviewed (Swiss-Prot)"
TEST_UNIPROT_TREMBL_TYPE = "UniProtKB unreviewed (TrEMBL)"
TEST_UNIPROT_JOB_ID = "1234567890"

TEST_UNIPROT_JOB_SUBMISSION_RESPONSE = {
    "jobId": TEST_UNIPROT_JOB_ID,
    "message": "Job submitted successfully",
}

TEST_UNIPROT_JOB_SUBMISSION_ERROR_RESPONSE = {
    "url": "http://rest.uniprot.org/idmapping/run",
    "messages": [
        "The parameter 'from' has an invalid value '{0}'.",
        "'to' is a required parameter",
        "'ids' is a required parameter",
        "The parameter 'to' has an invalid value '{0}'.",
        "'from' is a required parameter",
        "The combination of 'from={0}' and 'to={1}' parameters is invalid",
    ],
}


TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE = {
    "results": [
        {
            "from": f"{VALID_NT_ACCESSION}",
            "to": {"primaryAccession": f"{VALID_UNIPROT_ACCESSION}", "entryType": TEST_UNIPROT_SWISS_PROT_TYPE},
        },
    ]
}


TEST_UNIPROT_ID_MAPPING_TREMBL_RESPONSE = {
    "results": [
        {
            "from": f"{VALID_NT_ACCESSION}",
            "to": {"primaryAccession": f"{VALID_UNIPROT_ACCESSION}", "entryType": TEST_UNIPROT_TREMBL_TYPE},
        },
    ]
}


TEST_UNIPROT_ID_MAPPING_COMBINED_RESPONSE = {
    "results": [
        TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE["results"][0],
        TEST_UNIPROT_ID_MAPPING_TREMBL_RESPONSE["results"][0],
    ]
}

TEST_UNIPROT_ID_FAILED_ID_MAPPING_RESPONSE = {"failedIds": [VALID_NT_ACCESSION]}


TEST_UNIPROT_FINISHED_JOB_STATUS_RESPONSE = {
    "jobStatus": "FINISHED",
    "warnings": [{"code": 0, "message": "string"}],
    "errors": [{"code": 0, "message": "string"}],
    "start": datetime.now().isoformat(),
    "totalEntries": 1,
    "processedEntries": 1,
    "lastUpdated": datetime.now().isoformat(),
}


TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE = {
    "jobStatus": "RUNNING",
    "warnings": [{"code": 0, "message": "string"}],
    "errors": [{"code": 0, "message": "string"}],
    "start": datetime.now().isoformat(),
    "totalEntries": 21,
    "processedEntries": 12,
    "lastUpdated": datetime.now().isoformat(),
}


TEST_UNIPROT_REDIRECT_RESPONSE = {
    "from": "Refseq_pro",
    "to": "UniProtKB",
    "ids": [VALID_NT_ACCESSION],
    "taxId": "homo sapiens",
    "redirectURL": "https://redirect.url",
    "warnings": [{"code": 0, "message": "string"}],
    "errors": [{"code": 0, "message": "string"}],
}

TEST_GNOMAD_LOCUS_CONTIG = "chr10"
TEST_GNOMAD_LOCUS_POSITION = "87961093"
TEST_GNOMAD_ALLELES = "[A, G]"
TEST_GNOMAD_DATA_VERSION = "v1.pytest"
TEST_GNOMAD_ALLELE_COUNT = "3"
TEST_GNOMAD_ALLELE_NUMBER = "1613510"
TEST_GNOMAD_ALLELE_FREQUENCY = float(float(TEST_GNOMAD_ALLELE_COUNT) / float(TEST_GNOMAD_ALLELE_NUMBER))
TEST_GNOMAD_FAF95_MAX = "6.800000000000001e-07"
TEST_GNOMAD_FAF95_MAX_ANCESTRY = "nfe"


TEST_MAVEDB_ATHENA_ROW = {
    "locus.contig": TEST_GNOMAD_LOCUS_CONTIG,
    "locus.position": TEST_GNOMAD_LOCUS_POSITION,
    "alleles": TEST_GNOMAD_ALLELES,
    "caid": VALID_CLINGEN_CA_ID,
    "joint.freq.all.ac": TEST_GNOMAD_ALLELE_COUNT,
    "joint.freq.all.an": TEST_GNOMAD_ALLELE_NUMBER,
    "joint.fafmax.faf95_max_gen_anc": TEST_GNOMAD_FAF95_MAX_ANCESTRY,
    "joint.fafmax.faf95_max": TEST_GNOMAD_FAF95_MAX,
}

TEST_GNOMAD_VARIANT = {
    "db_name": "gnomAD",
    "db_identifier": f"10-{TEST_GNOMAD_LOCUS_POSITION}-A-G",
    "db_version": TEST_GNOMAD_DATA_VERSION,
    "allele_count": int(TEST_GNOMAD_ALLELE_COUNT),
    "allele_number": int(TEST_GNOMAD_ALLELE_NUMBER),
    "allele_frequency": TEST_GNOMAD_ALLELE_FREQUENCY,
    "faf95_max": float(TEST_GNOMAD_FAF95_MAX),
    "faf95_max_ancestry": TEST_GNOMAD_FAF95_MAX_ANCESTRY,
    "creation_date": date.today().isoformat(),
    "modification_date": date.today().isoformat(),
}

TEST_SAVED_GNOMAD_VARIANT = {
    "dbName": "gnomAD",
    "dbIdentifier": f"10-{TEST_GNOMAD_LOCUS_POSITION}-A-G",
    "dbVersion": TEST_GNOMAD_DATA_VERSION,
    "alleleCount": int(TEST_GNOMAD_ALLELE_COUNT),
    "alleleNumber": int(TEST_GNOMAD_ALLELE_NUMBER),
    "alleleFrequency": TEST_GNOMAD_ALLELE_FREQUENCY,
    "faf95Max": float(TEST_GNOMAD_FAF95_MAX),
    "faf95MaxAncestry": TEST_GNOMAD_FAF95_MAX_ANCESTRY,
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "recordType": "GnomADVariantWithMappedVariants",
    "id": 1,  # Presuming this is the only gnomAD variant in the database
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
