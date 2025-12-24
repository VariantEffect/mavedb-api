from copy import deepcopy

import pytest

from mavedb.view_models.publication_identifier import PublicationIdentifier, PublicationIdentifierCreate
from mavedb.view_models.score_set import (
    SavedScoreSet,
    ScoreSet,
    ScoreSetCreate,
    ScoreSetModify,
    ScoreSetUpdateAllOptional,
)
from mavedb.view_models.target_gene import SavedTargetGene, TargetGeneCreate
from tests.helpers.constants import (
    EXTRA_LICENSE,
    EXTRA_USER,
    SAVED_PUBMED_PUBLICATION,
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION,
    TEST_CROSSREF_IDENTIFIER,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET_RESPONSE,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_PUBMED_IDENTIFIER,
    VALID_EXPERIMENT_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_SCORE_SET_URN,
    VALID_TMP_URN,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict


def test_can_create_score_set():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set = ScoreSetCreate(**score_set_test)

    assert score_set.title == "Test Score Set Title"
    assert score_set.short_description == "Test score set"
    assert score_set.abstract_text == "Abstract"
    assert score_set.method_text == "Methods"


def test_cannot_create_score_set_without_a_target():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test.pop("targetGenes")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test, target_genes=[])

    assert "Score sets should define at least one target." in str(exc_info.value)


def test_cannot_create_score_set_with_multiple_primary_publications():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    target_genes = score_set_test.pop("targetGenes")

    identifier_one = PublicationIdentifierCreate(identifier="2019.12.12.207222")
    identifier_two = PublicationIdentifierCreate(identifier="2019.12.12.20733333")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[TargetGeneCreate(**target) for target in target_genes],
            primary_publication_identifiers=[identifier_one, identifier_two],
        )

    assert "Multiple primary publication identifiers are not allowed" in str(exc_info.value)


def test_cannot_create_score_set_without_target_gene_labels_when_multiple_targets_exist():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    score_set_test.pop("targetGenes")
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be empty when multiple targets are defined." in str(exc_info.value)


def test_cannot_create_score_set_with_non_unique_target_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    non_unique = "BRCA1"
    target_gene_one.target_sequence.label = non_unique
    target_gene_two.target_sequence.label = non_unique

    score_set_test.pop("targetGenes")
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be duplicated." in str(exc_info.value)


def test_cannot_create_score_set_without_a_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("title")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["title"] = " "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["title"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_without_a_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("shortDescription")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["shortDescription"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["shortDescription"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_without_an_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("abstractText")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["abstractText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["abstractText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_without_a_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("methodText")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["methodText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["methodText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


@pytest.mark.parametrize(
    "calibration", [deepcopy(TEST_BRNICH_SCORE_CALIBRATION), deepcopy(TEST_PATHOGENICITY_SCORE_CALIBRATION)]
)
def test_can_create_score_set_with_complete_and_valid_provided_calibrations(calibration):
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["score_calibrations"] = [calibration]

    score_set = ScoreSetCreate.model_validate(score_set_test)

    assert len(score_set.score_calibrations) == 1


def test_can_create_score_set_with_multiple_valid_calibrations():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["score_calibrations"] = [
        deepcopy(TEST_BRNICH_SCORE_CALIBRATION),
        deepcopy(TEST_BRNICH_SCORE_CALIBRATION),
        deepcopy(TEST_PATHOGENICITY_SCORE_CALIBRATION),
    ]

    score_set = ScoreSetCreate.model_validate(score_set_test)

    assert len(score_set.score_calibrations) == 3


def test_cannot_create_score_set_with_inconsistent_base_editor_flags():
    score_set_test = TEST_MINIMAL_ACC_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    target_gene_one.target_accession.is_base_editor = True
    target_gene_two.target_accession.is_base_editor = False

    score_set_test["targetGenes"] = [target_gene_one, target_gene_two]
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "All target accessions must be of the same base editor type." in str(exc_info.value)


def test_saved_score_set_synthetic_properties():
    score_set = TEST_MINIMAL_SEQ_SCORESET_RESPONSE.copy()
    score_set["urn"] = "urn:score-set-xxx"

    # Remove pre-set synthetic properties
    score_set.pop("metaAnalyzesScoreSetUrns")
    score_set.pop("metaAnalyzedByScoreSetUrns")
    score_set.pop("primaryPublicationIdentifiers")
    score_set.pop("secondaryPublicationIdentifiers")

    # Convert fields expecting an object to attributed objects
    external_identifiers = {"refseq_offset": None, "ensembl_offset": None, "uniprot_offset": None}
    target_genes = [
        dummy_attributed_object_from_dict({**target, **external_identifiers}) for target in score_set["targetGenes"]
    ]
    score_set["targetGenes"] = [SavedTargetGene.model_validate(target) for target in target_genes]

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    score_set["meta_analyzes_score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:meta-analyzes-xxx", "superseding_score_set": None})
    ]
    score_set["meta_analyzed_by_score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:meta-analyzed-xxx", "superseding_score_set": None})
    ]
    score_set["publication_identifier_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(**SAVED_PUBMED_PUBLICATION),
                "primary": True,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
    ]

    score_set_attributed_object = dummy_attributed_object_from_dict(score_set)
    saved_score_set = SavedScoreSet.model_validate(score_set_attributed_object)

    # meta_analyzes_score_sets
    assert len(saved_score_set.meta_analyzes_score_set_urns) == 1
    assert all([urn == "urn:meta-analyzes-xxx" for urn in saved_score_set.meta_analyzes_score_set_urns])
    # meta_analyzed_by_score_sets
    assert len(saved_score_set.meta_analyzed_by_score_set_urns) == 1
    assert all([urn == "urn:meta-analyzed-xxx" for urn in saved_score_set.meta_analyzed_by_score_set_urns])

    # primary_publication_identifiers, secondary_publication_identifiers
    assert len(saved_score_set.primary_publication_identifiers) == 1
    assert len(saved_score_set.secondary_publication_identifiers) == 2
    assert all(
        [
            publication.identifier == TEST_PUBMED_IDENTIFIER
            for publication in saved_score_set.primary_publication_identifiers
        ]
    )
    assert all(
        [
            publication.identifier == TEST_BIORXIV_IDENTIFIER
            for publication in saved_score_set.secondary_publication_identifiers
        ]
    )


@pytest.mark.parametrize(
    "attribute,updated_data",
    [
        ("title", "Updated Title"),
        ("method_text", "Updated Method Text"),
        ("abstract_text", "Updated Abstract Text"),
        ("short_description", "Updated Abstract Text"),
        ("title", "Updated Title"),
        ("extra_metadata", {"updated": "metadata"}),
        ("data_usage_policy", "data_usage_policy"),
        ("contributors", [{"orcid_id": EXTRA_USER["username"]}]),
        ("primary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}]),
        ("secondary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}]),
        ("doi_identifiers", [{"identifier": TEST_CROSSREF_IDENTIFIER}]),
        ("license_id", EXTRA_LICENSE["id"]),
        ("target_genes", TEST_MINIMAL_SEQ_SCORESET["targetGenes"]),
    ],
)
def test_score_set_update_all_optional(attribute, updated_data):
    ScoreSetUpdateAllOptional(**{attribute: updated_data})


@pytest.mark.parametrize(
    "exclude,expected_missing_fields",
    [
        ("publication_identifier_associations", ["primaryPublicationIdentifiers", "secondaryPublicationIdentifiers"]),
        ("meta_analyzes_score_sets", ["metaAnalyzesScoreSetUrns"]),
        ("meta_analyzed_by_score_sets", ["metaAnalyzedByScoreSetUrns"]),
    ],
)
def test_cannot_create_saved_score_set_without_all_attributed_properties(exclude, expected_missing_fields):
    score_set = TEST_MINIMAL_SEQ_SCORESET_RESPONSE.copy()
    score_set["urn"] = "urn:score-set-xxx"

    # Remove pre-set synthetic properties
    score_set.pop("metaAnalyzesScoreSetUrns")
    score_set.pop("metaAnalyzedByScoreSetUrns")
    score_set.pop("primaryPublicationIdentifiers")
    score_set.pop("secondaryPublicationIdentifiers")

    # Convert fields expecting an object to attributed objects
    external_identifiers = {"refseq_offset": None, "ensembl_offset": None, "uniprot_offset": None}
    target_genes = [
        dummy_attributed_object_from_dict({**target, **external_identifiers}) for target in score_set["targetGenes"]
    ]
    score_set["targetGenes"] = [SavedTargetGene.model_validate(target) for target in target_genes]

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    score_set["meta_analyzes_score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:meta-analyzes-xxx", "superseding_score_set": None})
    ]
    score_set["meta_analyzed_by_score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:meta-analyzed-xxx", "superseding_score_set": None})
    ]
    score_set["publication_identifier_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(**SAVED_PUBMED_PUBLICATION),
                "primary": True,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
    ]

    score_set.pop(exclude)
    score_set_attributed_object = dummy_attributed_object_from_dict(score_set)
    with pytest.raises(ValueError) as exc_info:
        SavedScoreSet.model_validate(score_set_attributed_object)

    assert "Field required" in str(exc_info.value)
    for exclude_field in expected_missing_fields:
        assert exclude_field in str(exc_info.value)


def test_can_create_score_set_with_none_type_superseded_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["superseded_score_set_urn"] = None

    saved_score_set = ScoreSetCreate(**score_set_test)

    assert saved_score_set.superseded_score_set_urn is None


def test_can_create_score_set_with_superseded_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["superseded_score_set_urn"] = VALID_SCORE_SET_URN

    saved_score_set = ScoreSetCreate(**score_set_test)

    assert saved_score_set.superseded_score_set_urn == VALID_SCORE_SET_URN


def test_cant_create_score_set_with_invalid_superseded_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["superseded_score_set_urn"] = "invalid-urn"

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert f"'{score_set_test['superseded_score_set_urn']}' is not a valid score set URN" in str(exc_info.value)


def test_cant_create_score_set_with_tmp_superseded_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["superseded_score_set_urn"] = VALID_TMP_URN

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert "cannot supersede a private score set - please edit it instead" in str(exc_info.value)


def test_can_create_score_set_with_none_type_meta_analyzes_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["meta_analyzes_score_set_urns"] = None

    saved_score_set = ScoreSetCreate(**score_set_test)

    assert saved_score_set.meta_analyzes_score_set_urns is None


def test_can_create_score_set_with_empty_meta_analyzes_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["meta_analyzes_score_set_urns"] = []

    saved_score_set = ScoreSetCreate(**score_set_test)

    assert saved_score_set.meta_analyzes_score_set_urns is None


def test_can_create_score_set_with_meta_analyzes_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["meta_analyzes_score_set_urns"] = [VALID_SCORE_SET_URN]

    saved_score_set = ScoreSetCreate(**score_set_test)
    assert saved_score_set.meta_analyzes_score_set_urns == [VALID_SCORE_SET_URN]


def test_cant_create_score_set_with_invalid_meta_analyzes_score_set_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["meta_analyzes_score_set_urns"] = ["invalid-urn"]

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert f"'{score_set_test['meta_analyzes_score_set_urns'][0]}' is not a valid score set URN" in str(exc_info.value)


def test_cant_create_score_set_with_invalid_experiment_urn():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = "invalid-urn"

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert f"'{score_set_test['experiment_urn']}' is not a valid experiment URN" in str(exc_info.value)


def test_cant_create_score_set_with_experiment_urn_if_is_meta_analysis():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["experiment_urn"] = VALID_EXPERIMENT_URN
    score_set_test["meta_analyzes_score_set_urns"] = [VALID_SCORE_SET_URN]

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert "experiment URN should not be supplied when your score set is a meta-analysis" in str(exc_info.value)


def test_cant_create_score_set_without_experiment_urn_if_not_meta_analysis():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["meta_analyzes_score_set_urns"] = []

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set_test)

    assert "experiment URN is required unless your score set is a meta-analysis" in str(exc_info.value)


def test_can_create_score_set_from_non_orm_context():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET_RESPONSE.copy()
    score_set_test["urn"] = "urn:score-set-xxx"
    score_set_test["experiment"]["urn"] = VALID_EXPERIMENT_URN
    score_set_test["experiment"]["experimentSetUrn"] = VALID_EXPERIMENT_SET_URN

    saved_score_set = ScoreSet.model_validate(score_set_test)

    assert saved_score_set.urn == "urn:score-set-xxx"
