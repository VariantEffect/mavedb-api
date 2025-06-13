import pytest

from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_set import ScoreSetCreate, ScoreSetModify
from mavedb.view_models.target_gene import TargetGeneCreate
from tests.helpers.constants import TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_SEQ_SCORESET
from humps import camelize

from mavedb.view_models.publication_identifier import PublicationIdentifier
from mavedb.view_models.score_set import SavedScoreSet
from mavedb.view_models.target_gene import SavedTargetGene

from tests.helpers.constants import (
    VALID_EXPERIMENT_URN,
    TEST_MINIMAL_SEQ_SCORESET_RESPONSE,
    TEST_PUBMED_IDENTIFIER,
    TEST_BIORXIV_IDENTIFIER,
    SAVED_PUBMED_PUBLICATION,
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


def test_cannot_create_score_set_with_too_many_boundaries():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1, 2.0)},
            {"label": "range_2", "classification": "abnormal", "range": (2.0, 2.1, 2.3)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Tuple should have at most 2 items" in str(exc_info.value)


def test_cannot_create_score_set_with_overlapping_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1.1)},
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_can_create_score_set_with_mixed_range_types():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
            {"label": "range_2", "classification": "abnormal", "range": ("1.1", 2.1)},
            {"label": "range_3", "classification": "abnormal", "range": (2.2, "3.2")},
        ],
    }

    ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_adjacent_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
        ],
    }

    ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_flipped_adjacent_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
        ],
    }

    ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_adjacent_negative_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_flipped_adjacent_negative_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
        ],
    }

    ScoreSetModify(**score_set_test)


def test_cannot_create_score_set_with_overlapping_upper_unbounded_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, None)},
            {"label": "range_2", "classification": "abnormal", "range": (1, None)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_cannot_create_score_set_with_overlapping_lower_unbounded_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (None, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (None, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_cannot_create_score_set_with_backwards_bounds():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (2, 1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "The lower bound of the score range may not be larger than the upper bound." in str(exc_info.value)


def test_cannot_create_score_set_with_equal_bounds():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 1,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "The lower and upper bound of the score range may not be the same." in str(exc_info.value)


def test_cannot_create_score_set_with_duplicate_range_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_1", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Detected repeated label: `range_1`. Range labels must be unique." in str(exc_info.value)


def test_cannot_create_score_set_with_duplicate_range_labels_whitespace():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "     range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_1       ", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Detected repeated label: `range_1`. Range labels must be unique." in str(exc_info.value)


def test_cannot_create_score_set_with_wild_type_outside_ranges():
    wt_score = 0.5
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert (
        f"The provided wild type score of {wt_score} is not within any of the provided normal ranges. This score should be within a normal range."
        in str(exc_info.value)
    )


def test_cannot_create_score_set_with_wild_type_outside_normal_range():
    wt_score = -1.5
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert (
        f"The provided wild type score of {wt_score} is not within any of the provided normal ranges. This score should be within a normal range."
        in str(exc_info.value)
    )


def test_cannot_create_score_set_with_wild_type_score_and_no_normal_range():
    wt_score = -0.5
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": "abnormal", "range": (-1, 0)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "A wild type score has been provided, but no normal classification range exists." in str(exc_info.value)


def test_cannot_create_score_set_with_normal_range_and_no_wild_type_score():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": None,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "A normal range has been provided, but no wild type score has been provided." in str(exc_info.value)


def test_cannot_create_score_set_without_default_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "other", "range": (-1, 0)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test)

    assert "Input should be 'normal', 'abnormal' or 'not_specified'" in str(exc_info.value)


@pytest.mark.parametrize("classification", ["normal", "abnormal", "not_specified"])
def test_can_create_score_set_with_any_range_classification(classification):
    wt_score = -0.5 if classification == "normal" else None
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": classification, "range": (-1, 0)},
        ],
    }

    ScoreSetModify(**score_set_test)


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


def test_saved_score_set_data_set_columns_are_camelized():
    score_set = TEST_MINIMAL_SEQ_SCORESET_RESPONSE.copy()
    score_set["urn"] = "urn:score-set-xxx"

    # Remove pre-set synthetic properties
    score_set.pop("metaAnalyzesScoreSetUrns")
    score_set.pop("metaAnalyzedByScoreSetUrns")
    score_set.pop("primaryPublicationIdentifiers")
    score_set.pop("secondaryPublicationIdentifiers")
    score_set.pop("datasetColumns")

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

    # The camelized dataset columns we are testing
    score_set["dataset_columns"] = {"camelize_me": "test", "noNeed": "test"}

    score_set_attributed_object = dummy_attributed_object_from_dict(score_set)
    saved_score_set = SavedScoreSet.model_validate(score_set_attributed_object)

    assert sorted(list(saved_score_set.dataset_columns.keys())) == sorted(
        [camelize(k) for k in score_set["dataset_columns"].keys()]
    )


@pytest.mark.parametrize(
    "exclude",
    ["publication_identifier_associations", "meta_analyzes_score_sets", "meta_analyzed_by_score_sets"],
)
def test_cannot_create_saved_score_set_without_all_attributed_properties(exclude):
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

    assert "Unable to create SavedScoreSet without attribute" in str(exc_info.value)
    assert exclude in str(exc_info.value)
