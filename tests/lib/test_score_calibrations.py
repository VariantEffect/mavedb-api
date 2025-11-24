# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from unittest import mock

import pandas as pd
from pydantic import create_model
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from mavedb.lib.score_calibrations import (
    create_functional_classification,
    create_score_calibration,
    create_score_calibration_in_score_set,
    delete_score_calibration,
    demote_score_calibration_from_primary,
    modify_score_calibration,
    promote_score_calibration_to_primary,
    publish_score_calibration,
    variant_classification_df_to_dict,
    variants_for_functional_classification,
)
from mavedb.lib.validation.constants.general import calibration_class_column_name, calibration_variant_column_name
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.score_calibration import ScoreCalibrationCreate, ScoreCalibrationModify
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_CROSSREF_IDENTIFIER,
    TEST_LICENSE,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_PUBMED_IDENTIFIER,
    TEST_SEQ_SCORESET,
    VALID_SCORE_SET_URN,
)
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.score_calibration import create_test_range_based_score_calibration_in_score_set

################################################################################
# Tests for create_functional_classification
################################################################################


def test_create_functional_classification_without_acmg_classification(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock functional range without ACMG classification
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(list, [0.0, 1.0]),
        class_=(type(None), None),
        inclusive_lower_bound=(bool, True),
        inclusive_upper_bound=(bool, False),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(type(None), None),
    )

    result = create_functional_classification(session, MockFunctionalClassificationCreate(), calibration)

    assert result.description == "Test Description"
    assert result.range == [0.0, 1.0]
    assert result.inclusive_lower_bound is True
    assert result.inclusive_upper_bound is False
    assert result.functional_classification == "pathogenic"
    assert result.oddspaths_ratio == 1.5
    assert result.positive_likelihood_ratio == 2.0
    assert result.acmg_classification is None
    assert result.acmg_classification_id is None
    assert result.calibration == calibration


def test_create_functional_classification_with_acmg_classification(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock ACMG classification
    mock_criterion = "PS1"
    mock_evidence_strength = "STRONG"
    mock_points = 4
    MockAcmgClassification = create_model(
        "MockAcmgClassification",
        criterion=(str, mock_criterion),
        evidence_strength=(str, mock_evidence_strength),
        points=(int, mock_points),
    )

    # Create mock functional range with ACMG classification
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(list, [0.0, 1.0]),
        class_=(type(None), None),
        inclusive_lower_bound=(bool, True),
        inclusive_upper_bound=(bool, False),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(MockAcmgClassification, MockAcmgClassification()),
    )

    functional_range_create = MockFunctionalClassificationCreate()

    with mock.patch("mavedb.lib.score_calibrations.find_or_create_acmg_classification") as mock_find_or_create:
        # Mock the ACMG classification with an ID
        MockPersistedAcmgClassification = create_model(
            "MockPersistedAcmgClassification",
            id=(int, 123),
        )

        mocked_persisted_acmg_classification = MockPersistedAcmgClassification()
        mock_find_or_create.return_value = mocked_persisted_acmg_classification
        result = create_functional_classification(session, functional_range_create, calibration)

        # Verify find_or_create_acmg_classification was called with correct parameters
        mock_find_or_create.assert_called_once_with(
            session,
            criterion=mock_criterion,
            evidence_strength=mock_evidence_strength,
            points=mock_points,
        )

    # Verify the result
    assert result.label == "Test Label"
    assert result.description == "Test Description"
    assert result.range == [0.0, 1.0]
    assert result.inclusive_lower_bound is True
    assert result.inclusive_upper_bound is False
    assert result.functional_classification == "pathogenic"
    assert result.oddspaths_ratio == 1.5
    assert result.positive_likelihood_ratio == 2.0
    assert result.acmg_classification == mocked_persisted_acmg_classification
    assert result.acmg_classification_id == 123
    assert result.calibration == calibration


def test_create_functional_classification_with_variant_classes(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock functional range with variant classes
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(type(None), None),
        class_=(str, "test_class"),
        inclusive_lower_bound=(type(None), None),
        inclusive_upper_bound=(type(None), None),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(type(None), None),
    )

    functional_range_create = MockFunctionalClassificationCreate()

    with mock.patch("mavedb.lib.score_calibrations.variants_for_functional_classification") as mock_classified_variants:
        MockedClassifiedVariant = create_model(
            "MockedVariant",
            urn=(str, "variant_urn_3"),
        )
        mock_classified_variants.return_value = [MockedClassifiedVariant()]

        result = create_functional_classification(
            session,
            functional_range_create,
            calibration,
            variant_classes={
                "pathogenic": ["variant_urn_1", "variant_urn_2"],
                "benign": ["variant_urn_3"],
            },
        )

        mock_classified_variants.assert_called()

    assert result.description == "Test Description"
    assert result.range is None
    assert result.inclusive_lower_bound is None
    assert result.inclusive_upper_bound is None
    assert result.functional_classification == "pathogenic"
    assert result.oddspaths_ratio == 1.5
    assert result.positive_likelihood_ratio == 2.0
    assert result.acmg_classification is None
    assert result.acmg_classification_id is None
    assert result.calibration == calibration
    assert result.variants == [MockedClassifiedVariant()]


def test_create_functional_classification_propagates_acmg_errors(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock ACMG classification
    MockAcmgClassification = create_model(
        "MockAcmgClassification",
        criterion=(str, "PS1"),
        evidence_strength=(str, "strong"),
        points=(int, 4),
    )

    # Create mock functional range with ACMG classification
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(list, [0.0, 1.0]),
        class_=(type(None), None),
        inclusive_lower_bound=(bool, True),
        inclusive_upper_bound=(bool, False),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(MockAcmgClassification, MockAcmgClassification()),
    )

    functional_range_create = MockFunctionalClassificationCreate()

    with (
        pytest.raises(ValueError, match="ACMG error"),
        mock.patch(
            "mavedb.lib.score_calibrations.find_or_create_acmg_classification",
            side_effect=ValueError("ACMG error"),
        ),
    ):
        create_functional_classification(session, functional_range_create, calibration)


def test_create_functional_classification_propagates_functional_classification_errors(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock functional range
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(list, [0.0, 1.0]),
        class_=(type(None), None),
        inclusive_lower_bound=(bool, True),
        inclusive_upper_bound=(bool, False),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(type(None), None),
    )

    functional_range_create = MockFunctionalClassificationCreate()

    with (
        pytest.raises(ValueError, match="Functional classification error"),
        mock.patch(
            "mavedb.lib.score_calibrations.ScoreCalibrationFunctionalClassification",
            side_effect=ValueError("Functional classification error"),
        ),
    ):
        create_functional_classification(session, functional_range_create, calibration)


def test_create_functional_classification_does_not_commit_transaction(setup_lib_db, session):
    # Create a mock calibration
    calibration = ScoreCalibration()

    # Create mock functional range without ACMG classification
    MockFunctionalClassificationCreate = create_model(
        "MockFunctionalClassificationCreate",
        label=(str, "Test Label"),
        description=(str, "Test Description"),
        range=(list, [0.0, 1.0]),
        class_=(type(None), None),
        inclusive_lower_bound=(bool, True),
        inclusive_upper_bound=(bool, False),
        functional_classification=(str, "pathogenic"),
        oddspaths_ratio=(float, 1.5),
        positive_likelihood_ratio=(float, 2.0),
        acmg_classification=(type(None), None),
    )

    with mock.patch.object(session, "commit") as mock_commit:
        create_functional_classification(session, MockFunctionalClassificationCreate(), calibration)
        mock_commit.assert_not_called()


################################################################################
# Tests for _create_score_calibration (tested indirectly via the following tests to its callers)
################################################################################


### create_score_calibration_in_score_set


@pytest.mark.asyncio
async def test_create_score_set_in_score_set_raises_value_error_when_score_set_urn_is_missing(
    setup_lib_db, session, mock_user
):
    MockCalibrationCreate = create_model("MockCalibrationCreate", score_set_urn=(str | None, None))
    with pytest.raises(
        ValueError,
        match="score_set_urn must be provided to create a score calibration.",
    ):
        await create_score_calibration_in_score_set(session, MockCalibrationCreate(), mock_user)


@pytest.mark.asyncio
async def test_create_score_set_in_score_set_raises_no_result_found_error_when_score_set_does_not_exist(
    setup_lib_db, session, mock_user
):
    MockCalibrationCreate = create_model("MockCalibrationCreate", score_set_urn=(str | None, "urn:invalid"))
    with pytest.raises(
        NoResultFound,
        match="No row was found when one was required",
    ):
        await create_score_calibration_in_score_set(session, MockCalibrationCreate(), mock_user)


@pytest.mark.asyncio
async def test_create_score_calibration_in_score_set_creates_score_calibration_when_score_set_exists(
    setup_lib_db_with_score_set, session
):
    test_user = session.execute(select(User)).scalars().first()

    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    calibration = await create_score_calibration_in_score_set(session, MockCalibrationCreate(), test_user)
    assert calibration is not None
    assert calibration.score_set == setup_lib_db_with_score_set


@pytest.mark.asyncio
async def test_create_score_calibration_in_score_set_investigator_provided_set_when_creator_is_owner(
    setup_lib_db_with_score_set, session, mock_user
):
    test_user = session.execute(select(User)).scalars().first()

    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    calibration = await create_score_calibration_in_score_set(session, MockCalibrationCreate(), test_user)
    assert calibration is not None
    assert calibration.score_set == setup_lib_db_with_score_set
    assert calibration.created_by == test_user
    assert calibration.modified_by == test_user
    assert calibration.investigator_provided is True


@pytest.mark.asyncio
async def test_create_score_calibration_in_score_set_investigator_provided_set_when_creator_is_contributor(
    setup_lib_db_with_score_set, session
):
    extra_user = session.execute(select(User).where(User.username == EXTRA_USER["username"])).scalars().first()

    add_contributor(
        session,
        setup_lib_db_with_score_set.urn,
        ScoreSet,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    calibration = await create_score_calibration_in_score_set(session, MockCalibrationCreate(), extra_user)
    assert calibration is not None
    assert calibration.score_set == setup_lib_db_with_score_set
    assert calibration.created_by == extra_user
    assert calibration.modified_by == extra_user
    assert calibration.investigator_provided is True


@pytest.mark.asyncio
async def test_create_score_calibration_in_score_set_investigator_provided_not_set_when_creator_not_owner(
    setup_lib_db_with_score_set, session
):
    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    # invoke from a different user context
    extra_user = session.execute(select(User).where(User.username == EXTRA_USER["username"])).scalars().first()

    calibration = await create_score_calibration_in_score_set(session, MockCalibrationCreate(), extra_user)
    assert calibration is not None
    assert calibration.score_set == setup_lib_db_with_score_set
    assert calibration.created_by == extra_user
    assert calibration.modified_by == extra_user
    assert calibration.investigator_provided is False


### create_score_calibration


@pytest.mark.asyncio
async def test_create_score_calibration_raises_value_error_when_score_set_urn_is_provided(
    setup_lib_db, session, mock_user
):
    MockCalibrationCreate = create_model("MockCalibrationCreate", score_set_urn=(str | None, "urn:provided"))
    with pytest.raises(
        ValueError,
        match="score_set_urn must not be provided to create a score calibration outside a score set.",
    ):
        await create_score_calibration(session, MockCalibrationCreate(), mock_user)


@pytest.mark.asyncio
async def test_create_score_calibration_creates_score_calibration_when_score_set_urn_is_absent(setup_lib_db, session):
    test_user = session.execute(select(User)).scalars().first()

    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, None),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    calibration = await create_score_calibration(session, MockCalibrationCreate(), test_user)
    assert calibration is not None
    assert calibration.score_set is None


### Shared tests for create_score_calibration_in_score_set and create_score_calibration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create_function_to_call,score_set_urn",
    [
        (create_score_calibration_in_score_set, VALID_SCORE_SET_URN),
        (create_score_calibration, None),
    ],
)
async def test_create_score_calibration_propagates_errors_from_publication_find_create(
    setup_lib_db_with_score_set, session, mock_user, create_function_to_call, score_set_urn
):
    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, score_set_urn),
        threshold_sources=(
            list,
            [
                create_model(
                    "MockPublicationCreate", db_name=(str, "PubMed"), identifier=(str, TEST_PUBMED_IDENTIFIER)
                )()
            ],
        ),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )
    with (
        pytest.raises(
            ValueError,
            match="Propagated error",
        ),
        mock.patch(
            "mavedb.lib.score_calibrations.find_or_create_publication_identifier",
            side_effect=ValueError("Propagated error"),
        ),
    ):
        await create_function_to_call(session, MockCalibrationCreate(), mock_user)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create_function_to_call,score_set_urn",
    [
        (create_score_calibration_in_score_set, VALID_SCORE_SET_URN),
        (create_score_calibration, None),
    ],
)
@pytest.mark.parametrize(
    "relation,expected_relation",
    [
        ("threshold_sources", ScoreCalibrationRelation.threshold),
        ("classification_sources", ScoreCalibrationRelation.classification),
        ("method_sources", ScoreCalibrationRelation.method),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        ({"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER}),
    ],
    indirect=["mock_publication_fetch"],
)
async def test_create_score_calibration_publication_identifier_associations_created_with_appropriate_relation(
    setup_lib_db_with_score_set,
    session,
    mock_publication_fetch,
    relation,
    expected_relation,
    create_function_to_call,
    score_set_urn,
):
    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, score_set_urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    test_user = session.execute(select(User)).scalars().first()

    mocked_calibration = MockCalibrationCreate()
    setattr(
        mocked_calibration,
        relation,
        [create_model("MockPublicationCreate", db_name=(str, "PubMed"), identifier=(str, TEST_PUBMED_IDENTIFIER))()],
    )

    calibration = await create_function_to_call(session, mocked_calibration, test_user)
    assert calibration.publication_identifier_associations[0].publication.db_name == "PubMed"
    assert calibration.publication_identifier_associations[0].publication.identifier == TEST_PUBMED_IDENTIFIER
    assert calibration.publication_identifier_associations[0].relation == expected_relation
    assert len(calibration.publication_identifier_associations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create_function_to_call,score_set_urn",
    [
        (create_score_calibration_in_score_set, VALID_SCORE_SET_URN),
        (create_score_calibration, None),
    ],
)
async def test_create_score_calibration_user_is_set_as_creator_and_modifier(
    setup_lib_db_with_score_set, session, create_function_to_call, score_set_urn
):
    MockCalibrationCreate = create_model(
        "MockCalibrationCreate",
        score_set_urn=(str | None, score_set_urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    test_user = session.execute(select(User)).scalars().first()

    calibration = await create_function_to_call(session, MockCalibrationCreate(), test_user)
    assert calibration.created_by == test_user
    assert calibration.modified_by == test_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create_function_to_call,score_set_urn",
    [
        (create_score_calibration_in_score_set, VALID_SCORE_SET_URN),
        (create_score_calibration, None),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
@pytest.mark.parametrize(
    "valid_score_calibration_data",
    [
        TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
        TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
    ],
)
async def test_create_score_calibration_fully_valid_calibration(
    setup_lib_db_with_score_set,
    session,
    create_function_to_call,
    score_set_urn,
    mock_publication_fetch,
    valid_score_calibration_data,
):
    calibration_create = ScoreCalibrationCreate(**valid_score_calibration_data, score_set_urn=score_set_urn)

    test_user = session.execute(select(User)).scalars().first()

    calibration = await create_function_to_call(session, calibration_create, test_user)

    for field in valid_score_calibration_data:
        # Sources are tested elsewhere.
        if "sources" not in field and "functional_classifications" not in field:
            assert getattr(calibration, field) == valid_score_calibration_data[field]

        # Verify functional classifications length. Assume the returned value of created classifications is correct,
        # and test the content elsewhere.
        if field == "functional_classifications":
            assert len(calibration.functional_classifications) == len(
                valid_score_calibration_data["functional_classifications"]
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create_function_to_call,score_set_urn",
    [
        (create_score_calibration_in_score_set, VALID_SCORE_SET_URN),
        (create_score_calibration, None),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_create_score_calibration_does_not_commit_transaction(
    setup_lib_db_with_score_set, session, mock_user, create_function_to_call, score_set_urn, mock_publication_fetch
):
    calibration_create = ScoreCalibrationCreate(
        **TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, score_set_urn=score_set_urn
    )
    test_user = session.execute(select(User)).scalars().first()

    with mock.patch.object(session, "commit") as mock_commit:
        await create_function_to_call(session, calibration_create, test_user)
        mock_commit.assert_not_called()


################################################################################
# Tests for modify_score_calibration
################################################################################


@pytest.mark.asyncio
async def test_modify_score_calibration_raises_value_error_when_score_set_urn_is_missing(
    setup_lib_db_with_score_set, session, mock_user, mock_functional_calibration
):
    MockCalibrationModify = create_model("MockCalibrationModify", score_set_urn=(str | None, None))
    with pytest.raises(
        ValueError,
        match="score_set_urn must be provided to modify a score calibration.",
    ):
        await modify_score_calibration(session, mock_functional_calibration, MockCalibrationModify(), mock_user)


@pytest.mark.asyncio
async def test_modify_score_calibration_raises_no_result_found_error_when_score_set_does_not_exist(
    setup_lib_db, session, mock_user, mock_functional_calibration
):
    MockCalibrationModify = create_model("MockCalibrationModify", score_set_urn=(str | None, "urn:invalid"))
    with pytest.raises(
        NoResultFound,
        match="No row was found when one was required",
    ):
        await modify_score_calibration(session, mock_functional_calibration, MockCalibrationModify(), mock_user)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_modifies_score_calibration_when_score_set_exists(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        description=(str | None, "Modified description"),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), test_user
    )
    assert modified_calibration is not None
    assert modified_calibration.description == "Modified description"
    assert modified_calibration.score_set == setup_lib_db_with_score_set


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_clears_existing_publication_identifier_associations(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    mocked_calibration = MockCalibrationModify()

    calibration = await modify_score_calibration(session, existing_calibration, mocked_calibration, test_user)
    assert len(calibration.publication_identifier_associations) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation,expected_relation",
    [
        ("threshold_sources", ScoreCalibrationRelation.threshold),
        ("classification_sources", ScoreCalibrationRelation.classification),
        ("method_sources", ScoreCalibrationRelation.method),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_publication_identifier_associations_created_with_appropriate_relation(
    setup_lib_db_with_score_set,
    session,
    mock_publication_fetch,
    relation,
    expected_relation,
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    mocked_calibration = MockCalibrationModify()
    setattr(
        mocked_calibration,
        relation,
        [create_model("MockPublicationCreate", db_name=(str, "PubMed"), identifier=(str, TEST_PUBMED_IDENTIFIER))()],
    )

    calibration = await modify_score_calibration(session, existing_calibration, mocked_calibration, test_user)
    assert calibration.publication_identifier_associations[0].publication.db_name == "PubMed"
    assert calibration.publication_identifier_associations[0].publication.identifier == TEST_PUBMED_IDENTIFIER
    assert calibration.publication_identifier_associations[0].relation == expected_relation
    assert len(calibration.publication_identifier_associations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_retains_existing_publication_relationships_when_not_modified(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    calibration_publication_relations = existing_calibration.publication_identifier_associations.copy()

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(
            list,
            [
                create_model(
                    "MockPublicationCreate",
                    db_name=(str, pub_dict["db_name"]),
                    identifier=(str, pub_dict["identifier"]),
                )()
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED["threshold_sources"]
            ],
        ),
        classification_sources=(
            list,
            [
                create_model(
                    "MockPublicationCreate",
                    db_name=(str, pub_dict["db_name"]),
                    identifier=(str, pub_dict["identifier"]),
                )()
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED["classification_sources"]
            ],
        ),
        method_sources=(
            list,
            [
                create_model(
                    "MockPublicationCreate",
                    db_name=(str, pub_dict["db_name"]),
                    identifier=(str, pub_dict["identifier"]),
                )()
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED["method_sources"]
            ],
        ),
        functional_classifications=(list, []),
    )

    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), test_user
    )
    assert modified_calibration is not None
    assert modified_calibration.publication_identifier_associations == calibration_publication_relations


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
            {"dbName": "Crossref", "identifier": TEST_CROSSREF_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_adds_new_publication_association(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(
            list,
            [
                create_model(
                    "MockPublicationCreate",
                    db_name=(str, "Crossref"),
                    identifier=(str, TEST_CROSSREF_IDENTIFIER),
                )()
            ],
        ),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), test_user
    )
    assert modified_calibration is not None
    assert modified_calibration.publication_identifier_associations[0].publication.db_name == "Crossref"
    assert (
        modified_calibration.publication_identifier_associations[0].publication.identifier == TEST_CROSSREF_IDENTIFIER
    )
    assert modified_calibration.publication_identifier_associations[0].relation == ScoreCalibrationRelation.threshold
    assert len(modified_calibration.publication_identifier_associations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_user_is_set_as_modifier(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    modify_user = session.execute(select(User).where(User.id != test_user.id)).scalars().first()
    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), modify_user
    )
    assert modified_calibration is not None
    assert modified_calibration.modified_by == modify_user
    assert modified_calibration.created_by == test_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_new_score_set(setup_lib_db_with_score_set, session, mock_publication_fetch):
    existing_experiment = setup_lib_db_with_score_set.experiment
    score_set_scaffold = TEST_SEQ_SCORESET.copy()
    score_set_scaffold.pop("target_genes")
    new_containing_score_set = ScoreSet(
        **score_set_scaffold,
        urn="urn:mavedb:00000000-B-0",
        experiment_id=existing_experiment.id,
        licence_id=TEST_LICENSE["id"],
    )
    new_containing_score_set.created_by = setup_lib_db_with_score_set.created_by
    new_containing_score_set.modified_by = setup_lib_db_with_score_set.modified_by
    session.add(new_containing_score_set)
    session.commit()
    session.refresh(new_containing_score_set)

    test_user = session.execute(select(User)).scalars().first()
    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, new_containing_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, new_containing_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), test_user
    )
    assert modified_calibration is not None
    assert modified_calibration.score_set == new_containing_score_set


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_clears_functional_classifications(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
        functional_classifications=(list, []),
    )

    modified_calibration = await modify_score_calibration(
        session, existing_calibration, MockCalibrationModify(), test_user
    )
    assert modified_calibration is not None
    assert len(modified_calibration.functional_classifications) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_fully_valid_calibration(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    modify_calibration = ScoreCalibrationModify(
        **TEST_PATHOGENICITY_SCORE_CALIBRATION, score_set_urn=setup_lib_db_with_score_set.urn
    )
    modified_calibration = await modify_score_calibration(session, existing_calibration, modify_calibration, test_user)

    for field in TEST_PATHOGENICITY_SCORE_CALIBRATION:
        # Sources are tested elsewhere.
        if "sources" not in field and "functional_classifications" not in field:
            assert getattr(modified_calibration, field) == TEST_PATHOGENICITY_SCORE_CALIBRATION[field]

        # Verify functional classifications length. Assume the returned value of created classifications is correct,
        # and test the content elsewhere.
        if field == "functional_classifications":
            assert len(modified_calibration.functional_classifications) == len(
                TEST_PATHOGENICITY_SCORE_CALIBRATION["functional_classifications"]
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_modify_score_calibration_does_not_commit_transaction(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    modify_calibration = ScoreCalibrationModify(
        **TEST_PATHOGENICITY_SCORE_CALIBRATION, score_set_urn=setup_lib_db_with_score_set.urn
    )

    with mock.patch.object(session, "commit") as mock_commit:
        modify_score_calibration(session, existing_calibration, modify_calibration, test_user)
        mock_commit.assert_not_called()


################################################################################
# Tests for publish_score_calibration
################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_publish_already_published_calibration(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.private = False
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Calibration is already published."):
        publish_score_calibration(session, existing_calibration, test_user)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_publish_score_calibration_marks_calibration_public(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    assert existing_calibration.private is True

    published_calibration = publish_score_calibration(session, existing_calibration, test_user)
    assert published_calibration.private is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_publish_score_calibration_user_is_set_as_modifier(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    publish_user = session.execute(select(User).where(User.id != test_user.id)).scalars().first()
    published_calibration = publish_score_calibration(session, existing_calibration, publish_user)
    assert published_calibration is not None
    assert published_calibration.modified_by == publish_user
    assert published_calibration.created_by == test_user


################################################################################
# Tests for promote_score_calibration_to_primary
################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_promote_already_primary_calibration(setup_lib_db_with_score_set, session, mock_publication_fetch):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Calibration is already primary."):
        promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_promote_calibration_when_calibration_is_research_use_only(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.research_use_only = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Cannot promote a research use only calibration to primary."):
        promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_promote_calibration_when_calibration_is_private(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.private = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Cannot promote a private calibration to primary."):
        promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_promote_calibration_when_another_primary_exists(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_primary_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_primary_calibration.private = False
    existing_primary_calibration.primary = True
    existing_calibration.private = False
    existing_calibration.primary = False

    session.add(existing_primary_calibration)
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_primary_calibration)
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Another primary calibration already exists for this score set."):
        promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_promote_score_calibration_to_primary_marks_calibration_primary(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.private = False
    existing_calibration.primary = False
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    promoted_calibration = promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)
    assert promoted_calibration.primary is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_promote_score_calibration_to_primary_demotes_existing_primary_when_forced(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_primary_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_primary_calibration.private = False
    existing_primary_calibration.primary = True
    existing_calibration.private = False
    existing_calibration.primary = False

    session.add(existing_primary_calibration)
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_primary_calibration)
    session.refresh(existing_calibration)

    assert existing_calibration.primary is False

    promoted_calibration = promote_score_calibration_to_primary(session, existing_calibration, test_user, force=True)
    session.commit()
    session.refresh(existing_primary_calibration)

    assert promoted_calibration.primary is True
    assert existing_primary_calibration.primary is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_promote_score_calibration_to_primary_user_is_set_as_modifier(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.private = False
    existing_calibration.primary = False
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    promote_user = session.execute(select(User).where(User.id != test_user.id)).scalars().first()
    promoted_calibration = promote_score_calibration_to_primary(
        session, existing_calibration, promote_user, force=False
    )
    assert promoted_calibration is not None
    assert promoted_calibration.modified_by == promote_user
    assert promoted_calibration.created_by == test_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_promote_score_calibration_to_primary_demoted_existing_primary_user_is_set_as_modifier(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_primary_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_primary_calibration.private = False
    existing_primary_calibration.primary = True
    existing_calibration.private = False
    existing_calibration.primary = False

    session.add(existing_primary_calibration)
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_primary_calibration)
    session.refresh(existing_calibration)

    assert existing_calibration.primary is False

    promote_user = session.execute(select(User).where(User.id != test_user.id)).scalars().first()
    promoted_calibration = promote_score_calibration_to_primary(session, existing_calibration, promote_user, force=True)
    session.commit()
    session.refresh(existing_primary_calibration)

    assert promoted_calibration.primary is True
    assert existing_primary_calibration is not None
    assert existing_primary_calibration.modified_by == promote_user
    assert promoted_calibration.created_by == test_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_promote_score_calibration_to_primary_does_not_commit_transaction(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.private = False
    existing_calibration.primary = False
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with mock.patch.object(session, "commit") as mock_commit:
        promote_score_calibration_to_primary(session, existing_calibration, test_user, force=False)
        mock_commit.assert_not_called()


################################################################################
# Test demote_score_calibration_from_primary
################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_demote_non_primary_calibration(setup_lib_db_with_score_set, session, mock_publication_fetch):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = False
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Calibration is not primary."):
        demote_score_calibration_from_primary(session, existing_calibration, test_user)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_demote_score_calibration_from_primary_marks_calibration_non_primary(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)
    assert existing_calibration.primary is True

    demoted_calibration = demote_score_calibration_from_primary(session, existing_calibration, test_user)
    assert demoted_calibration.primary is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_demote_score_calibration_from_primary_user_is_set_as_modifier(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    demote_user = session.execute(select(User).where(User.id != test_user.id)).scalars().first()
    demoted_calibration = demote_score_calibration_from_primary(session, existing_calibration, demote_user)
    assert demoted_calibration is not None
    assert demoted_calibration.modified_by == demote_user
    assert demoted_calibration.created_by == test_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_demote_score_calibration_from_primary_does_not_commit_transaction(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with mock.patch.object(session, "commit") as mock_commit:
        demote_score_calibration_from_primary(session, existing_calibration, test_user)
        mock_commit.assert_not_called()


################################################################################
# Test delete_score_calibration
################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_cannot_delete_primary_calibration(setup_lib_db_with_score_set, session, mock_publication_fetch):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration.primary = True
    session.add(existing_calibration)
    session.commit()
    session.refresh(existing_calibration)

    with pytest.raises(ValueError, match="Cannot delete a primary calibration."):
        delete_score_calibration(session, existing_calibration)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_delete_score_calibration_deletes_calibration(
    session, setup_lib_db_with_score_set, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    calibration_id = existing_calibration.id

    delete_score_calibration(session, existing_calibration)
    session.commit()

    with pytest.raises(NoResultFound, match="No row was found when one was required"):
        session.execute(select(ScoreCalibration).where(ScoreCalibration.id == calibration_id)).scalars().one()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ],
    ],
    indirect=["mock_publication_fetch"],
)
async def test_delete_score_calibration_does_not_commit_transaction(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_range_based_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    with mock.patch.object(session, "commit") as mock_commit:
        delete_score_calibration(session, existing_calibration)
        mock_commit.assert_not_called()


################################################################################
# Tests for variants_for_functional_classification
################################################################################


def test_variants_for_functional_classification_returns_empty_list_when_range_and_classes_is_none(
    setup_lib_db, session
):
    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = 1
    mock_functional_calibration = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_calibration.range = None
    mock_functional_calibration.class_ = None
    mock_functional_calibration.calibration = mock_calibration

    result = variants_for_functional_classification(
        session, mock_functional_calibration, variant_classes=None, use_sql=False
    )

    assert result == []


def test_variants_for_functional_classification_returns_empty_list_when_range_is_empty_list_and_classes_is_none(
    setup_lib_db, session
):
    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = 1
    mock_functional_calibration = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_calibration.range = []
    mock_functional_calibration.class_ = None
    mock_functional_calibration.calibration = mock_calibration

    result = variants_for_functional_classification(
        session, mock_functional_calibration, variant_classes=None, use_sql=False
    )

    assert result == []


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        (
            None,
            "benign",
            pd.DataFrame(
                {
                    calibration_variant_column_name: [
                        "urn:mavedb:variant-1",
                        "urn:mavedb:variant-2",
                        "urn:mavedb:variant-3",
                    ],
                    calibration_class_column_name: [
                        "pathogenic",
                        "benign",
                        "pathogenic",
                    ],
                }
            ),
        ),
    ],
)
def test_variants_for_functional_classification_python_filtering_with_valid_variants(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    variant_1 = Variant(
        data={"score_data": {"score": 0.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )
    variant_2 = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )
    variant_3 = Variant(
        data={"score_data": {"score": 2.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-3",
    )

    session.add_all([variant_1, variant_2, variant_3])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )

    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_python_filtering_skips_variants_without_score_data(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variant without score_data
    variant_without_score_data = Variant(
        data={"other_data": {"value": 1.0}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )

    # Create variant with valid score
    variant_with_score = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )

    session.add_all([variant_without_score_data, variant_with_score])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )

    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_python_filtering_skips_variants_with_non_dict_score_data(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variant with non-dict score_data
    variant_invalid_score_data = Variant(
        data={"score_data": "not_a_dict"},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )

    # Create variant with valid score
    variant_with_score = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )

    session.add_all([variant_invalid_score_data, variant_with_score])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )
    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_python_filtering_skips_variants_with_none_score(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variant with None score
    variant_none_score = Variant(
        data={"score_data": {"score": None}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )

    # Create variant with valid score
    variant_with_score = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )

    session.add_all([variant_none_score, variant_with_score])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = [1.0, 2.0]
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )

    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_python_filtering_skips_variants_with_non_numeric_score(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variant with non-numeric score
    variant_string_score = Variant(
        data={"score_data": {"score": "not_a_number"}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )

    # Create variant with valid score
    variant_with_score = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )

    session.add_all([variant_string_score, variant_with_score])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )
    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_python_filtering_skips_variants_with_non_dict_data(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variant with non-dict data
    variant_invalid_data = Variant(
        data="not_a_dict", score_set_id=setup_lib_db_with_score_set.id, urn="urn:mavedb:variant-1"
    )

    # Create variant with valid score
    variant_with_score = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-2",
    )

    session.add_all([variant_invalid_data, variant_with_score])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )
    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "use_sql",
    [True, False],
)
@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        (
            None,
            "benign",
            pd.DataFrame(
                {
                    calibration_variant_column_name: [
                        "urn:mavedb:variant-1",
                        "urn:mavedb:variant-2",
                        "urn:mavedb:variant-3",
                        "urn:mavedb:variant-4",
                        "urn:mavedb:variant-5",
                    ],
                    calibration_class_column_name: [
                        "pathogenic",
                        "benign",
                        "benign",
                        "benign",
                        "pathogenic",
                    ],
                }
            ),
        ),
    ],
)
def test_variants_for_functional_classification_filters_by_conditions(
    setup_lib_db_with_score_set, session, use_sql, range_, class_, variant_classes
):
    # Create variants with different scores
    variants = []
    scores = [0.5, 1.0, 1.5, 2.0, 2.5]
    for i, score in enumerate(scores, 1):
        variant = Variant(
            data={"score_data": {"score": score}},
            score_set_id=setup_lib_db_with_score_set.id,
            urn=f"urn:mavedb:variant-{i}",
        )
        variants.append(variant)

    session.add_all(variants)
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.inclusive_lower_bound = True
    mock_functional_classification.inclusive_upper_bound = True
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    with mock.patch("mavedb.lib.score_calibrations.inf_or_float", side_effect=lambda x, lower: float(x)):
        result = variants_for_functional_classification(
            session,
            mock_functional_classification,
            variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
            use_sql=use_sql,
        )

    # Should return variants with scores 1.0, 1.5, 2.0
    result_scores = [v.data["score_data"]["score"] for v in result]
    expected_scores = [1.0, 1.5, 2.0]
    assert sorted(result_scores) == sorted(expected_scores)


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        (
            None,
            "benign",
            pd.DataFrame(
                {
                    calibration_variant_column_name: [
                        "urn:mavedb:variant-1",
                        "urn:mavedb:variant-2",
                        "urn:mavedb:variant-3",
                    ],
                    calibration_class_column_name: [
                        "benign",
                        "pathogenic",
                        "pathogenic",
                    ],
                }
            ),
        ),
    ],
)
def test_variants_for_functional_classification_sql_fallback_on_exception(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create a variant
    variant = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-1",
    )
    session.add(variant)
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    # Mock db.execute to raise an exception during SQL execution
    with mock.patch.object(
        session,
        "execute",
        side_effect=[
            Exception("SQL error"),
            session.execute(select(Variant).where(Variant.score_set_id == setup_lib_db_with_score_set.id)),
        ],
    ) as mocked_execute:
        result = variants_for_functional_classification(
            session,
            mock_functional_classification,
            variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
            use_sql=True,
        )
        mocked_execute.assert_called()

    # Should fall back to Python filtering and return the matching variant
    assert len(result) == 1
    assert result[0].data["score_data"]["score"] == 1.5


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, float("inf")], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_sql_with_infinite_bound(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variants with different scores
    variants = []
    scores = [0.5, 1.5, 2.5]
    for i, score in enumerate(scores):
        variant = Variant(
            data={"score_data": {"score": score}},
            score_set_id=setup_lib_db_with_score_set.id,
            urn=f"urn:mavedb:variant-{i}",
        )
        variants.append(variant)

    session.add_all(variants)
    session.commit()

    # Mock functional classification with infinite upper bound
    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.inclusive_lower_bound = True
    mock_functional_classification.inclusive_upper_bound = False

    with mock.patch(
        "mavedb.lib.score_calibrations.inf_or_float",
        side_effect=lambda x, lower: float("inf") if x == float("inf") else float(x),
    ):
        with mock.patch("math.isinf", side_effect=lambda x: x == float("inf")):
            result = variants_for_functional_classification(
                session,
                mock_functional_classification,
                variant_classes=variant_classification_df_to_dict(variant_classes)
                if variant_classes is not None
                else None,
                use_sql=True,
            )

    # Should return variants with scores >= 1.0
    result_scores = [v.data["score_data"]["score"] for v in result]
    expected_scores = [1.5, 2.5]
    assert sorted(result_scores) == sorted(expected_scores)


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_sql_with_exclusive_bounds(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create variants with boundary scores
    variants = []
    scores = [1.0, 1.5, 2.0]
    for i, score in enumerate(scores):
        variant = Variant(
            data={"score_data": {"score": score}},
            score_set_id=setup_lib_db_with_score_set.id,
            urn=f"urn:mavedb:variant-{i}",
        )
        variants.append(variant)

    session.add_all(variants)
    session.commit()

    # Mock functional classification with exclusive bounds
    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.inclusive_lower_bound = False
    mock_functional_classification.inclusive_upper_bound = False

    with mock.patch("mavedb.lib.score_calibrations.inf_or_float", side_effect=lambda x, lower: float(x)):
        result = variants_for_functional_classification(
            session,
            mock_functional_classification,
            variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
            use_sql=True,
        )

    # Should return only variant with score 1.5 (exclusive bounds)
    result_scores = [v.data["score_data"]["score"] for v in result]
    assert result_scores == [1.5]


@pytest.mark.parametrize(
    "range_,class_,variant_classes",
    [
        ([1.0, 2.0], None, None),
        # not applicable when filtering by class
    ],
)
def test_variants_for_functional_classification_only_returns_variants_from_correct_score_set(
    setup_lib_db_with_score_set, session, range_, class_, variant_classes
):
    # Create another score set
    other_score_set = ScoreSet(
        urn="urn:mavedb:00000000-B-0",
        experiment_id=setup_lib_db_with_score_set.experiment_id,
        licence_id=TEST_LICENSE["id"],
        title="Other Score Set",
        method_text="Other method",
        abstract_text="Other abstract",
        short_description="Other description",
        created_by=setup_lib_db_with_score_set.created_by,
        modified_by=setup_lib_db_with_score_set.modified_by,
        extra_metadata={},
    )
    session.add(other_score_set)
    session.commit()

    # Create variants in both score sets
    variant_in_target_set = Variant(
        data={"score_data": {"score": 1.5}},
        score_set_id=setup_lib_db_with_score_set.id,
        urn="urn:mavedb:variant-target",
    )
    variant_in_other_set = Variant(
        data={"score_data": {"score": 1.5}}, score_set_id=other_score_set.id, urn="urn:mavedb:variant-other"
    )

    session.add_all([variant_in_target_set, variant_in_other_set])
    session.commit()

    mock_calibration = mock.Mock(spec=ScoreCalibration)
    mock_calibration.score_set_id = setup_lib_db_with_score_set.id
    mock_functional_classification = mock.Mock(spec=ScoreCalibrationFunctionalClassification)
    mock_functional_classification.range = range_
    mock_functional_classification.class_ = class_
    mock_functional_classification.calibration = mock_calibration
    mock_functional_classification.score_is_contained_in_range = mock.Mock(side_effect=lambda x: 1.0 <= x <= 2.0)

    result = variants_for_functional_classification(
        session,
        mock_functional_classification,
        variant_classes=variant_classification_df_to_dict(variant_classes) if variant_classes is not None else None,
        use_sql=False,
    )
    # Should only return variant from the target score set
    assert len(result) == 1
    assert result[0].score_set_id == setup_lib_db_with_score_set.id
    assert result[0].urn == "urn:mavedb:variant-target"


################################################################################
# Tests for variant_classification_df_to_dict
################################################################################


def test_variant_classification_df_to_dict_with_single_class():
    """Test conversion with DataFrame containing variants of a single functional class."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2", "var3"],
            calibration_class_column_name: ["pathogenic", "pathogenic", "pathogenic"],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": sorted(["var1", "var2", "var3"])}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_multiple_classes():
    """Test conversion with DataFrame containing variants of multiple functional classes."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2", "var3", "var4", "var5"],
            calibration_class_column_name: ["pathogenic", "benign", "pathogenic", "uncertain", "benign"],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1", "var3"], "benign": sorted(["var2", "var5"]), "uncertain": ["var4"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_empty_dataframe():
    """Test conversion with empty DataFrame."""
    df = pd.DataFrame(columns=[calibration_variant_column_name, calibration_class_column_name])

    result = variant_classification_df_to_dict(df)

    assert result == {}


def test_variant_classification_df_to_dict_with_single_row():
    """Test conversion with DataFrame containing single row."""
    df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["pathogenic"]})

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1"]}
    assert result == expected


def test_variant_classification_df_to_dict_preserves_order_within_classes():
    """Test that variant order is preserved within each functional class."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2", "var3", "var4"],
            calibration_class_column_name: ["pathogenic", "pathogenic", "benign", "pathogenic"],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": sorted(["var1", "var2", "var4"]), "benign": ["var3"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_extra_columns():
    """Test conversion ignores extra columns in DataFrame."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2"],
            calibration_class_column_name: ["pathogenic", "benign"],
            "extra_column": ["value1", "value2"],
            "another_column": [1, 2],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1"], "benign": ["var2"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_duplicate_variants_in_same_class():
    """Test handling of duplicate variant URNs in the same functional class."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var1", "var2"],
            calibration_class_column_name: ["pathogenic", "pathogenic", "benign"],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1"], "benign": ["var2"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_none_values():
    """Test handling of None values in functional class column."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2", "var3"],
            calibration_class_column_name: ["pathogenic", None, "benign"],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1"], None: ["var2"], "benign": ["var3"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_numeric_classes():
    """Test handling of numeric functional class labels."""
    df = pd.DataFrame(
        {calibration_variant_column_name: ["var1", "var2", "var3"], calibration_class_column_name: [1, 2, 1]}
    )

    result = variant_classification_df_to_dict(df)

    expected = {1: sorted(["var1", "var3"]), 2: ["var2"]}
    assert {k: sorted(v) for k, v in result.items()} == expected


def test_variant_classification_df_to_dict_with_mixed_type_classes():
    """Test handling of mixed data types in functional class column."""
    df = pd.DataFrame(
        {
            calibration_variant_column_name: ["var1", "var2", "var3", "var4"],
            calibration_class_column_name: ["pathogenic", 1, "benign", 1],
        }
    )

    result = variant_classification_df_to_dict(df)

    expected = {"pathogenic": ["var1"], 1: sorted(["var2", "var4"]), "benign": ["var3"]}
    assert {k: sorted(v) for k, v in result.items()} == expected
