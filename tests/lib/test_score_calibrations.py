# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from unittest import mock

from pydantic import create_model
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from mavedb.lib.score_calibrations import (
    create_score_calibration,
    create_score_calibration_in_score_set,
    delete_score_calibration,
    demote_score_calibration_from_primary,
    modify_score_calibration,
    promote_score_calibration_to_primary,
    publish_score_calibration,
)
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.view_models.score_calibration import ScoreCalibrationCreate, ScoreCalibrationModify

from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION,
    TEST_CROSSREF_IDENTIFIER,
    TEST_LICENSE,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_PUBMED_IDENTIFIER,
    TEST_SEQ_SCORESET,
    VALID_SCORE_SET_URN,
)
from tests.helpers.util.score_calibration import create_test_score_calibration_in_score_set

################################################################################
# Tests for create_score_calibration
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
    )

    calibration = await create_score_calibration_in_score_set(session, MockCalibrationCreate(), test_user)
    assert calibration is not None
    assert calibration.score_set == setup_lib_db_with_score_set


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
async def test_create_score_calibration_fully_valid_calibration(
    setup_lib_db_with_score_set, session, create_function_to_call, score_set_urn, mock_publication_fetch
):
    calibration_create = ScoreCalibrationCreate(**TEST_BRNICH_SCORE_CALIBRATION, score_set_urn=score_set_urn)

    test_user = session.execute(select(User)).scalars().first()

    calibration = await create_function_to_call(session, calibration_create, test_user)

    for field in TEST_BRNICH_SCORE_CALIBRATION:
        # Sources are tested elsewhere
        # XXX: Ranges are a pain to compare between JSONB and dict input, so are assumed correct
        if "sources" not in field and "functional_ranges" not in field:
            assert getattr(calibration, field) == TEST_BRNICH_SCORE_CALIBRATION[field]


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

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        description=(str | None, "Modified description"),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
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

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
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

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION["threshold_sources"]
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
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION["classification_sources"]
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
                for pub_dict in TEST_BRNICH_SCORE_CALIBRATION["method_sources"]
            ],
        ),
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, setup_lib_db_with_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
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

    session.add(new_containing_score_set)
    session.commit()
    session.refresh(new_containing_score_set)

    test_user = session.execute(select(User)).scalars().first()
    existing_calibration = await create_test_score_calibration_in_score_set(
        session, new_containing_score_set.urn, test_user
    )

    MockCalibrationModify = create_model(
        "MockCalibrationModify",
        score_set_urn=(str | None, new_containing_score_set.urn),
        threshold_sources=(list, []),
        classification_sources=(list, []),
        method_sources=(list, []),
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
async def test_modify_score_calibration_fully_valid_calibration(
    setup_lib_db_with_score_set, session, mock_publication_fetch
):
    test_user = session.execute(select(User)).scalars().first()

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )

    modify_calibration = ScoreCalibrationModify(
        **TEST_PATHOGENICITY_SCORE_CALIBRATION, score_set_urn=setup_lib_db_with_score_set.urn
    )
    modified_calibration = await modify_score_calibration(session, existing_calibration, modify_calibration, test_user)

    for field in TEST_PATHOGENICITY_SCORE_CALIBRATION:
        # Sources are tested elsewhere
        # XXX: Ranges are a pain to compare between JSONB and dict input, so are assumed correct
        if "sources" not in field and "functional_ranges" not in field:
            assert getattr(modified_calibration, field) == TEST_PATHOGENICITY_SCORE_CALIBRATION[field]


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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_primary_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_primary_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_primary_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
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

    existing_calibration = await create_test_score_calibration_in_score_set(
        session, setup_lib_db_with_score_set.urn, test_user
    )
    calibration_id = existing_calibration.id

    delete_score_calibration(session, existing_calibration)
    session.commit()

    with pytest.raises(NoResultFound, match="No row was found when one was required"):
        session.execute(select(ScoreCalibration).where(ScoreCalibration.id == calibration_id)).scalars().one()
