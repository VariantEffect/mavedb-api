from datetime import date, datetime
from pathlib import Path
from shutil import copytree
from unittest import mock

import pytest

from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.role import Role
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    ADMIN_USER,
    EXTRA_USER,
    TEST_ACMG_BS3_STRONG_CLASSIFICATION,
    TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS,
    TEST_ACMG_PS3_STRONG_CLASSIFICATION,
    TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS,
    TEST_EXPERIMENT,
    TEST_EXPERIMENT_SET,
    TEST_INACTIVE_LICENSE,
    TEST_LICENSE,
    TEST_MAVEDB_ATHENA_ROW,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_MINIMAL_VARIANT,
    TEST_PUBMED_IDENTIFIER,
    TEST_SAVED_TAXONOMY,
    TEST_SEQ_SCORESET,
    TEST_USER,
    VALID_EXPERIMENT_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_SCORE_SET_URN,
)
from tests.helpers.mocks.factories import (
    create_mock_license,
    create_mock_mapped_variant,
    create_mock_score_calibration,
    create_mock_score_set,
    create_mock_user,
)


@pytest.fixture
def setup_lib_db(session):
    """
    Sets up the lib test db with a user, reference, and license. Its more straightforward to use
    the well tested client methods to insert experiments and score sets to the db for testing.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(User(**ADMIN_USER, role_objs=[Role(name=UserRole.admin)]))
    db.add(Taxonomy(**TEST_SAVED_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.add(License(**TEST_INACTIVE_LICENSE))
    db.add(ACMGClassification(**TEST_ACMG_PS3_STRONG_CLASSIFICATION))
    db.add(ACMGClassification(**TEST_ACMG_BS3_STRONG_CLASSIFICATION))
    db.add(ACMGClassification(**TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS))
    db.add(ACMGClassification(**TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS))
    db.commit()


@pytest.fixture
def setup_lib_db_with_score_set(session, setup_lib_db):
    """
    Sets up the lib test db with a user, reference, license, and a score set.
    """
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()

    experiment_set = ExperimentSet(**TEST_EXPERIMENT_SET, urn=VALID_EXPERIMENT_SET_URN)
    experiment_set.created_by = user
    experiment_set.modified_by = user
    session.add(experiment_set)
    session.commit()
    session.refresh(experiment_set)

    experiment = Experiment(**TEST_EXPERIMENT, urn=VALID_EXPERIMENT_URN, experiment_set_id=experiment_set.id)
    experiment.created_by = user
    experiment.modified_by = user
    session.add(experiment)
    session.commit()
    session.refresh(experiment)

    score_set_scaffold = TEST_SEQ_SCORESET.copy()
    score_set_scaffold.pop("target_genes")
    score_set = ScoreSet(
        **score_set_scaffold, urn=VALID_SCORE_SET_URN, experiment_id=experiment.id, licence_id=TEST_LICENSE["id"]
    )
    score_set.created_by = user
    score_set.modified_by = user
    session.add(score_set)
    session.commit()
    session.refresh(score_set)

    return score_set


@pytest.fixture
def setup_lib_db_with_variant(session, setup_lib_db_with_score_set):
    """
    Sets up the lib test db with a user, reference, license, and a score set.
    """

    variant = Variant(
        **TEST_MINIMAL_VARIANT, urn=f"{setup_lib_db_with_score_set.urn}#1", score_set_id=setup_lib_db_with_score_set.id
    )

    session.add(variant)
    session.commit()
    session.refresh(variant)

    return variant


@pytest.fixture
def setup_lib_db_with_mapped_variant(session, setup_lib_db_with_variant):
    """
    Sets up the lib test db with a user, reference, license, and a score set.
    """

    mapped_variant = MappedVariant(**TEST_MINIMAL_MAPPED_VARIANT, variant_id=setup_lib_db_with_variant.id)

    session.add(mapped_variant)
    session.commit()
    session.refresh(mapped_variant)

    return mapped_variant


@pytest.fixture
def mock_user():
    # Use the properly configured mock user creation function
    return create_mock_user(
        username=TEST_USER["username"], first_name=TEST_USER["first_name"], last_name=TEST_USER["last_name"]
    )


@pytest.fixture
def mock_publication():
    mv = mock.Mock(spec=PublicationIdentifier)
    mv.identifier = TEST_PUBMED_IDENTIFIER
    mv.url = f"http://www.ncbi.nlm.nih.gov/pubmed/{TEST_PUBMED_IDENTIFIER}"
    return mv


@pytest.fixture
def mock_publication_associations(mock_publication):
    mv = mock.Mock(spec=ScoreSetPublicationIdentifierAssociation)
    mv.publication = mock_publication
    mv.primary = True
    return [mv]


@pytest.fixture
def mock_experiment_set():
    resource = mock.Mock(spec=ExperimentSet)
    resource.urn = VALID_EXPERIMENT_SET_URN
    resource.creation_date = datetime(2023, 1, 1)
    resource.modification_date = datetime(2023, 1, 2)
    return resource


@pytest.fixture
def mock_experiment():
    experiment = mock.Mock(spec=Experiment)
    experiment.title = "Test Experiment"
    experiment.short_description = "Short description"
    experiment.abstract_text = "Abstract"
    experiment.method_text = "Method"
    experiment.urn = VALID_EXPERIMENT_URN
    experiment.creation_date = datetime(2023, 1, 1)
    experiment.modification_date = datetime(2023, 1, 2)
    return experiment


@pytest.fixture
def mock_functional_calibration(mock_user):
    # Create properly configured mock score calibration with functional classifications
    calibration = create_mock_score_calibration(
        urn="test:calibration:functional",
        title="Test Functional Calibration",
        created_by=mock_user,
        functional_classifications=[
            {"classification": "normal", "lowerBound": -1.0, "upperBound": 0.0, "acmgClassification": None},
            {"classification": "abnormal", "lowerBound": 0.0, "upperBound": 1.0, "acmgClassification": None},
        ],
        primary=True,
        research_use_only=False,
        calibrationMetadata={"method": "test"},
        baselineScoreDescription="Test baseline score description",
    )

    return calibration


@pytest.fixture
def mock_pathogenicity_calibration(mock_user):
    # Create properly configured mock score calibration with pathogenicity classifications
    calibration = create_mock_score_calibration(
        urn="test:calibration:pathogenicity",
        title="Test Pathogenicity Calibration",
        created_by=mock_user,
        functional_classifications=[
            {"classification": "likely_pathogenic", "lowerBound": 0.5, "upperBound": 1.0, "acmgClassification": "PS3"},
            {"classification": "likely_benign", "lowerBound": -1.0, "upperBound": -0.5, "acmgClassification": "BS3"},
        ],
        primary=True,
        research_use_only=False,
        calibrationMetadata={"method": "test"},
        baselineScoreDescription="Test baseline score description",
    )

    return calibration


@pytest.fixture
def mock_score_set(mock_user, mock_experiment, mock_publication_associations):
    # Create a properly configured mock score set
    score_set = create_mock_score_set(
        urn=VALID_SCORE_SET_URN,
        title="Mock score set",
        short_description="Short description",
        published_date=date(2023, 1, 1),
        license=create_mock_license("MIT", "MIT License", "1.0"),
    )

    # Override some attributes with the provided fixtures
    score_set.created_by = mock_user
    score_set.modified_by = mock_user
    score_set.experiment = mock_experiment
    score_set.publication_identifier_associations = mock_publication_associations
    score_set.abstract_text = "Abstract"
    score_set.method_text = "Method"
    score_set.creation_date = datetime(2023, 1, 2)
    score_set.modification_date = datetime(2023, 1, 3)

    return score_set


@pytest.fixture
def mock_score_set_with_functional_calibrations(mock_score_set, mock_functional_calibration):
    mock_score_set.score_calibrations = [mock_functional_calibration]
    return mock_score_set


@pytest.fixture
def mock_score_set_with_pathogenicity_calibrations(mock_score_set, mock_pathogenicity_calibration):
    mock_score_set.score_calibrations = [mock_pathogenicity_calibration]
    return mock_score_set


@pytest.fixture
def mock_variant(mock_score_set):
    # Create a properly configured variant using the mapped variant helper
    # then return just the variant part
    mapped_variant = create_mock_mapped_variant(urn=f"{VALID_SCORE_SET_URN}#1", score=1.0, score_set=mock_score_set)

    # Set proper dates
    mapped_variant.variant.creation_date = date(2023, 1, 2)
    mapped_variant.variant.modification_date = date(2023, 1, 3)

    return mapped_variant.variant


@pytest.fixture
def mock_variant_with_functional_calibration_score_set(mock_variant, mock_score_set_with_functional_calibrations):
    mock_variant.score_set = mock_score_set_with_functional_calibrations
    return mock_variant


@pytest.fixture
def mock_variant_with_pathogenicity_calibration_score_set(mock_variant, mock_score_set_with_pathogenicity_calibrations):
    mock_variant.score_set = mock_score_set_with_pathogenicity_calibrations
    return mock_variant


@pytest.fixture
def mock_mapped_variant(mock_variant):
    # Create a properly configured mapped variant using the helper function
    mv = create_mock_mapped_variant(
        mapping_api_version="pytest.mapping.1.0", mapped_date=datetime(2023, 1, 2), clingen_allele_id="CA123456"
    )

    # Override the variant with the provided mock_variant
    mv.variant = mock_variant
    mv.modification_date = datetime(2023, 1, 3)

    return mv


@pytest.fixture
def mock_mapped_variant_with_functional_calibration_score_set(
    mock_mapped_variant, mock_variant_with_functional_calibration_score_set
):
    mock_mapped_variant.variant = mock_variant_with_functional_calibration_score_set
    return mock_mapped_variant


@pytest.fixture
def mock_mapped_variant_with_pathogenicity_calibration_score_set(
    mock_mapped_variant, mock_variant_with_pathogenicity_calibration_score_set
):
    mock_mapped_variant.variant = mock_variant_with_pathogenicity_calibration_score_set
    return mock_mapped_variant


@pytest.fixture
def mocked_gnomad_variant_row():
    gnomad_variant = mock.Mock()

    for key, value in TEST_MAVEDB_ATHENA_ROW.items():
        setattr(gnomad_variant, key, value)

    return gnomad_variant


def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"
