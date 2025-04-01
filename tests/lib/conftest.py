from pathlib import Path
from shutil import copytree

from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.role import Role
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from tests.helpers.constants import (
    ADMIN_USER,
    EXTRA_USER,
    TEST_EXPERIMENT,
    TEST_EXPERIMENT_SET,
    TEST_LICENSE,
    TEST_INACTIVE_LICENSE,
    TEST_MAVEDB_ATHENA_ROW,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_MINIMAL_VARIANT,
    TEST_PUBMED_IDENTIFIER,
    TEST_SAVED_TAXONOMY,
    TEST_SEQ_SCORESET,
    TEST_USER,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    VALID_SCORE_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_EXPERIMENT_SET_URN,
    TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT,
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
    db.commit()


@pytest.fixture
def setup_lib_db_with_score_set(session, setup_lib_db):
    """
    Sets up the lib test db with a user, reference, license, and a score set.
    """

    experiment_set = ExperimentSet(**TEST_EXPERIMENT_SET, urn=VALID_EXPERIMENT_SET_URN)
    session.add(experiment_set)
    session.commit()
    session.refresh(experiment_set)

    experiment = Experiment(**TEST_EXPERIMENT, urn=VALID_EXPERIMENT_URN, experiment_set_id=experiment_set.id)
    session.add(experiment)
    session.commit()
    session.refresh(experiment)

    score_set_scaffold = TEST_SEQ_SCORESET.copy()
    score_set_scaffold.pop("target_genes")
    score_set = ScoreSet(
        **score_set_scaffold, urn=VALID_SCORE_SET_URN, experiment_id=experiment.id, licence_id=TEST_LICENSE["id"]
    )

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
    mv = mock.Mock(spec=User)
    mv.username = TEST_USER["username"]
    return mv


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
    experiment.urn = VALID_EXPERIMENT_URN
    experiment.creation_date = datetime(2023, 1, 1)
    experiment.modification_date = datetime(2023, 1, 2)
    return experiment


@pytest.fixture
def mock_score_set(mock_user, mock_experiment, mock_publication_associations):
    score_set = mock.Mock(spec=ScoreSet)
    score_set.urn = VALID_SCORE_SET_URN
    score_set.score_ranges = TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT
    score_set.license.short_name = "MIT"
    score_set.created_by = mock_user
    score_set.modified_by = mock_user
    score_set.published_date = datetime(2023, 1, 1)
    score_set.title = "Mock score set"
    score_set.creation_date = datetime(2023, 1, 2)
    score_set.modification_date = datetime(2023, 1, 3)
    score_set.experiment = mock_experiment
    score_set.publication_identifier_associations = mock_publication_associations
    return score_set


@pytest.fixture
def mock_variant(mock_score_set):
    variant = mock.Mock(spec=Variant)
    variant.urn = f"{VALID_SCORE_SET_URN}#1"
    variant.score_set = mock_score_set
    variant.data = {"score_data": {"score": 1.0}}
    variant.creation_date = datetime(2023, 1, 2)
    variant.modification_date = datetime(2023, 1, 3)
    return variant


@pytest.fixture
def mock_mapped_variant(mock_variant):
    mv = mock.Mock(spec=MappedVariant)
    mv.mapping_api_version = "pytest.mapping.1.0"
    mv.mapped_date = datetime(2023, 1, 1)
    mv.variant = mock_variant
    mv.pre_mapped = TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X
    mv.post_mapped = TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
    mv.mapped_date = datetime(2023, 1, 2)
    mv.modification_date = datetime(2023, 1, 3)
    return mv


@pytest.fixture
def mocked_gnomad_variant_row():
    gnomad_variant = mock.Mock()

    for key, value in TEST_MAVEDB_ATHENA_ROW.items():
        setattr(gnomad_variant, key, value)

    return gnomad_variant


def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"
