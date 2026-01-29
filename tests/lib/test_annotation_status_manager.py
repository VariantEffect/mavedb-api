import pytest

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.variant import Variant


@pytest.fixture
def annotation_status_manager(session):
    """Fixture to provide an AnnotationStatusManager instance."""
    return AnnotationStatusManager(session)


@pytest.fixture
def existing_annotation_status(session, annotation_status_manager, setup_lib_db_with_variant):
    """Fixture to create an existing annotation status in the database."""

    # Add initial annotation
    annotation = annotation_status_manager.add_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
        version="v1",
        annotation_data={},
        status=AnnotationStatus.SUCCESS,
        current=True,
    )
    session.commit()

    assert annotation.id is not None
    assert annotation.current is True

    return annotation


@pytest.fixture
def existing_unversioned_annotation_status(session, annotation_status_manager, setup_lib_db_with_variant):
    """Fixture to create an existing annotation status in the database."""

    # Add initial annotation
    annotation = annotation_status_manager.add_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
        version=None,
        annotation_data={},
        status=AnnotationStatus.SUCCESS,
        current=True,
    )
    session.commit()

    assert annotation.id is not None
    assert annotation.current is True

    return annotation


@pytest.mark.unit
class TestAnnotationStatusManagerCreateAnnotationUnit:
    """Unit tests for AnnotationStatusManager.add_annotation method."""

    @pytest.mark.parametrize(
        "annotation_type",
        AnnotationType._member_map_.values(),
    )
    @pytest.mark.parametrize(
        "status",
        AnnotationStatus._member_map_.values(),
    )
    def test_add_annotation_creates_entry_with_annotation_type_version_status(
        self, session, annotation_status_manager, annotation_type, status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation creates a new entry with correct type and version."""
        annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=annotation_type,
            version="v1.0",
            annotation_data={},
            current=True,
            status=status,
        )
        session.commit()

        assert annotation.annotation_type == annotation_type.value
        assert annotation.status == status.value
        assert annotation.version == "v1.0"

    def test_add_annotation_persists_annotation_data(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """Test that adding an annotation persists the provided annotation data."""
        annotation_data = {
            "success_data": {"some_key": "some_value"},
            "error_message": None,
            "failure_category": None,
        }
        annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            status=AnnotationStatus.SUCCESS,
            version="v1.0",
            annotation_data=annotation_data,
            current=True,
        )
        session.commit()

        for key, value in annotation_data.items():
            assert getattr(annotation, key) == value

    def test_add_annotation_creates_entry_and_marks_previous_not_current(
        self, session, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation creates a new entry and marks previous ones as not current."""
        manager = AnnotationStatusManager(session)

        # Add second annotation for same (variant, type, version)
        annotation = manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        session.commit()

        assert annotation.id is not None
        assert annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is False

    def test_add_annotation_with_different_version_keeps_previous_current(
        self, session, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation with a different version keeps previous current."""
        manager = AnnotationStatusManager(session)

        # Add second annotation for same (variant, type) but different version
        annotation = manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        assert annotation.id is not None
        assert annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is True

    def test_add_annotation_with_different_type_keeps_previous_current(
        self, session, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation with a different type keeps previous current."""
        manager = AnnotationStatusManager(session)

        # Add second annotation for same variant but different type
        annotation = manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        assert annotation.id is not None
        assert annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is True

    def test_add_annotation_without_version(self, session, annotation_status_manager, setup_lib_db_with_variant):
        """Test that adding an annotation without specifying version works correctly."""
        annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            version=None,
            annotation_data={},
            status=AnnotationStatus.SKIPPED,
            current=True,
        )
        session.commit()

        assert annotation.id is not None
        assert annotation.version is None
        assert annotation.current is True

    def test_add_annotation_multiple_without_version_marks_previous_not_current(
        self, session, annotation_status_manager, existing_unversioned_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding multiple annotations without version marks previous ones as not current."""

        # Add second annotation without version
        second_annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=None,
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        session.commit()

        assert second_annotation.id is not None
        assert second_annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_unversioned_annotation_status)
        assert existing_unversioned_annotation_status.current is False

    def test_add_annotation_different_type_without_version_keeps_previous_current(
        self, session, annotation_status_manager, existing_unversioned_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation of different type without version keeps previous current."""

        # Add second annotation of different type without version
        second_annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version=None,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        assert second_annotation.id is not None
        assert second_annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_unversioned_annotation_status)
        assert existing_unversioned_annotation_status.current is True

    def test_add_annotation_multiple_variants_independent_current_flags(
        self, session, annotation_status_manager, setup_lib_db_with_score_set
    ):
        """Test that adding annotations for different variants maintains independent current flags."""

        variant1 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={})
        variant2 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.2A>T", hgvs_pro="NP_000000.1:p.Met2Val", data={})
        session.add_all([variant1, variant2])
        session.commit()
        session.refresh(variant1)
        session.refresh(variant2)

        # Add annotation for variant 1
        annotation1 = annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        # Add annotation for variant 2
        annotation2 = annotation_status_manager.add_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        assert annotation1.id is not None
        assert annotation1.current is True

        assert annotation2.id is not None
        assert annotation2.current is True


class TestAnnotationStatusManagerGetCurrentAnnotationUnit:
    """Unit tests for AnnotationStatusManager.get_current_annotation method."""

    def test_get_current_annotation_returns_none_when_no_entry(
        self, annotation_status_manager, setup_lib_db_with_variant
    ):
        """Test that getting current annotation returns None when no entry exists."""
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert annotation is None

    def test_get_current_annotation_returns_correct_entry(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that getting current annotation returns the correct entry."""
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert annotation.id == existing_annotation_status.id
        assert annotation.current is True

    def test_get_current_annotation_returns_none_for_non_current(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that getting current annotation returns None when the entry is not current."""
        # Mark existing annotation as not current
        existing_annotation_status.current = False
        session.commit()

        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert annotation is None

    def test_get_current_annotation_with_different_version_returns_none(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that getting current annotation with different version returns None."""
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        assert annotation is None

    def test_get_current_annotation_with_different_type_returns_none(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that getting current annotation with different type returns None."""
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version="v1",
        )
        assert annotation is None

    def test_get_current_annotation_without_version_returns_correct_entry(
        self, session, annotation_status_manager, existing_unversioned_annotation_status, setup_lib_db_with_variant
    ):
        """Test that getting current annotation without version returns the correct entry."""
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=None,
        )
        assert annotation.id == existing_unversioned_annotation_status.id
        assert annotation.current is True


class TestAnnotationStatusManagerIntegration:
    """Integration tests for AnnotationStatusManager methods."""

    def test_add_and_get_current_annotation_work_together(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """Test that adding and getting current annotation work together correctly."""
        # Add annotation
        added_annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        # Get current annotation
        retrieved_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        assert retrieved_annotation is not None
        assert retrieved_annotation.id == added_annotation.id
        assert retrieved_annotation.current is True
        assert retrieved_annotation.status == AnnotationStatus.SUCCESS

    @pytest.mark.parametrize(
        "version",
        ["v1.0", "v2.0", None],
    )
    def test_add_multiple_and_get_current_returns_latest(
        self, session, annotation_status_manager, version, setup_lib_db_with_variant
    ):
        """Test that adding multiple annotations and getting current returns the latest one."""
        # Add first annotation
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        session.commit()

        # Add second annotation
        second_annotation = annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        # Get current annotation
        retrieved_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
        )

        assert retrieved_annotation is not None
        assert retrieved_annotation.id == second_annotation.id
        assert retrieved_annotation.current is True
        assert retrieved_annotation.version == version
        assert retrieved_annotation.status == AnnotationStatus.SUCCESS

    @pytest.mark.parametrize(
        "version",
        ["v1.0", "v2.0", None],
    )
    def test_add_annotations_for_different_variants_and_get_current_independent(
        self, session, annotation_status_manager, version, setup_lib_db_with_score_set
    ):
        """Test that adding annotations for different variants and getting current works independently."""

        variant1 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={})
        variant2 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.2A>T", hgvs_pro="NP_000000.1:p.Met2Val", data={})
        session.add_all([variant1, variant2])
        session.commit()
        session.refresh(variant1)
        session.refresh(variant2)

        # Add annotation for variant 1
        annotation1 = annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        session.commit()

        # Add annotation for variant 2
        annotation2 = annotation_status_manager.add_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        session.commit()

        # Get current annotation for variant 1
        retrieved_annotation1 = annotation_status_manager.get_current_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
        )

        assert retrieved_annotation1 is not None
        assert retrieved_annotation1.id == annotation1.id
        assert retrieved_annotation1.current is True
        assert retrieved_annotation1.status == AnnotationStatus.SUCCESS
        assert retrieved_annotation1.version == version

        # Get current annotation for variant 2
        retrieved_annotation2 = annotation_status_manager.get_current_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
        )

        assert retrieved_annotation2 is not None
        assert retrieved_annotation2.id == annotation2.id
        assert retrieved_annotation2.current is True
        assert retrieved_annotation2.status == AnnotationStatus.FAILED
        assert retrieved_annotation2.version == version
