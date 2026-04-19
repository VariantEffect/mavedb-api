# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

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
    annotation_status_manager.add_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
        version="v1",
        annotation_data={},
        status=AnnotationStatus.SUCCESS,
        current=True,
    )
    annotation_status_manager.flush()
    session.commit()

    annotation = annotation_status_manager.get_current_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
        version="v1",
    )

    assert annotation.id is not None
    assert annotation.current is True

    return annotation


@pytest.fixture
def existing_unversioned_annotation_status(session, annotation_status_manager, setup_lib_db_with_variant):
    """Fixture to create an existing annotation status in the database."""

    # Add initial annotation
    annotation_status_manager.add_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
        version=None,
        annotation_data={},
        status=AnnotationStatus.SUCCESS,
        current=True,
    )
    annotation_status_manager.flush()
    session.commit()

    annotation = annotation_status_manager.get_current_annotation(
        variant_id=setup_lib_db_with_variant.id,
        annotation_type=AnnotationType.VRS_MAPPING,
    )

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
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=annotation_type,
            version="v1.0",
            annotation_data={},
            current=True,
            status=status,
        )
        annotation_status_manager.flush()
        session.commit()

        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=annotation_type,
            version="v1.0",
        )

        assert annotation is not None
        assert annotation.annotation_type == annotation_type
        assert annotation.status == status
        assert annotation.version == "v1.0"

    def test_add_annotation_persists_annotation_data(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """Test that adding an annotation persists the provided annotation data."""
        annotation_data = {
            "annotation_metadata": {"some_key": "some_value"},
            "error_message": None,
            "failure_category": None,
        }
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            status=AnnotationStatus.SUCCESS,
            version="v1.0",
            annotation_data=annotation_data,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1.0",
        )

        assert annotation is not None
        for key, value in annotation_data.items():
            assert getattr(annotation, key) == value

    def test_add_annotation_creates_entry_and_marks_previous_not_current(
        self, session, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding an annotation creates a new entry and marks previous ones as not current."""
        manager = AnnotationStatusManager(session)

        # Add second annotation for same (variant, type, version)
        manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        manager.flush()
        session.commit()

        annotation = manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        assert annotation is not None
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
        manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        manager.flush()
        session.commit()

        annotation = manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )

        assert annotation is not None
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
        manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        manager.flush()
        session.commit()

        annotation = manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version="v1",
        )

        assert annotation is not None
        assert annotation.id is not None
        assert annotation.current is True

        # Refresh first annotation from DB
        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is True

    def test_add_annotation_without_version(self, session, annotation_status_manager, setup_lib_db_with_variant):
        """Test that adding an annotation without specifying version works correctly."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            version=None,
            annotation_data={},
            status=AnnotationStatus.SKIPPED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
        )

        assert annotation is not None
        assert annotation.id is not None
        assert annotation.version is None
        assert annotation.current is True

    def test_add_annotation_multiple_without_version_marks_previous_not_current(
        self, session, annotation_status_manager, existing_unversioned_annotation_status, setup_lib_db_with_variant
    ):
        """Test that adding multiple annotations without version marks previous ones as not current."""

        # Add second annotation without version
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=None,
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        second_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
        )

        assert second_annotation is not None
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
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version=None,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        second_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
        )

        assert second_annotation is not None
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
        annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )

        # Add annotation for variant 2
        annotation_status_manager.add_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        annotation1 = annotation_status_manager.get_current_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        annotation2 = annotation_status_manager.get_current_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        assert annotation1 is not None
        assert annotation1.id is not None
        assert annotation1.current is True

        assert annotation2 is not None
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
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        # Get current annotation
        retrieved_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        assert retrieved_annotation is not None
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
        annotation_status_manager.flush()
        session.commit()

        # Add second annotation
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        # Get current annotation
        retrieved_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
        )

        assert retrieved_annotation is not None
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
        annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )

        # Add annotation for variant 2
        annotation_status_manager.add_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        # Get current annotation for variant 1
        retrieved_annotation1 = annotation_status_manager.get_current_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version=version,
        )

        assert retrieved_annotation1 is not None
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
        assert retrieved_annotation2.current is True
        assert retrieved_annotation2.status == AnnotationStatus.FAILED
        assert retrieved_annotation2.version == version


@pytest.mark.unit
class TestAnnotationStatusManagerReplaceAllVersionsUnit:
    """Unit tests for the replace_all_versions parameter of AnnotationStatusManager.add_annotation."""

    def test_replace_all_versions_false_keeps_different_version_current(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """Default behavior: a new annotation only retires the same version, not others."""
        # existing_annotation_status is version "v1", current=True
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()
        session.commit()

        new_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        assert new_annotation is not None
        assert new_annotation.current is True

        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is True

    def test_replace_all_versions_true_retires_all_versions(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """replace_all_versions=True retires all current records for (variant, type) regardless of version."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()

        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()
        session.commit()

        # Both v1 and v2 are current at this point (replace_all_versions=False)
        v1 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        v2 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        assert v1 is not None and v1.current is True
        assert v2 is not None and v2.current is True

        # Now add v3 with replace_all_versions=True — should retire both v1 and v2
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v3",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=True,
        )
        annotation_status_manager.flush()
        session.commit()

        session.refresh(v1)
        session.refresh(v2)
        assert v1.current is False
        assert v2.current is False

        v3 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v3",
        )
        assert v3 is not None and v3.current is True

    def test_replace_all_versions_true_only_affects_matching_type(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """replace_all_versions=True only retires records for the same annotation_type."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        vrs = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        clinvar = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v1",
        )

        # replace VRS_MAPPING only
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=True,
        )
        annotation_status_manager.flush()
        session.commit()

        session.refresh(vrs)
        session.refresh(clinvar)
        assert vrs.current is False
        assert clinvar.current is True

        new_vrs = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        assert new_vrs is not None and new_vrs.current is True

    def test_replace_all_versions_true_only_affects_matching_variant(
        self, session, annotation_status_manager, setup_lib_db_with_score_set
    ):
        """replace_all_versions=True only retires records for the same variant_id."""
        variant1 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={})
        variant2 = Variant(score_set_id=1, hgvs_nt="NM_000000.1:c.2A>T", hgvs_pro="NP_000000.1:p.Met2Val", data={})
        session.add_all([variant1, variant2])
        session.commit()
        session.refresh(variant1)
        session.refresh(variant2)

        annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        ann1 = annotation_status_manager.get_current_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        ann2 = annotation_status_manager.get_current_annotation(
            variant_id=variant2.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        # replace variant1 only
        annotation_status_manager.add_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=True,
        )
        annotation_status_manager.flush()
        session.commit()

        session.refresh(ann1)
        session.refresh(ann2)
        assert ann1.current is False
        assert ann2.current is True  # untouched

        new_ann1 = annotation_status_manager.get_current_annotation(
            variant_id=variant1.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        assert new_ann1 is not None and new_ann1.current is True

    def test_replace_all_versions_true_same_version_also_retired(
        self, session, annotation_status_manager, existing_annotation_status, setup_lib_db_with_variant
    ):
        """replace_all_versions=True retires a same-version record just as replace_all_versions=False would."""
        # existing_annotation_status is version "v1"
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
            replace_all_versions=True,
        )
        annotation_status_manager.flush()
        session.commit()

        session.refresh(existing_annotation_status)
        assert existing_annotation_status.current is False

        new_annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert new_annotation is not None
        assert new_annotation.current is True
        assert new_annotation.status == AnnotationStatus.FAILED


@pytest.mark.unit
class TestAnnotationStatusManagerBatchingUnit:
    """Unit tests for batching and flush behavior."""

    def test_flush_noop_when_empty(self, annotation_status_manager):
        """flush() with no pending annotations does nothing and does not error."""
        annotation_status_manager.flush()  # should not raise

    def test_auto_flush_at_batch_size(self, session, setup_lib_db_with_score_set):
        """Annotations are auto-flushed to the DB when batch_size is reached."""
        variants = [
            Variant(score_set_id=1, hgvs_nt=f"NM_000000.1:c.{i}A>G", hgvs_pro=f"NP_000000.1:p.Met{i}Val", data={})
            for i in range(3)
        ]
        session.add_all(variants)
        session.commit()
        for v in variants:
            session.refresh(v)

        manager = AnnotationStatusManager(session, batch_size=2)

        # Add first — stays pending (below threshold)
        manager.add_annotation(
            variant_id=variants[0].id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        assert len(manager._pending) == 1

        # Add second — triggers auto-flush (reaches batch_size=2)
        manager.add_annotation(
            variant_id=variants[1].id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        assert len(manager._pending) == 0  # flushed

        # Verify the auto-flushed rows are visible in the DB
        ann = manager.get_current_annotation(
            variant_id=variants[0].id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert ann is not None and ann.current is True

        # Add a third — stays pending (below threshold again)
        manager.add_annotation(
            variant_id=variants[2].id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        assert len(manager._pending) == 1

        # Explicit flush persists the remainder
        manager.flush()
        assert len(manager._pending) == 0

        ann3 = manager.get_current_annotation(
            variant_id=variants[2].id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert ann3 is not None and ann3.current is True

    def test_get_current_annotation_auto_flushes_pending(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_current_annotation() flushes pending writes before querying."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        # No explicit flush — get_current_annotation should auto-flush
        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        assert annotation is not None
        assert annotation.current is True
        assert len(annotation_status_manager._pending) == 0

    def test_flush_clears_internal_buffers(self, session, annotation_status_manager, setup_lib_db_with_variant):
        """flush() clears both _pending and _retirement_filters."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        assert len(annotation_status_manager._pending) == 1
        assert len(annotation_status_manager._retirement_filters) == 1

        annotation_status_manager.flush()
        assert len(annotation_status_manager._pending) == 0
        assert len(annotation_status_manager._retirement_filters) == 0

    def test_batch_retirement_groups_by_annotation_type(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """Multiple annotation types in one batch are retired independently."""
        # Create initial annotations for two types
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        vrs_v1 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        clinvar_v1 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v1",
        )

        # Now add replacements for both types in one batch
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        session.refresh(vrs_v1)
        session.refresh(clinvar_v1)
        assert vrs_v1.current is False
        assert clinvar_v1.current is False

        vrs_v2 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
        )
        clinvar_v2 = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="v2",
        )
        assert vrs_v2 is not None and vrs_v2.current is True
        assert clinvar_v2 is not None and clinvar_v2.current is True


@pytest.mark.unit
class TestAnnotationStatusManagerAuditHelpersUnit:
    """Unit tests for audit query helpers: get_annotation_history and get_all_current_annotations."""

    def test_get_annotation_history_returns_all_rows_newest_first(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_annotation_history returns both current and retired rows, newest first."""
        # Create two annotations for the same (variant, type, version) — first gets retired
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()

        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        history = annotation_status_manager.get_annotation_history(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )

        assert len(history) == 2
        # Newest first
        assert history[0].status == AnnotationStatus.FAILED
        assert history[0].current is True
        assert history[1].status == AnnotationStatus.SUCCESS
        assert history[1].current is False

    def test_get_annotation_history_filters_by_version(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_annotation_history with version only returns matching rows."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-01",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-02",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()
        session.commit()

        history_jan = annotation_status_manager.get_annotation_history(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-01",
        )
        assert len(history_jan) == 1
        assert history_jan[0].version == "2025-01"

    def test_get_annotation_history_without_version_returns_all_versions(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_annotation_history without version returns rows across all versions."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-01",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-02",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()
        session.commit()

        history = annotation_status_manager.get_annotation_history(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
        )
        assert len(history) == 2

    def test_get_annotation_history_empty_for_no_records(self, annotation_status_manager, setup_lib_db_with_variant):
        """get_annotation_history returns empty list when no records exist."""
        history = annotation_status_manager.get_annotation_history(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
        )
        assert history == []

    def test_get_annotation_history_auto_flushes_pending(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_annotation_history flushes pending writes before querying."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        # No explicit flush
        history = annotation_status_manager.get_annotation_history(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
        )
        assert len(history) == 1
        assert len(annotation_status_manager._pending) == 0

    def test_get_all_current_annotations_returns_all_types(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_all_current_annotations returns current annotations across all types."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-01",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version=None,
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        all_current = annotation_status_manager.get_all_current_annotations(
            variant_id=setup_lib_db_with_variant.id,
        )
        assert len(all_current) == 3
        types = {a.annotation_type for a in all_current}
        assert types == {
            AnnotationType.VRS_MAPPING,
            AnnotationType.CLINVAR_CONTROL,
            AnnotationType.CLINGEN_ALLELE_ID,
        }

    def test_get_all_current_annotations_excludes_retired(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_all_current_annotations does not include retired rows."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()

        # Replace it — v1 becomes retired
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v2",
            annotation_data={},
            status=AnnotationStatus.FAILED,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        all_current = annotation_status_manager.get_all_current_annotations(
            variant_id=setup_lib_db_with_variant.id,
        )
        assert len(all_current) == 1
        assert all_current[0].version == "v2"

    def test_get_all_current_annotations_empty_for_no_records(
        self, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_all_current_annotations returns empty list when no records exist."""
        result = annotation_status_manager.get_all_current_annotations(
            variant_id=setup_lib_db_with_variant.id,
        )
        assert result == []

    def test_get_all_current_annotations_auto_flushes_pending(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_all_current_annotations flushes pending writes before querying."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        # No explicit flush
        result = annotation_status_manager.get_all_current_annotations(
            variant_id=setup_lib_db_with_variant.id,
        )
        assert len(result) == 1
        assert len(annotation_status_manager._pending) == 0

    def test_get_all_current_annotations_ordered_by_type_then_version(
        self, session, annotation_status_manager, setup_lib_db_with_variant
    ):
        """get_all_current_annotations returns results ordered by annotation_type, version."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-02",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version="2025-01",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
            replace_all_versions=False,
        )
        annotation_status_manager.flush()
        session.commit()

        all_current = annotation_status_manager.get_all_current_annotations(
            variant_id=setup_lib_db_with_variant.id,
        )
        assert len(all_current) == 3
        # clinvar_control < vrs_mapping alphabetically
        assert all_current[0].annotation_type == AnnotationType.CLINVAR_CONTROL
        assert all_current[0].version == "2025-01"
        assert all_current[1].annotation_type == AnnotationType.CLINVAR_CONTROL
        assert all_current[1].version == "2025-02"
        assert all_current[2].annotation_type == AnnotationType.VRS_MAPPING


@pytest.mark.unit
class TestVariantAnnotationStatusReprUnit:
    """Unit tests for the VariantAnnotationStatus __repr__ method."""

    def test_repr_includes_key_fields(self, session, annotation_status_manager, setup_lib_db_with_variant):
        """__repr__ includes id, variant_id, type, version, status, current, and created_at."""
        annotation_status_manager.add_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
            annotation_data={},
            status=AnnotationStatus.SUCCESS,
            current=True,
        )
        annotation_status_manager.flush()
        session.commit()

        annotation = annotation_status_manager.get_current_annotation(
            variant_id=setup_lib_db_with_variant.id,
            annotation_type=AnnotationType.VRS_MAPPING,
            version="v1",
        )
        repr_str = repr(annotation)

        assert "VariantAnnotationStatus" in repr_str
        assert f"id={annotation.id}" in repr_str
        assert f"variant_id={setup_lib_db_with_variant.id}" in repr_str
        assert "type='vrs_mapping'" in repr_str
        assert "version='v1'" in repr_str
        assert "status='success'" in repr_str
        assert "current=True" in repr_str
        assert "created_at=" in repr_str
