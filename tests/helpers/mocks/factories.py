"""
Shared mock object factories for the test suite.

This module provides factory functions for creating mock objects that are commonly
used across different test modules, reducing duplication and ensuring consistency.
"""

from datetime import date, datetime
from unittest.mock import MagicMock

from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided
from tests.helpers.constants import (
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
)
from tests.helpers.mocks.mock_utilities import (
    MockObjectWithPydanticFunctionality,
    MockVariantCollection,
    create_sealed_mock,
)

# ---------------------------------------------------------------------------
# License and Legal Helpers
# ---------------------------------------------------------------------------


def create_mock_license(
    short_name="CC BY 4.0",
    long_name="Creative Commons Attribution 4.0 International",
    version="4.0",
    link="default",
):
    """Create a mock license with specified properties."""
    return create_sealed_mock(
        short_name=short_name,
        long_name=long_name,
        version=version,
        link=(
            f"https://creativecommons.org/licenses/{short_name.lower().replace(' ', '-')}/4.0/"
            if link == "default"
            else link
        ),
    )


# ---------------------------------------------------------------------------
# User and Identity Helpers
# ---------------------------------------------------------------------------


def create_mock_user(username="Test User", first_name="Test", last_name="User", **kwargs):
    """Create a mock User with specified properties for both Pydantic validation and direct attribute access."""
    data = {
        "username": str(username),
        "firstName": first_name,
        "lastName": last_name,
        "recordType": "User",
        "email": kwargs.get("email", "test@example.com"),
        "orcidId": kwargs.get("orcid_id", "0000-0000-0000-0000"),
    }
    return MockObjectWithPydanticFunctionality(data)


# ---------------------------------------------------------------------------
# Publication and Reference Helpers
# ---------------------------------------------------------------------------


def create_mock_publication(url="https://example.com/publication", title="Test Publication"):
    """Create a mock Publication with specified properties."""
    return create_sealed_mock(url=url, title=title)


def create_mock_score_set_publication_identifier_association(publication=None, primary=True):
    """Create a mock ScoreSetPublicationIdentifierAssociation."""
    return create_sealed_mock(publication=publication or create_mock_publication(), primary=primary)


# ---------------------------------------------------------------------------
# Functional Classification Helpers
# ---------------------------------------------------------------------------


def create_mock_functional_classification(
    functional_classification=FunctionalClassificationOptions.not_specified,
    label="Test Classification",
    range_values=None,
    acmg_classification=None,
    variant_in_range=True,
):
    """Create a functional classification mock for both Pydantic validation and direct attribute access.

    By default, ``acmg_classification`` is None (appropriate for functional-only
    calibrations). For pathogenicity calibrations, pass an explicit ACMG dict whose
    criterion matches the functional classification (abnormal -> PS3, normal -> BS3).
    """
    # Coerce to enum instance for `is` comparisons in classification.py
    if isinstance(functional_classification, str):
        fc_enum = FunctionalClassificationOptions(functional_classification)
    else:
        fc_enum = functional_classification

    data = {
        "label": label,
        "description": f"Test description for {label}",
        "functionalClassification": fc_enum,
        "range": range_values,
        "acmgClassification": acmg_classification,
        "inclusiveLowerBound": True if range_values else None,
        "inclusiveUpperBound": False if range_values else None,
        "recordType": "ScoreCalibrationFunctionalClassification",
        "variants": MockVariantCollection(variant_in_range),
    }

    return MockObjectWithPydanticFunctionality(data)


# ---------------------------------------------------------------------------
# Project Structure Helpers
# ---------------------------------------------------------------------------


def create_mock_experiment(urn="urn:mavedb:00000001-a", title="Test Experiment", short_description="A test experiment"):
    """Create a mock Experiment with specified properties."""
    return create_sealed_mock(urn=urn, title=title, short_description=short_description)


def create_mock_target_gene(name="BRCA1", category="Protein coding", external_identifiers=None):
    """Create a mock TargetGene with specified properties."""
    return create_sealed_mock(
        name=name,
        category=category,
        external_identifiers=external_identifiers or [],
        mapped_hgnc_name=name,  # Common requirement for annotation tests
    )


def create_mock_score_set(
    urn="urn:mavedb:00000001-a-1",
    title="Test Score Set",
    short_description="A test score set for validation",
    published_date=None,
    license=None,
    **kwargs,
):
    """Create a mock ScoreSet with specified properties."""
    mock_user = create_mock_user()
    mock_target_gene = create_mock_target_gene()
    mock_experiment = create_mock_experiment()

    return create_sealed_mock(
        urn=urn,
        title=title,
        short_description=short_description,
        published_date=published_date if published_date is not None else date(2024, 1, 15),
        license=license or create_mock_license(),
        target_genes=[mock_target_gene],
        score_calibrations=[],
        experiment=mock_experiment,
        created_by=mock_user,
        modified_by=mock_user,
        publication_identifier_associations=[],
        **kwargs,
    )


def create_mock_score_calibration(functional_classifications=None, primary=True, **kwargs):
    """Create a mock ScoreCalibration for both Pydantic validation and direct attribute access.

    When ``functional_classifications`` is not provided a minimal default list is
    created (single not_specified classification with no ACMG).
    """
    user = kwargs.get("created_by") or create_mock_user()

    if functional_classifications is None:
        functional_classifications = [
            create_mock_functional_classification(
                label="Default Classification",
                functional_classification=FunctionalClassificationOptions.not_specified,
                range_values=[0.0, 1.0],
            )
        ]

    data = {
        "id": kwargs.get("id", 1),
        "urn": kwargs.get("urn", "test:calibration:1"),
        "scoreSetId": kwargs.get("score_set_id", 1),
        "scoreSet": kwargs.get("score_set"),
        "title": kwargs.get("title", "Test Calibration"),
        "baselineScore": kwargs.get("baseline_score", 0.0),
        "baselineScoreDescription": kwargs.get("baseline_score_description", "Test baseline description"),
        "notes": kwargs.get("notes", "Test notes"),
        "calibrationMetadata": kwargs.get(
            "calibration_metadata", {"method": "test_method", "description": "Test calibration metadata"}
        ),
        "recordType": kwargs.get("record_type", "ScoreCalibration"),
        "createdBy": user,
        "modifiedBy": user,
        "creationDate": kwargs.get("creation_date", date(2024, 1, 1)),
        "modificationDate": kwargs.get("modification_date", date(2024, 1, 1)),
        "investigatorProvided": kwargs.get("investigator_provided", True),
        "primary": primary,
        "private": kwargs.get("private", not primary),
        "researchUseOnly": kwargs.get("research_use_only", False),
        "thresholdSources": kwargs.get("threshold_sources", []),
        "classificationSources": kwargs.get("classification_sources", []),
        "methodSources": kwargs.get("method_sources", []),
        "publicationIdentifierAssociations": kwargs.get("publication_identifier_associations", []),
        "functionalClassifications": functional_classifications,
    }

    return MockObjectWithPydanticFunctionality(data)


def create_mock_acmg_classification(
    criterion=ACMGCriterion.PS3.name,
    evidence_strength=StrengthOfEvidenceProvided.STRONG.name,
):
    """Create a mock ACMG classification with specified criterion and evidence strength."""
    # Map string values to enum values
    criterion_enum = getattr(ACMGCriterion, criterion) if isinstance(criterion, str) else criterion
    strength_enum = (
        getattr(StrengthOfEvidenceProvided, evidence_strength)
        if isinstance(evidence_strength, str)
        else evidence_strength
    )

    # Calculate points based on criterion and strength
    points_map = {
        "VERY_STRONG": 8,
        "STRONG": 4,
        "MODERATE_PLUS": 3,
        "MODERATE": 2,
        "SUPPORTING": 1,
    }
    points = points_map.get(evidence_strength if isinstance(evidence_strength, str) else evidence_strength.name, 0)

    return MockObjectWithPydanticFunctionality(
        {
            "criterion": criterion_enum,
            "evidenceStrength": strength_enum,
            "points": points,
            "creationDate": date.today().isoformat(),
            "modificationDate": date.today().isoformat(),
        }
    )


def create_mock_pathogenicity_range(acmg_classification=None, variant_in_range=True):
    """Create a mock pathogenicity range (functional classification with ACMG classification).

    Args:
        acmg_classification: Optional ACMG classification. If explicitly None, no classification is added.
        variant_in_range: Whether the variant should be considered in this range (default True).
    """
    classification_kwargs = {}
    if acmg_classification is not None:
        classification_kwargs["acmg_classification"] = acmg_classification

    return create_mock_functional_classification(
        functional_classification=FunctionalClassificationOptions.abnormal,
        label="Pathogenic Range",
        range_values=[0.7, 1.0],
        variant_in_range=variant_in_range,
        **classification_kwargs,
    )


def create_mock_score_calibration_with_ranges(score_set=None, user=None):
    """Create a mock score calibration with functional classifications (legacy wrapper for compatibility)."""
    functional_classifications = [
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.normal,
            label="Normal Range",
            range_values=[-1.0, 0.3],
        ),
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.abnormal,
            label="Abnormal Range",
            range_values=[0.7, 1.0],
        ),
    ]
    return create_mock_score_calibration(functional_classifications=functional_classifications)


# ---------------------------------------------------------------------------
# Variant and Mapping Helpers
# ---------------------------------------------------------------------------


def create_mock_variant(urn="test:variant", score=0.5, score_set=None):
    """Create a mock Variant with specified properties."""
    return create_sealed_mock(
        urn=urn,
        data={"score_data": {"score": score}},
        score_set=score_set or create_mock_score_set(),
        id=1,
        score=score,
        mapping_api_version="1.0",
        mapped_date=datetime(2024, 1, 15, 10, 30, 0),
        creation_date=datetime(2024, 1, 1, 0, 0, 0),
        modification_date=datetime(2024, 1, 1, 0, 0, 0),
        hgvs_nt="NM_000001.1:c.1A>G",
        hgvs_pro="NP_000001.1:p.Met1Val",
        hgvs_splice=None,
    )


def create_mock_mapped_variant(
    urn="test:urn",
    score=0.5,
    mapping_api_version="1.0",
    mapped_date=None,
    clingen_allele_id=None,
    score_set=None,
):
    """Create a mock MappedVariant with specified properties."""
    mock_variant = create_mock_variant(urn=urn, score=score, score_set=score_set)

    return create_sealed_mock(
        variant=mock_variant,
        mapping_api_version=mapping_api_version,
        mapped_date=mapped_date or datetime(2024, 1, 15, 10, 30, 0),
        clingen_allele_id=clingen_allele_id,
        pre_mapped=TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
        post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    )


# ---------------------------------------------------------------------------
# Utility Helpers
# ---------------------------------------------------------------------------


def create_mock_resource_with_dates(class_name="TestResource", creation_date=None, modification_date=None):
    """Create a mock resource with creation/modification dates."""
    mock_resource = MagicMock()
    mock_resource.__class__.__name__ = class_name
    mock_resource.creation_date = creation_date or datetime(2024, 1, 1, 9, 0, 0)
    mock_resource.modification_date = modification_date or datetime(2024, 1, 5, 15, 30, 0)
    return mock_resource


# ---------------------------------------------------------------------------
# Score Set with Calibrations Helpers
# ---------------------------------------------------------------------------


def create_mock_functional_calibration_score_set(**kwargs):
    """Create a mock score set with functional calibrations (no ACMG classifications)."""
    score_set = create_mock_score_set(**kwargs)

    functional_classifications = [
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.normal,
            label="Normal Function",
            range_values=[0.0, 1.0],
            variant_in_range=True,
        ),
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.abnormal,
            label="Abnormal Function",
            range_values=[-1.0, 0.0],
            variant_in_range=True,
        ),
    ]

    calibration = create_mock_score_calibration(
        functional_classifications=functional_classifications,
        primary=True,
        score_set=score_set,
    )
    score_set.score_calibrations = [calibration]
    return score_set


def create_mock_pathogenicity_calibration_score_set(**kwargs):
    """Create a mock score set with pathogenicity calibrations (ACMG classifications included)."""
    score_set = create_mock_score_set(**kwargs)

    pathogenicity_classifications = [
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.abnormal,
            label="Pathogenic Range",
            range_values=[0.7, 1.0],
            acmg_classification=MockObjectWithPydanticFunctionality(
                {
                    "criterion": ACMGCriterion.PS3,
                    "evidenceStrength": StrengthOfEvidenceProvided.STRONG,
                    "points": 4,
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                }
            ),
            variant_in_range=True,
        ),
        create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.normal,
            label="Benign Range",
            range_values=[-1.0, -0.7],
            acmg_classification=MockObjectWithPydanticFunctionality(
                {
                    "criterion": ACMGCriterion.BS3,
                    "evidenceStrength": StrengthOfEvidenceProvided.STRONG,
                    "points": -4,
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                }
            ),
            variant_in_range=True,
        ),
    ]

    calibration = create_mock_score_calibration(
        functional_classifications=pathogenicity_classifications,
        primary=True,
        score_set=score_set,
    )
    score_set.score_calibrations = [calibration]
    return score_set


def create_mock_mapped_variant_with_functional_calibration_score_set(**kwargs):
    """Create a mock mapped variant with functional calibration score set."""
    score_set = create_mock_functional_calibration_score_set()
    return create_mock_mapped_variant(score_set=score_set, **kwargs)


def create_mock_mapped_variant_with_pathogenicity_calibration_score_set(**kwargs):
    """Create a mock mapped variant with pathogenicity calibration score set."""
    score_set = create_mock_pathogenicity_calibration_score_set()
    return create_mock_mapped_variant(score_set=score_set, **kwargs)
