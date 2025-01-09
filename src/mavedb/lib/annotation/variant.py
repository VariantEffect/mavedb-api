import logging
from typing import Optional

from ga4gh.va_spec.profiles import (
    AssayVariantEffectMeasurementStudyResult,
    AssayVariantEffectFunctionalClassificationStatement,
    AssayVariantEffectClinicalClassificationStatement,
)
from ga4gh.vrs.models import MolecularVariation


from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.classification import (
    functional_classification_of_variant,
    pillar_project_clinical_classification_of_variant,
)
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
    pillar_project_calibration_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
)
from mavedb.lib.annotation.dataset import score_set_to_data_set
from mavedb.lib.annotation.method import (
    publication_identifiers_to_method,
    variant_interpretation_functional_guideline_method,
    variant_interpretation_clinical_guideline_method,
)
from mavedb.lib.annotation.document import variant_to_document, score_set_to_document, experiment_as_iri
from mavedb.view_models.annotated_variant import AnnotatedVariant

logger = logging.getLogger(__name__)


# NOTE: Early VRS 1.3 objects may contain an extra nesting level, where Allele objects
# are contained in a `variation` property. Although it's unlikely variants of this form
# will ever be exported in this format, we handle the possibility.
def _variation_from_mapped_variant(mapped_variant: MappedVariant) -> dict:
    try:
        variation = mapped_variant.post_mapped["variation"]
    except KeyError:
        variation = mapped_variant.post_mapped

    return variation


def mapped_variant_to_variant_effect_measurement_study_result(
    mapped_variant: MappedVariant,
) -> AssayVariantEffectMeasurementStudyResult:
    """
    Create a AssayVariantEffectMeasurementStudyResult object from the provided MaveDB mapped variant. The primary
    goal of this object is to report the variant effect score of a given variant in a functional assay.
    """
    return AssayVariantEffectMeasurementStudyResult(
        sourceDataSet=[score_set_to_data_set(mapped_variant.variant.score_set)],
        focusVariant=MolecularVariation(**_variation_from_mapped_variant(mapped_variant)),
        # This property of this column is guaranteed to be defined.
        score=mapped_variant.variant.data["score_data"]["score"],  # type: ignore
        specifiedBy=publication_identifiers_to_method(
            mapped_variant.variant.score_set.publication_identifier_associations
        ),
        reportedIn=[variant_to_document(mapped_variant.variant)],
        # User contributions are made at the data set level, and thus not tracked here.
        contributions=[mavedb_api_contribution(), mavedb_vrs_contribution(mapped_variant)],
    )


def mapped_variant_to_functional_classification_statement(
    mapped_variant: MappedVariant,
) -> Optional[AssayVariantEffectFunctionalClassificationStatement]:
    """
    Create a AssayVariantEffectFunctionalClassificationStatement object from the provided MaveDB mapped variant. The primary
    goal of this object is to assign a functional classification to a variant effect from a functional assay.
    """
    classification = functional_classification_of_variant(mapped_variant)

    if classification is None:
        return None

    return AssayVariantEffectFunctionalClassificationStatement(
        subjectVariant=MolecularVariation(**_variation_from_mapped_variant(mapped_variant)),
        objectAssay=experiment_as_iri(mapped_variant.variant.score_set.experiment),
        classification=classification,
        specifiedBy=variant_interpretation_functional_guideline_method(),
        reportedIn=[
            score_set_to_document(mapped_variant.variant.score_set),
            variant_to_document(mapped_variant.variant),
        ],
        # Because this informational entity spans both a variant and the assay to which it belongs, we report
        # contributions to both entities here.
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            # TODO: Is this potentially misleading? We aren't actually sure who last modified the variant.
            # mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
            mavedb_creator_contribution(mapped_variant.variant.score_set, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(
                mapped_variant.variant.score_set, mapped_variant.variant.score_set.modified_by
            ),
        ],
    )


def mapped_variant_to_clinical_classification_statement(
    mapped_variant: MappedVariant,
) -> Optional[AssayVariantEffectClinicalClassificationStatement]:
    """
    Create a AssayVariantEffectClinicalClassificationStatement object from the provided MaveDB mapped variant. The primary
    goal of this object is to assign a clinical strength of evidence to a variant effect from a functional assay.
    """
    classification = pillar_project_clinical_classification_of_variant(mapped_variant)

    if classification is None:
        return None

    return AssayVariantEffectClinicalClassificationStatement(
        subjectVariant=MolecularVariation(**_variation_from_mapped_variant(mapped_variant)),
        objectAssay=experiment_as_iri(mapped_variant.variant.score_set.experiment),
        classification=classification,
        specifiedBy=variant_interpretation_clinical_guideline_method(),
        reportedIn=[
            score_set_to_document(mapped_variant.variant.score_set),
            variant_to_document(mapped_variant.variant),
        ],
        # Because this informational entity spans both a variant and the assay to which it belongs, we report
        # contributions to both entities here.
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            pillar_project_calibration_contribution(),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            # TODO: Is this potentially misleading? We aren't actually sure who last modified the variant.
            # mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
            mavedb_creator_contribution(mapped_variant.variant.score_set, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(
                mapped_variant.variant.score_set, mapped_variant.variant.score_set.modified_by
            ),
        ],
    )


def annotation_for_variant(mapped_variant: MappedVariant) -> AnnotatedVariant:
    return {
        f"{mapped_variant.variant.urn}": {
            "AssayVariantEffectMeasurementStudyResult": mapped_variant_to_variant_effect_measurement_study_result(
                mapped_variant
            ),
            "AssayVariantEffectFunctionalClassificationStatement": mapped_variant_to_functional_classification_statement(
                mapped_variant
            ),
            "AssayVariantEffectClinicalClassificationStatement": mapped_variant_to_clinical_classification_statement(
                mapped_variant
            ),
        }
    }
