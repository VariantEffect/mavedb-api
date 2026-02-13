from collections.abc import Sequence
from typing import Any, Literal, Optional

from ga4gh.core.models import Extension
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided as VaSpecStrengthOfEvidenceProvided
from ga4gh.vrs.models import (
    Allele,
    CisPhasedBlock,
    Expression,
    LiteralSequenceExpression,
    MolecularVariation,
    SequenceLocation,
    SequenceReference,
)

from mavedb.lib.annotation.classification import (
    ExperimentalVariantFunctionalImpactClassification,
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata
from mavedb.lib.types.annotation import SequenceFeature
from mavedb.lib.variants import target_for_variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification


def serialize_evidence_items(evidence: Sequence[Any]) -> list[dict[str, Any]]:
    """Serialize evidence objects to dictionaries using `model_dump(exclude_none=True)`.

    Args:
        evidence (Sequence[Any]): Evidence objects expected to provide a
            `model_dump` method.

    Returns:
        list[dict[str, Any]]: Evidence payloads suitable for assignment to
            GA4GH VA model fields such as `hasEvidenceItems`.

    Raises:
        TypeError: If any item does not expose a callable `model_dump` method.
    """

    serialized_evidence: list[dict[str, Any]] = []

    for evidence_item in evidence:
        model_dump = getattr(evidence_item, "model_dump", None)
        if not callable(model_dump):
            raise TypeError("Evidence items must provide a callable model_dump method.")

        serialized_evidence.append(model_dump(exclude_none=True))

    return serialized_evidence


def allele_from_mapped_variant_dictionary_result(allelic_mapping_results: dict) -> Allele:
    """
    Converts a dictionary containing allelic mapping results into an Allele object.

    This function handles the possibility of an extra nesting level in early VRS 1.3 objects,
    where Allele objects are contained within a `variation` property. If the `variation` key
    is not present, the function assumes the dictionary itself represents the variation.

    Args:
        allelic_mapping_results (dict): A dictionary containing allelic mapping results.
            It may include a `variation` key or directly represent the variation.

    Returns:
        Allele: An Allele object constructed from the provided mapping results.

    Raises:
        KeyError: If required keys are missing from the input dictionary.
    """

    # NOTE: Early VRS 1.3 objects may contain an extra nesting level, where Allele objects
    # are contained in a `variation` property. Although it's unlikely variants of this form
    # will ever be exported in this format, we handle the possibility.
    try:
        variation = allelic_mapping_results["variation"]
    except KeyError:
        variation = allelic_mapping_results

    return Allele(
        id=variation.get("id"),
        state=LiteralSequenceExpression(**variation["state"]),
        digest=variation.get("digest"),
        location=SequenceLocation(
            start=variation.get("location", {}).get("start"),
            end=variation.get("location", {}).get("end"),
            digest=variation.get("location", {}).get("digest"),
            id=variation.get("location", {}).get("id"),
            sequenceReference=SequenceReference(
                name=variation.get("location", {}).get("sequenceReference", {}).get("name"),
                refgetAccession=variation.get("location", {}).get("sequenceReference", {}).get("refgetAccession"),
            ),
        ),
        extensions=[
            Extension(
                id=extension.get("id"),
                name=extension["name"],
                description=extension.get("description"),
                value=extension.get("value"),
            )
            for extension in variation.get("extensions", [])
        ],
        expressions=[
            Expression(
                id=expression.get("id"),
                syntax=expression["syntax"],
                syntax_version=expression.get("syntax_version"),
                value=expression["value"],
            )
            for expression in variation.get("expressions", [])
        ],
    )


def vrs_object_from_mapped_variant(mapping_results: dict) -> MolecularVariation:
    """
    Extracts a VRS (Variation Representation Specification) object from a mapped variant.

    This function processes a dictionary of mapping results and returns a VRS object,
    which can either be an `Allele` or a `CisPhasedBlock`. The type of VRS object
    returned depends on the "type" field in the input dictionary.

    Args:
        mapping_results (dict): A dictionary containing the mapping results of a variant.
            It must include a "type" key indicating the type of VRS object
            ("CisPhasedBlock" or "Haplotype" for a CisPhasedBlock, or "Allele").
            If the type is "CisPhasedBlock" or "Haplotype", the dictionary must also
            include a "members" key containing a list of member alleles.

    Returns:
        MolecularVariation: A VRS object representing the mapped variant. This will be
            either a `CisPhasedBlock` containing its member variants or an `Allele`
            derived from the mapping results.

    Raises:
        KeyError: If required keys are missing from the `mapping_results` dictionary.
    """
    if mapping_results.get("type") == "CisPhasedBlock" or mapping_results.get("type") == "Haplotype":
        return MolecularVariation(
            # It's unclear why MyPy complains about the missing id field, so just add it as None (it is None by default anyway)
            CisPhasedBlock(
                id=None,
                members=[allele_from_mapped_variant_dictionary_result(member) for member in mapping_results["members"]],
            )
        )

    return MolecularVariation(allele_from_mapped_variant_dictionary_result(mapping_results))


def variation_from_mapped_variant(mapped_variant: MappedVariant) -> MolecularVariation:
    """
    Converts mapping results from a mapped variant into a MolecularVariation object.

    This function takes a `MappedVariant` object and extracts its post-mapped
    variant to generate a corresponding `MolecularVariation` object. If the
    `post_mapped` attribute of the `MappedVariant` is `None`, an exception is raised.

    Args:
        mapped_variant (MappedVariant): The mapped variant object containing
            the variant data.

    Returns:
        MolecularVariation: The molecular variation derived from the post-mapped
            variant.

    Raises:
        MappingDataDoesntExistException: If the `post_mapped` attribute of the
            `mapped_variant` is `None`, indicating that the post-mapped variant
            data is unavailable.
    """
    if mapped_variant.post_mapped is None:
        raise MappingDataDoesntExistException(
            f"Variant {mapped_variant.variant.urn} does not have a post mapped variant."
            " Unable to extract variation data."
        )

    return vrs_object_from_mapped_variant(mapped_variant.post_mapped)


def _can_annotate_variant_base_assumptions(mapped_variant: MappedVariant) -> bool:
    """
    Check if a mapped variant meets the basic requirements for annotation.

    This function validates that a mapped variant has the necessary data
    to proceed with annotation by checking for a valid score value.

    Args:
        mapped_variant (MappedVariant): The mapped variant to check for
            annotation eligibility.

    Returns:
        bool: True if the variant can be annotated (has score ranges and
            a non-None score), False otherwise.
    """
    # This property is guaranteed to exist for all variants.
    if mapped_variant.variant.data["score_data"]["score"] is None:  # type: ignore
        return False

    return True


def score_calibration_may_be_used_for_annotation(
    score_calibration: ScoreCalibration,
    annotation_type: Literal["pathogenicity", "functional"],
    allow_research_use_only_calibrations: bool = False,
) -> bool:
    """
    Check if a score calibration may be used for annotation based on its properties.

    This function evaluates whether a given score calibration is suitable for use in
    annotation by checking its research use only status and the presence of required
    classifications based on the annotation type.

    Args:
        score_calibration (ScoreCalibration): The score calibration to evaluate.
        annotation_type (Literal["pathogenicity", "functional"]): The type of annotation
            being considered, which determines the required classifications for validity.
        allow_research_use_only_calibrations (bool, optional): Whether to allow calibrations
            marked as research use only for annotation. Defaults to False.

    Returns:
        bool: True if the score calibration can be used for annotation, False otherwise.
    """
    if score_calibration.research_use_only and not allow_research_use_only_calibrations:
        return False

    if score_calibration.functional_classifications is None or len(score_calibration.functional_classifications) == 0:
        return False

    if annotation_type == "pathogenicity" and all(
        fr.acmg_classification is None for fr in score_calibration.functional_classifications
    ):
        return False

    return True


def _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
    mapped_variant: MappedVariant,
    annotation_type: Literal["pathogenicity", "functional"],
    allow_research_use_only_calibrations: bool = False,
) -> bool:
    """
    Check if a mapped variant's score set contains any of the required calibrations for annotation.

    Args:
        mapped_variant (MappedVariant): The mapped variant object containing the variant with score set data.
        annotation_type (Literal["pathogenicity", "functional"]): The type of annotation to check for.
            Must be either "pathogenicity" or "functional".
        allow_research_use_only_calibrations (bool, optional): Whether to consider calibrations marked as
            research use only as valid for annotation. Defaults to False.

    Returns:
        bool: True if the variant's score set contains at least one valid calibration with the required
            classifications for the specified annotation type. False otherwise.
    """
    if mapped_variant.variant.score_set.score_calibrations is None:
        return False

    return any(
        score_calibration_may_be_used_for_annotation(
            score_calibration,
            annotation_type,
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        )
        for score_calibration in mapped_variant.variant.score_set.score_calibrations
    )


def can_annotate_variant_for_pathogenicity_evidence(
    mapped_variant: MappedVariant, allow_research_use_only_calibrations=False
) -> bool:
    """
    Determine if a mapped variant can be annotated for pathogenicity evidence.

    This function checks if a variant meets all the necessary conditions to receive
    pathogenicity evidence annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for pathogenicity evidence annotation.

    Args:
        mapped_variant (MappedVariant): The mapped variant object to evaluate
            for pathogenicity evidence annotation eligibility.

    Returns:
        bool: True if the variant can be annotated for pathogenicity evidence,
            False otherwise.

    Notes:
        The function performs two main validation checks:
        1. Basic annotation assumptions via _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for pathogenicity evidence annotation.

        Both checks must pass for the variant to be considered eligible for
        pathogenicity evidence annotation.
    """
    if not _can_annotate_variant_base_assumptions(mapped_variant):
        return False
    if not _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
        mapped_variant, "pathogenicity", allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return False

    return True


def can_annotate_variant_for_functional_statement(
    mapped_variant: MappedVariant, allow_research_use_only_calibrations=False
) -> bool:
    """
    Determine if a mapped variant can be annotated for functional statements.

    This function checks if a variant meets all the necessary conditions to receive
    functional annotations by validating base assumptions and ensuring the variant's
    score calibrations contain the required kinds for functional annotation.

    Args:
        mapped_variant (MappedVariant): The variant object to check for annotation
            eligibility, containing mapping information and score data.

    Returns:
        bool: True if the variant can be annotated for functional statements,
            False otherwise.

    Notes:
        The function performs two main checks:
        1. Validates base assumptions using _can_annotate_variant_base_assumptions
        2. Verifies score calibrations have an appropriate calibration for functional annotation.
    """
    if not _can_annotate_variant_base_assumptions(mapped_variant):
        return False
    if not _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
        mapped_variant, "functional", allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return False

    return True


def sequence_feature_for_mapped_variant(mapped_variant: MappedVariant) -> SequenceFeature:
    """
    Extract the sequence feature (e.g., gene or transcript) associated with a mapped variant.

    This function retrieves the sequence feature from the mapped variant's data, which is
    necessary for generating annotations that reference specific genomic features.

    Args:
        mapped_variant (MappedVariant): The mapped variant object containing the variant data.

    Returns:
        SequenceFeature: Named tuple with:
            - `identifier`: sequence feature identifier (gene/transcript ID or name)
            - `system`: source/system URL for the identifier

    """
    target = target_for_variant(mapped_variant.variant)
    if target is None:
        raise MappingDataDoesntExistException(
            f"Variant {mapped_variant.variant.urn} does not have an identifiable target gene."
            " Unable to extract sequence feature for annotation."
        )

    # Prefer the mapped HGNC name if it's available, as this is more likely to be stable and recognizable than accessions or other identifiers.
    # If the mapped HGNC name is not available, fall back to extracting an identifier from the post-mapped metadata, which may be a gene or
    # transcript identifier of varying formats. If neither of those options are available, fall back to the target gene's name as listed in MaveDB.
    if target.mapped_hgnc_name:
        return SequenceFeature(target.mapped_hgnc_name, "https://www.genenames.org/")

    post_mapped_ids = extract_ids_from_post_mapped_metadata(
        target.post_mapped_metadata if target.post_mapped_metadata else {}  # type: ignore
    )
    if post_mapped_ids:
        post_mapped_id = post_mapped_ids[0]
        if post_mapped_id.startswith("ENSG") or post_mapped_id.startswith("ENST") or post_mapped_id.startswith("ENSP"):
            return SequenceFeature(post_mapped_id, "https://www.ensembl.org/index.html")
        elif post_mapped_id.startswith("NM_") or post_mapped_id.startswith("NR_") or post_mapped_id.startswith("NP_"):
            return SequenceFeature(post_mapped_id, "https://www.ncbi.nlm.nih.gov/refseq/")

        return SequenceFeature(post_mapped_id, "transcript or gene identifier of unknown source")

    if target.name:
        return SequenceFeature(target.name, "https://www.mavedb.org/")

    raise MappingDataDoesntExistException(
        f"Variant {mapped_variant.variant.urn} does not have an identifiable sequence feature in its target gene data."
        " Unable to extract sequence feature for annotation."
    )


def select_strongest_functional_calibration(
    mapped_variant: MappedVariant,
    calibrations: list[ScoreCalibration],
) -> tuple[Optional[ScoreCalibration], Optional[ScoreCalibrationFunctionalClassification]]:
    """
    Select the calibration with the strongest evidence for functional classification.

    In case of ties or conflicting classifications, defaults to normal classification.
    Returns the calibration and its functional classification range that contains the variant.

    If the variant is not contained in any range, returns (first_calibration, None) to indicate
    the variant should be classified as INDETERMINATE but still receive annotations.
    """
    if not calibrations:
        return None, None

    # Collect all calibrations and their classifications
    candidates: list[
        tuple[
            ScoreCalibration,
            ScoreCalibrationFunctionalClassification,
            ExperimentalVariantFunctionalImpactClassification,
        ]
    ] = []

    for calibration in calibrations:
        functional_range, classification = functional_classification_of_variant(mapped_variant, calibration)
        if functional_range is not None:
            candidates.append((calibration, functional_range, classification))

    # If variant is not in any range, return first calibration with None to indicate INDETERMINATE
    if not candidates:
        return calibrations[0], None

    # If only one candidate, return it
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    # Check if all classifications agree
    classifications = [c[2] for c in candidates]
    if all(cls == classifications[0] for cls in classifications):
        # All agree, return the first one
        return candidates[0][0], candidates[0][1]

    # Conflict exists: default to normal classification
    normal_candidates = [c for c in candidates if c[2] == ExperimentalVariantFunctionalImpactClassification.NORMAL]
    if normal_candidates:
        return normal_candidates[0][0], normal_candidates[0][1]

    # If no normal classification, return the first candidate
    return candidates[0][0], candidates[0][1]


def select_strongest_pathogenicity_calibration(
    mapped_variant: MappedVariant,
    calibrations: list[ScoreCalibration],
) -> tuple[Optional[ScoreCalibration], Optional[ScoreCalibrationFunctionalClassification]]:
    """
    Select the calibration with the strongest evidence for pathogenicity classification.

    Uses ACMG evidence strength to determine the strongest evidence.
    In case of ties with conflicting evidence (both benign and pathogenic), defaults to uncertain
    significance by returning None for the functional range.
    Returns the calibration and its functional classification range that contains the variant.

    If the variant is not contained in any range, returns (first_calibration, None) to indicate
    the variant should receive annotations even though it's not classified in any range.
    """
    if not calibrations:
        return None, None

    # Define evidence strength ordering (higher index = stronger evidence)
    # Note: VA-Spec StrengthOfEvidenceProvided doesn't have MODERATE_PLUS, only our MaveDB enum does.
    # The classification.py module maps MODERATE_PLUS to MODERATE when returning VA-Spec enum values.
    strength_order = {
        None: 0,
        VaSpecStrengthOfEvidenceProvided.SUPPORTING: 1,
        VaSpecStrengthOfEvidenceProvided.MODERATE: 2,
        VaSpecStrengthOfEvidenceProvided.STRONG: 3,
        VaSpecStrengthOfEvidenceProvided.VERY_STRONG: 4,
    }

    # Collect all calibrations with their evidence strength and criterion
    candidates: list[tuple[ScoreCalibration, ScoreCalibrationFunctionalClassification, int, bool]] = []

    for calibration in calibrations:
        functional_range, criterion, evidence_strength = pathogenicity_classification_of_variant(
            mapped_variant, calibration
        )
        if functional_range is not None:
            strength_value = strength_order.get(evidence_strength, 0)
            is_benign = criterion.name.startswith("B") if criterion else False
            candidates.append((calibration, functional_range, strength_value, is_benign))

    # If variant is not in any range, return first calibration with None
    if not candidates:
        return calibrations[0], None

    # If only one candidate, return it
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    # Find the maximum strength
    max_strength = max(c[2] for c in candidates)
    strongest_candidates = [c for c in candidates if c[2] == max_strength]

    # If only one with max strength, return it
    if len(strongest_candidates) == 1:
        return strongest_candidates[0][0], strongest_candidates[0][1]

    # Tie: check for conflicting evidence (both benign and pathogenic)
    has_benign = any(c[3] for c in strongest_candidates)
    has_pathogenic = any(not c[3] for c in strongest_candidates)

    # If there's a conflict between benign and pathogenic evidence of equal strength,
    # return None for the functional range to indicate uncertain significance
    if has_benign and has_pathogenic:
        return strongest_candidates[0][0], None

    # Otherwise return the first of the strongest
    return strongest_candidates[0][0], strongest_candidates[0][1]
