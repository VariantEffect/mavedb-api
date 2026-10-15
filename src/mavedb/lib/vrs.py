"""Deserialization of stored ``post_mapped`` JSONB into GA4GH VRS objects.

The mapping pipeline writes each allele's mapped representation as a VRS-shaped dict in
``Allele.post_mapped``. These helpers rehydrate that dict into strict ``ga4gh.vrs`` Pydantic models for the
Cat-VRS transit, the variant-detail envelope, and the VA-Spec annotation subjects.
"""

from ga4gh.core.models import Extension
from ga4gh.vrs.models import (
    Allele,
    CisPhasedBlock,
    Expression,
    LengthExpression,
    LiteralSequenceExpression,
    MolecularVariation,
    ReferenceLengthExpression,
    SequenceLocation,
    SequenceReference,
)


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

    state_dict = variation["state"]
    state: ReferenceLengthExpression | LengthExpression | LiteralSequenceExpression
    if state_dict.get("type") == "ReferenceLengthExpression":
        state = ReferenceLengthExpression(**state_dict)
    elif state_dict.get("type") == "LengthExpression":
        state = LengthExpression(**state_dict)
    elif state_dict.get("type") == "LiteralSequenceExpression":
        state = LiteralSequenceExpression(**state_dict)
    else:
        raise ValueError(
            f"Unsupported VRS Allele state type {state_dict.get('type')!r}. "
            "Update allele_from_mapped_variant_dictionary_result to handle this type."
        )

    # Mapping results were not guaranteed to be generated on this version of VRS.
    # Explicit field extraction for alleles is intentional: stored dicts may contain extra fields (e.g. "type" on
    # Extension, "label" on SequenceReference) that the strict VRS Pydantic models forbid. In such cases,
    # using model_validate() directly is not possible.
    return Allele(
        id=variation.get("id"),
        state=state,
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
    Extracts a VRS (Variation Representation Specification) object from a stored ``post_mapped`` dict.

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
