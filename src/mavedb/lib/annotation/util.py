from ga4gh.core.models import Extension
from ga4gh.vrs.models import (
    MolecularVariation,
    Allele,
    CisPhasedBlock,
    SequenceLocation,
    SequenceReference,
    Expression,
    LiteralSequenceExpression,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException


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
            CisPhasedBlock(
                members=[allele_from_mapped_variant_dictionary_result(member) for member in mapping_results["members"]]
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
