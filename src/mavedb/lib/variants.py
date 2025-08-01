from mavedb.models.mapped_variant import MappedVariant


def hgvs_from_vrs_allele(allele: dict) -> str:
    """
    Extract the HGVS notation from the VRS allele.
    """
    try:
        # VRS 2.X
        return allele["expressions"][0]["value"]
    except KeyError:
        # VRS 1.X
        return allele["variation"]["expressions"][0]["value"]


def hgvs_from_mapped_variant(mapped_variant: MappedVariant) -> list[str]:
    """
    Extract the HGVS notation from the post_mapped field of the MappedVariant object.
    """
    post_mapped_object: dict = mapped_variant.post_mapped  # type: ignore

    if not post_mapped_object:
        return []

    if post_mapped_object["type"] == "Haplotype":  # type: ignore
        return [hgvs_from_vrs_allele(allele) for allele in post_mapped_object["members"]]
    elif post_mapped_object["type"] == "CisPhasedBlock":  # type: ignore
        return [hgvs_from_vrs_allele(allele) for allele in post_mapped_object["members"]]
    elif post_mapped_object["type"] == "Allele":  # type: ignore
        return [hgvs_from_vrs_allele(post_mapped_object)]
    else:
        raise ValueError(f"Unsupported post_mapped type: {post_mapped_object['type']}")
