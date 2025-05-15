import logging
import warnings
from typing import Hashable, Optional, TYPE_CHECKING

import pandas as pd
from fqfa.validator import dna_bases_validator
from mavehgvs.exceptions import MaveHgvsParseError
from mavehgvs.variant import Variant

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.dataframe.column import (
    generate_variant_prefixes,
    validate_variant_column,
    validate_variant_formatting,
    validate_hgvs_column_properties,
    construct_target_sequence_mappings,
)
from mavedb.lib.validation.constants.target import strict_valid_sequence_types as valid_sequence_types
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.target_accession import TargetAccession

if TYPE_CHECKING:
    from cdot.hgvs.dataproviders import RESTDataProvider
    from hgvs.parser import Parser
    from hgvs.validator import Validator


logger = logging.getLogger(__name__)


def validate_hgvs_transgenic_column(column: pd.Series, is_index: bool, targets: dict[str, TargetSequence]) -> None:
    """
    Validate the variants in an HGVS column from a dataframe.

    Tests whether the column has a correct and consistent prefix.
    This function also validates all individual variants in the column and checks for agreement against the target
    sequence (for non-splice variants).

    Implementation NOTE: We assume variants will only be presented as fully qualified (accession:variant)
    if this column is being validated against multiple targets.

    Parameters
    ----------
    column : pd.Series
        The column from the dataframe to validate
    is_index : bool
        True if this is the index column for the dataframe and therefore cannot have missing values; else False
    targets : dict
        Dictionary containing a mapping of target gene names to their sequences.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the target sequence does is not dna or protein (or inferred as dna or protein)
    ValueError
        If the target sequence is not valid for the variants (e.g. protein sequence for nucleotide variants)
    ValidationError
        If one of the variants fails validation
    """
    validate_variant_column(column, is_index)
    validate_variant_formatting(
        column=column,
        prefixes=generate_variant_prefixes(column),
        targets=list(targets.keys()),
        fully_qualified=len(targets) > 1,
    )

    observed_sequence_types = validate_observed_sequence_types(targets)
    validate_hgvs_column_properties(column, observed_sequence_types)
    target_seqs = construct_target_sequence_mappings(column, targets)

    parsed_variants = [
        validate_transgenic_variant(idx, variant, target_seqs, len(targets) > 1) for idx, variant in column.items()
    ]

    # format and raise an error message that contains all invalid variants
    if any(not valid for valid, _ in parsed_variants):
        invalid_variants = [variant for valid, variant in parsed_variants if not valid]
        raise ValidationError(
            f"encountered {len(invalid_variants)} invalid variant strings.", triggers=invalid_variants
        )

    return


def validate_hgvs_genomic_column(
    column: pd.Series, is_index: bool, targets: list[TargetAccession], hdp: Optional["RESTDataProvider"]
) -> None:
    """
    Validate the variants in an HGVS column from a dataframe.

    Tests whether the column has a correct and consistent prefix.
    This function also validates all individual variants in the column and checks for agreement against the target
    sequence (for non-splice variants).

    Parameters
    ----------
    column : pd.Series
        The column from the dataframe to validate
    is_index : bool
        True if this is the index column for the dataframe and therefore cannot have missing values; else False
    targets : list
        Dictionary containing a list of target accessions.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the target sequence does is not dna or protein (or inferred as dna or protein)
    ValueError
        If the target sequence is not valid for the variants (e.g. protein sequence for nucleotide variants)
    ValidationError
        If one of the variants fails validation
    """
    target_accession_identifiers = [target.accession for target in targets if target.accession is not None]
    validate_variant_column(column, is_index)
    validate_variant_formatting(
        column=column,
        prefixes=generate_variant_prefixes(column),
        targets=target_accession_identifiers,
        fully_qualified=True,
    )

    # Attempt to import dependencies from the hgvs package.
    #
    # For interoperability with Mavetools, we'd prefer if users were not required to install `hgvs`, which requires postgresql and psycopg2 as
    # dependencies. We resolve these dependencies only when necessary, treating them as semi-optional. For the purposes of this package, if the
    # hdp parameter is ever omitted it will be inferred so long as the `hgvs` package is installed and available. For the purposes of validator
    # packages such as Mavetools, users may omit the hdp parameter and proceed with non-strict validation which will log a warning. To silence
    # the warning, users should install `hgvs` and pass a data provider to this function. -capodb 2025-02-26
    try:
        import hgvs.parser
        import hgvs.validator

        if hdp is None:
            import mavedb.deps

            hdp = mavedb.deps.hgvs_data_provider()

        hp = hgvs.parser.Parser()
        vr = hgvs.validator.Validator(hdp=hdp)

    except ModuleNotFoundError as err:
        if hdp is not None:
            logger.error(
                f"Failed to import `hgvs` from a context in which it is required. A data provider ({hdp.data_version()}) is available to this function, so "
                + "it is inferred that strict validation is desired. Strict validation requires the `hgvs` package for parsing and validation of HGVS strings with "
                + "accession information. Please ensure the `hgvs` package is installed (https://github.com/biocommons/hgvs/?tab=readme-ov-file#installing-hgvs-locally) "
                + "to silence this error."
            )
            raise err

        warnings.warn(
            "Failed to import `hgvs`, and no data provider is available. Skipping strict validation of HGVS genomic variants. HGVS variant strings "
            + "will be validated for format only, and accession information will be ignored and assumed correct. To enable strict validation against provided accessions and "
            + "silence this warning, install the `hgvs` package. See: https://github.com/biocommons/hgvs/?tab=readme-ov-file#installing-hgvs-locally."
        )

        hp, vr = None, None

    if hp is not None and vr is not None:
        parsed_variants = [validate_genomic_variant(idx, variant, hp, vr) for idx, variant in column.items()]
    else:
        parsed_variants = [
            validate_transgenic_variant(
                idx,
                variant,
                {target: None for target in target_accession_identifiers},
                len(target_accession_identifiers) > 1,
            )
            for idx, variant in column.items()
        ]

    # format and raise an error message that contains all invalid variants
    if any(not valid for valid, _ in parsed_variants):
        invalid_variants = [variant for valid, variant in parsed_variants if not valid]
        raise ValidationError(
            f"encountered {len(invalid_variants)} invalid variant strings.", triggers=invalid_variants
        )

    return


def validate_genomic_variant(
    idx: Hashable, variant_string: str, parser: "Parser", validator: "Validator"
) -> tuple[bool, Optional[str]]:
    def _validate_allelic_variation(variant: Variant) -> bool:
        """
        The HGVS package is currently unable to parse allelic variation, and this doesn't seem like a planned
        feature (see: https://github.com/biocommons/hgvs/issues/538). As a workaround and because MaveHGVS
        does support this sort of multivariant we can:
        - Validate that the multi-variant allele is valid HGVS.
        - Validate each sub-variant in an allele is valid with respect to the transcript.

        Parameters
        ----------
        variant : MaveHGVS Style Variant
            The multi-variant allele to validate.

        Returns
        -------
        bool
            True if the allele is valid.

        Raises
        ------
        MaveHgvsParseError
            If the variant is not a valid HGVS string (for reasons of syntax).
        hgvs.exceptions.HGVSError
            If the variant is not a valid HGVS string (for reasons of transcript/variant inconsistency).
        """

        for variant_sub_string in variant.components(): # type: ignore
            validator.validate(parser.parse(variant_sub_string), strict=False)

        return True

    # Not pretty, but if we make it here we're guaranteed to have hgvs installed as a package, and we
    # should make use of the built in exception they provide for variant validation.
    import hgvs.exceptions

    if not variant_string:
        return True, None

    for variant in variant_string.split(" "):
        try:
            variant_obj = Variant(variant)
            if variant_obj.is_multi_variant():
                _validate_allelic_variation(variant_obj)
            else:
                validator.validate(parser.parse(str(variant_obj)), strict=False)
        except MaveHgvsParseError as e:
            logger.error("err", exc_info=e)
            return False, f"Failed to parse variant string '{variant}' at row {idx}."
        except hgvs.exceptions.HGVSError as e:
            return False, f"Failed to parse row {idx} with HGVS exception: {e}."

    return True, None


def validate_transgenic_variant(
    idx: Hashable, variant_string: str, target_sequences: dict[str, Optional[str]], is_fully_qualified: bool
) -> tuple[bool, Optional[str]]:
    if not variant_string:
        return True, None

    # variants can exist on the same line separated by a space
    for variant in variant_string.split(" "):
        if is_fully_qualified:
            name, variant = str(variant).split(":")
        else:
            name = list(target_sequences.keys())[0]

        if variant is not None:
            try:
                Variant(variant, targetseq=target_sequences[name])
            except MaveHgvsParseError:
                try:
                    Variant(variant)  # note this will get called a second time for splice variants
                except MaveHgvsParseError:
                    return False, f"invalid variant string '{variant}' at row {idx} for sequence {name}"
                else:
                    return False, f"target sequence mismatch for '{variant}' at row {idx} for sequence {name}"

    return True, None


def validate_guide_sequence_column(column: pd.Series, is_index: bool) -> None:
    validate_variant_column(column, is_index)
    if column.apply(lambda x: dna_bases_validator(x) is None if x is not None else False).any():
        raise ValidationError("Invalid guide sequence provided: all guide sequences must be valid DNA sequences.")


def validate_observed_sequence_types(targets: dict[str, TargetSequence]) -> list[str]:
    """
    Ensures that the sequence types of the given target sequences are an accepted type.

    Parameters
    ----------
    targets : (dict[str, TargetSequence])
        A dictionary where the keys are target names and the values are TargetSequence objects.

    Returns
    -------
        list[str]: A list of observed sequence types from the target sequences.

    Raises
    ------
    ValueError
        If no targets are provided.
    ValueError
        If any of the target sequences have an invalid sequence type.
    """
    if not targets:
        raise ValueError("No targets were provided; cannot validate observed sequence types with none observed.")

    observed_sequence_types = [target.sequence_type for target in targets.values() if target.sequence_type is not None]
    invalid_sequence_types = set(observed_sequence_types) - set(valid_sequence_types)
    if invalid_sequence_types:
        raise ValueError(
            f"Some targets are invalid sequence types: {invalid_sequence_types}. Sequence types shoud be one of: {valid_sequence_types}"
        )

    return observed_sequence_types


def validate_hgvs_prefix_combinations(
    hgvs_nt: Optional[str], hgvs_splice: Optional[str], hgvs_pro: Optional[str], transgenic: bool
) -> None:
    """
    Validate the combination of HGVS variant prefixes.

    This function assumes that other validation, such as checking that all variants in the column have the same prefix,
    has already been performed.

    Parameters
    ----------
    hgvs_nt : Optional[str]
        The first character (prefix) of the HGVS nucleotide variant strings, or None if not used.
    hgvs_splice : Optional[str]
        The first character (prefix) of the HGVS splice variant strings, or None if not used.
    hgvs_pro : Optional[str]
        The first character (prefix) of the HGVS protein variant strings, or None if not used.
    transgenic : bool
        Whether we should validate these prefix combinations as transgenic variants

    Returns
    -------
    None

    Raises
    ------
    ValueErrorz
        If upstream validation failed and an invalid prefix string was passed to this function
    ValidationError
        If the combination of prefixes is not valid
    """
    # ensure that the prefixes are valid - this validation should have been performed before this function was called
    if hgvs_nt not in list("cngmo") + [None]:
        raise ValueError("invalid nucleotide prefix")
    if hgvs_splice not in list("cn") + [None]:
        raise ValueError("invalid nucleotide prefix")
    if hgvs_pro not in ["p", None]:
        raise ValueError("invalid protein prefix")

    # test agreement of prefixes across columns
    if hgvs_splice is not None:
        if hgvs_nt not in list("gmo"):
            raise ValidationError("nucleotide variants must use valid genomic prefix when splice variants are present")
        if hgvs_pro is not None:
            if hgvs_splice != "c":
                raise ValidationError("splice variants' must use 'c.' prefix when protein variants are present")
        else:
            if hgvs_splice != "n":
                raise ValidationError("splice variants must use 'n.' prefix when protein variants are not present")
    elif hgvs_pro is not None and hgvs_nt is not None:
        if hgvs_nt != "c":
            raise ValidationError(
                "nucleotide variants must use 'c.' prefix when protein variants are present and splicing variants are"
                " not present"
            )
    # Only raise if this data will not be validated by biocommons.hgvs
    elif hgvs_nt is not None:  # just hgvs_nt
        if hgvs_nt != "n" and transgenic:
            raise ValidationError("nucleotide variants must use 'n.' prefix when only nucleotide variants are defined")
