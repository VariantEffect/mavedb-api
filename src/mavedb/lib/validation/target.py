from fqfa import infer_sequence_type
from fqfa.validator import amino_acids_validator, dna_bases_validator

from mavedb.lib.validation.constants.target import valid_sequence_types
from mavedb.lib.validation.exceptions import ValidationError


def validate_sequence_category(sequence_type: str):
    """
    If the sequence type provided does not fall within a pre-defined list of valid sequence types.

    Parameters
    __________
    sequence_type: str
        The sequence type to be validated.

    Raises
    ______
    ValidationError
        If the sequence type provided is not valid.
    """
    if sequence_type not in valid_sequence_types:
        raise ValidationError(f"'{sequence_type}' is not a valid sequence type")


def validate_target_sequence(target_seq: str, target_seq_type: str):
    """
    Validates a target sequence whether match sequence_type.

    Parameters
    __________
    sequence : str
        The target sequence that will be validated.

    Raises
    ______
    ValidationError
        If the target sequence does not consist of ACTG or Amino acid.
    """
    if target_seq_type == "infer":
        if infer_sequence_type(target_seq) not in valid_sequence_types:
            raise ValidationError(f"invalid inferred sequence type '{infer_sequence_type(target_seq)}'")
    elif target_seq_type == "dna":
        if dna_bases_validator(target_seq) is None:
            raise ValidationError("invalid dna sequence provided")
    elif target_seq_type == "protein":
        if amino_acids_validator(target_seq) is None:
            raise ValidationError("invalid protein sequence provided")
    else:
        raise ValueError(f"unexpected sequence type '{target_seq_type}'")
