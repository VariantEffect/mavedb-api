from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.target import valid_categories, valid_sequence_types
from fqfa import infer_sequence_type


def validate_target_category(category: str):
    """
    If the target category provided does not fall within a pre-defined list of valid categories.

    Parameters
    __________
    category: str
        The target category to be validated.

    Raises
    ______
    ValidationError
        If the target category provided is not valid.
    """
    if category not in valid_categories:
        raise ValidationError("{} is not a valid target category. Valid categories are "
                              "Protein coding, Regulatory, and Other noncoding".format(category))


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
        raise ValidationError("{} is not a valid sequence type. Valid sequence types are "
                              "Infer, DNA, and Protein".format(sequence_type))


def validate_target_sequence(sequence_type: str, target_seq: str):
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
    # Get target sequence type
    target_seq_type = infer_sequence_type(target_seq)
    if target_seq_type is None:
        raise ValidationError("sequence is invalid. It is not a correct target sequence.")
    elif target_seq_type != sequence_type:
        raise ValidationError("sequence type does not match sequence_type. "
                              "sequence type is {}, while sequence_type is {}.".format(target_seq_type, sequence_type))