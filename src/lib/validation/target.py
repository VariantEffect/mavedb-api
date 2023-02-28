from src.lib.validation.exceptions import ValidationError
from src.lib.validation.constants.target import valid_categories, valid_sequence_types


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
        raise ValidationError("{}'s is not a valid target category. Valid categories are "
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
        raise ValidationError("{}'s is not a valid sequence type. Valid sequence types are "
                              "Infer, DNA, and Protein".format(sequence_type))


def validate_target_sequence(target_seq: str):
    """
    Validates a target sequence. The sequence should consists of only ACTG and the length should be a multiple of 3.

    Parameters
    __________
    sequence : str
        The target sequence that will be validated.

    Raises
    ______
    ValidationError
        If the target sequence does not consist of ACTG or if the length of the sequence is not a multiple of 3.
    """
    # if target_seq is not made solely of characters ACTG
    check_chars = [letter in "ACTG" for letter in target_seq]
    if False in check_chars:
        raise ValidationError("target_seq is invalid, must be composed only of bases ACTG.")
    if len(target_seq) % 3 != 0:
        raise ValidationError("target_seq is invalid, length must be a multiple of three.")