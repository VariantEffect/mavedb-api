import math
from random import choice
from typing import Optional, SupportsIndex, Union

from mavehgvs.variant import Variant

from mavedb.lib.validation.constants.conversion import aa_dict_key_1, codon_dict_DNA
from mavedb.lib.validation.constants.general import null_values_re


def is_null(value):
    """
    Returns True if a stripped/lowercase value in in `nan_col_values`.

    Parameters
    __________
    value : str
        The value to be checked as null or not.

    Returns
    _______
    bool
        True value is NoneType or if value matches the stated regex patterns in constants.null_values_re.
    """
    value = str(value).strip().lower()
    if not value:
        return True
    match = null_values_re.fullmatch(value)
    if match:
        return True
    else:
        return False
    # return null_values_re.fullmatch(value) or not value


def generate_hgvs(prefix: str = "c") -> str:
    """
    Generates a random hgvs string from a small sample.
    """
    if prefix == "p":
        # Subset of 3-letter codes, chosen at random.
        amino_acids = [
            "Ala",
            "Leu",
            "Gly",
            "Val",
            "Tyr",
            "Met",
            "Cys",
            "His",
            "Glu",
            "Phe",
        ]
        ref = choice(amino_acids)
        alt = choice(amino_acids)
        return f"{prefix}.{ref}{choice(range(1, 100))}{alt}"
    else:
        alt = choice("ATCG")
        ref = choice("ATCG")
        return f"{prefix}.{choice(range(1, 100))}{ref}>{alt}"


def construct_hgvs_pro(wt: str, mutant: str, position: int, target_seq: Optional[str] = None):
    # TODO: the testing on this function needs to be improved
    """
    Given the wt and mutant 3 lette amino acid codes as well as the position, this function generates a validated
    hgvs_pro string.

    Parameters
    __________
    wt: str
        The wt 3 letter amino acid code.
    mutant: str
        The mutant 3 letter amino acid code.
    position: int
        The position of the change.

    Returns
    _______
    hgvs
        The constructed hgvs_pro string.

    Raises
    ______
    ValueError
        If the wt or mutant 3 letter amino acid codes are invalid.
    """
    # TODO account for when variant codon is None, a deletion event
    # check that the provided 3 letter amino acid codes are valid
    if wt not in aa_dict_key_1.values():
        raise ValueError(
            "wt 3 letter amino acid code {} is invalid, " "must be one of the following: {}".format(
                wt, list(aa_dict_key_1.values())
            )
        )
    if mutant not in aa_dict_key_1.values():
        raise ValueError(
            "wt 3 letter amino acid code {} is invalid, " "must be one of the following: {}".format(
                mutant, list(aa_dict_key_1.values())
            )
        )

    if wt == mutant:
        hgvs = "p." + wt + str(position) + "="
    else:
        hgvs = "p." + wt + str(position) + mutant
    # validate variant
    Variant(hgvs)
    # var.validate_hgvs_string(value=hgvs, column="p", targetseq=target_seq)
    return hgvs


def convert_hgvs_nt_to_hgvs_pro(hgvs_nt: str, target_seq: str):
    # TODO note that this only works for codon changes and single mutants
    """
    This function takes a hgvs_nt variant string and its associated target sequence and returns
    a validated hgvs_pro equivalent.

    Parameters
    __________
    hgvs_nt: string
        The hgvs_nt string that will be converted.
    target_seq:
        The target sequence associated with the hgvs_nt variant.

    Raises
    ______
    TypeError
        If target_seq is not string.
    ValueError
        If target_seq is not made solely of characters ACTG.
    """
    # check that the hgvs_nt variant is valid with regards to the target sequence
    # validate_hgvs_string(value=hgvs_nt,
    #                     column="nt",
    #                     targetseq=target_seq)

    # check for TypeError
    # if target_seq is not string
    if not isinstance(target_seq, str):
        raise TypeError("target_seq must be string.")

    # check for ValueError
    # if target_seq is not made solely of characters ACTG
    check_chars = [letter in "ACTG" for letter in target_seq]
    if False in check_chars:
        raise ValueError("target_seq is invalid, must be composed only of bases ACTG.")

    # identify variant_position and get codon_number associated with it

    if _is_wild_type(hgvs_nt):  # variant_codon is wild-type
        codon_number = None
        target_codon = None
    else:  # any other variant change
        # instantiate Variant object
        variant = Variant(hgvs_nt)
        # get variant position and convert to int
        if type(variant.positions) is list:  # multiple positions values exist
            variant_position = int(str(variant.positions[0]))
        elif type(variant.positions) is tuple:
            variant_position = int(str(variant.positions[0]))
        else:  # only one value for positions
            variant_position = int(str(variant.positions))
        # now that we have the variant_position, get codon_number
        codon_number = round((variant_position / 3) + 0.5)
        # use codon_number to get target_codon from target_seq
        target_codon = target_seq[(codon_number - 1) * 3 : codon_number * 3]

    # declare variables for codon data
    # keep track of the number and location of the changes within the codon
    sub_one = None
    sub_two = None
    sub_three = None
    # keep tack of the number and value of the changes within the codon
    sub_one_nuc = None
    sub_two_nuc = None
    sub_three_nuc = None
    # keep track of the full codon changes
    variant_codon = None

    # determine sequence of variant_codon

    if _is_wild_type(hgvs_nt):  # variant_codon is wild-type
        variant_codon = target_codon
        sub_one = None  # no nucleotide substitutions
    elif _is_deletion(hgvs_nt):  # target_codon was deleted
        variant_codon = None
        sub_one = None  # no nucleotide substitutions
    elif _is_substitution_one_base(hgvs_nt):  # variant_codon has one nucleotide substitution
        assert isinstance(variant.sequence, SupportsIndex)
        # instantiate Variant object
        variant = Variant(hgvs_nt)
        # get index of nucleotide substitution
        sub_one = int(str(variant.positions)) % 3 - 1
        # get nucleotide of substitution
        sub_one_nuc = variant.sequence[1]
        # set other possible indices for codon substitution to None
        sub_two = None
        sub_three = None
    elif _is_substitution_two_bases_nonadjacent(hgvs_nt):  # variant has two nucleotide substitutions, non-adjacent
        # instantiate Variant object
        variant = Variant(hgvs_nt)
        assert isinstance(variant.sequence, SupportsIndex)
        assert isinstance(variant.positions, SupportsIndex)
        # get indices of nucleotide substitutions
        sub_one = int(str(variant.positions[0])) % 3 - 1
        sub_two = int(str(variant.positions[1])) % 3 - 1
        # get nucleotides of substitutions
        sub_one_nuc = variant.sequence[0][1]
        sub_two_nuc = variant.sequence[1][1]
        # set other possible indices for codon substitution to None
        sub_three = None
    else:  # variant_codon has two or three adjacent nucleotide substitutions
        # instantiate Variant object
        variant = Variant(hgvs_nt)
        assert isinstance(variant.sequence, str)
        assert isinstance(variant.positions, SupportsIndex)
        variant_codon = variant.sequence
        # get index of first codon substitution
        sub_one = int(str(variant.positions[0])) % 3 - 1
        # get string of substituted nucleotides
        sub_nucs = variant.sequence
        if len(sub_nucs) == 2:  # variant codon has two adjacent nucleotide substitutions
            # assign additional nucleotide substitution indices
            sub_two = sub_one + 1
            # get nucleotides of substitutions
            sub_one_nuc = sub_nucs[0]
            sub_two_nuc = sub_nucs[1]
            # set other possible indices for codon substitution to None
            sub_three = None
        else:  # variant has three adjacent nucleotide substitutions
            # assign additional nucleotide substitution indices
            sub_two = sub_one + 1
            sub_three = sub_two + 1
            # get nucleotides of substitutions
            sub_one_nuc = sub_nucs[0]
            sub_two_nuc = sub_nucs[1]
            sub_three_nuc = sub_nucs[2]

    # using data generated above (substituted nucleotides and indices in codon), construct variant_codon

    # only assign variant_codon if nucleotide substitution occurred
    if sub_one is not None:
        # declare and initialize variant_codon
        variant_codon = ""
        # set first nucleotide of variant_codon
        if sub_one == 0:
            variant_codon = variant_codon + sub_one_nuc
        else:
            variant_codon = variant_codon + target_codon[0]
        # set second nucleotide of variant_codon
        if sub_one == 1:
            variant_codon = variant_codon + sub_one_nuc
        elif sub_two == 1:
            variant_codon = variant_codon + sub_two_nuc
        else:
            variant_codon = variant_codon + target_codon[1]
        # set third nucleotide of variant_codon
        if sub_one == -1 or sub_one == 2:
            variant_codon = variant_codon + sub_one_nuc
        elif sub_two == -1 or sub_two == 2:
            variant_codon = variant_codon + sub_two_nuc
        elif sub_three == -1 or sub_three == 2:
            variant_codon = variant_codon + sub_three_nuc
        else:
            variant_codon = variant_codon + target_codon[2]

    # convert to 3 letter amino acid code
    assert target_codon is not None
    target_aa = codon_dict_DNA[target_codon]
    if not variant_codon:
        return "p.="

    variant_aa = codon_dict_DNA[variant_codon]

    assert codon_number is not None
    return construct_hgvs_pro(wt=target_aa, mutant=variant_aa, position=codon_number, target_seq=target_seq)


def inf_or_float(v: Optional[Union[float, int, str]], lower: bool) -> float:
    """
    This function takes an optional float and either converts the passed nonetype
    object to the appropriate infinity value (based on lower) or returns the float
    directly.

    Parameters
    ----------
    v : float or None
        an optional floating point value
    lower : bool
        whether the value is a lower bound

    Returns
    -------
    v : float
        Infinity or -Infinity if the initially passed v was None. v otherwise.
    """
    if v is None:
        if lower:
            return -math.inf
        else:
            return math.inf
    else:
        return float(v)


def _is_wild_type(hgvs: str):
    # TODO this is no longer valid
    """
    This function takes an hgvs formatted string and returns True if the hgvs string indicates
    there was no change from the target sequence.

    Parameters
    ----------
    hgvs : string
        hgvs formatted string

    Returns
    -------
    wt : bool
        True if hgvs string indicates wild type
    """
    wt = False
    if hgvs.startswith("_wt"):
        wt = True
    return wt


def _is_deletion(hgvs: str):
    """
    This function takes an hgvs formatted string and returns True if the hgvs string indicates
    there was a deletion.

    Parameters
    ----------
    hgvs : string
        hgvs formatted string

    Returns
    -------
    deletion : bool
        True if hgvs string is indicates a deletion
    """
    deletion = False
    if hgvs.endswith("del"):
        deletion = True
    return deletion


def _is_substitution_one_base(hgvs: str):
    """
    This function takes an hgvs formatted string and returns True if the hgvs string indicates
    there was a substitution at one base of the codon.

    Parameters
    ----------
    hgvs : string
        hgvs formatted string

    Returns
    -------
    sub_one : bool
        True if hgvs string is indicates a substitution at one base of codon
    """
    sub_one = False
    if hgvs[-2] == ">":
        sub_one = True
    return sub_one


def _is_substitution_two_bases_nonadjacent(hgvs: str):
    """
    This function takes an hgvs formatted string and returns True if the hgvs string indicates
    there were substitutions (non-adjacent) in the codon.

    Parameters
    ----------
    hgvs : string
        hgvs formatted string

    Returns
    -------
    sub_two : bool
        True if hgvs string is indicates a substitution at one base of codon
    """
    sub_two = False
    if hgvs[-1] == "]":
        sub_two = True
    return sub_two
