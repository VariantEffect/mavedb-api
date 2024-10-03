from typing import Optional

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null


# TODO: value will not be Optional when we confirm the final controlled keyword list.
def validate_description(value: str, key: str, description: Optional[str]):
    if value.lower() == "other" and (description is None or description.strip() == ""):
        raise ValidationError(
            "Other option does not allow empty description.", custom_loc=["body", "keywordDescriptions", key]
        )


def validate_duplicates(keywords: list):
    keys = []
    values = []
    for k in keywords:
        keys.append(k.keyword.key.lower())  # k: ExperimentControlledKeywordCreate object
        if k.keyword.value.lower() != "other":
            values.append(k.keyword.value.lower())

    keys_set = set(keys)
    values_set = set(values)

    if len(keys) != len(keys_set):
        raise ValidationError("Duplicate keys found in keywords.")
    if len(values) != len(values_set):
        raise ValidationError("Duplicate values found in keywords.")


def validate_keyword(keyword: str):
    """
    Validates a keyword.

    Parameters
    __________
    keyword: str

    Raises
    ______
    ValidationError
        If keyword is invalid or null.
    """
    if is_null(keyword) or not isinstance(keyword, str):
        raise ValidationError("{} are not valid keyword. Keyword must be a non null list of strings.".format(keyword))


def validate_keyword_keys(keywords: list):
    keyword_dict = {k.keyword.key.lower(): k.keyword.value.lower() for k in keywords}
    variant_library_method = keyword_dict.get("variant library creation method", "")

    if variant_library_method == "endogenous locus library method":
        if not (
            "endogenous locus library method system" in keyword_dict
            and "endogenous locus library method mechanism" in keyword_dict
        ):
            raise ValidationError(
                "If 'Variant Library Creation Method' is 'Endogenous locus library method', "
                "both 'Endogenous Locus Library Method System' and 'Endogenous Locus Library Method Mechanism' "
                "must be present."
            )

    elif variant_library_method == "in vitro construct library method":
        if not (
            "in vitro construct library method system" in keyword_dict
            and "in vitro construct library method mechanism" in keyword_dict
        ):
            raise ValidationError(
                "If 'Variant Library Creation Method' is 'In vitro construct library method', "
                "both 'In Vitro Construct Library Method System' and 'In Vitro Construct Library Method Mechanism' "
                "must be present."
            )

    elif variant_library_method == "other":
        if any(
            k in keyword_dict
            for k in [
                "endogenous locus library method system",
                "endogenous locus library method mechanism",
                "in vitro construct library method system",
                "in vitro construct library method mechanism",
            ]
        ):
            raise ValidationError(
                "If 'Variant Library Creation Method' is 'Other', none of "
                "'Endogenous Locus Library Method System', 'Endogenous Locus Library Method Mechanism', "
                "'In Vitro Construct Library Method System', or 'In Vitro Construct Library Method Mechanism' "
                "should be present."
            )


def validate_keyword_list(keywords: list):
    validate_duplicates(keywords)
    validate_keyword_keys(keywords)
