import re


def sanitize_string(s: str):
    """
    Sanitize a string to a consistent format:
    - Strip leading and trailing whitespace
    - Convert to lowercase
    - Replace internal whitespace with underscores
    """
    return re.sub(r"\s+", "_", s.strip().lower())
