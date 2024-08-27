import logging


def canonical_only(record: logging.LogRecord):
    return record.__dict__.get("canonical", False)
