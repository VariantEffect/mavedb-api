import logging
import re
import string
from typing import Optional
from uuid import uuid4

from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb.lib.validation.urn_re import (
    MAVEDB_EXPERIMENT_SET_URN_DIGITS,
    MAVEDB_URN_NAMESPACE,
)
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet

logger = logging.getLogger(__name__)


def generate_experiment_set_urn(db: Session):
    """
    Generate a new URN for an experiment set.

    Experiment set URNs include an 8-digit, zero-padded, sequentially-assigned numeric part. This function finds the
    maximum value in the database and adds one to form the new URN. To ensure atomicity, it should be called in the
    context of a database transaction.

    :param db: An active database session
    :return: The next available experiment set URN
    """

    # TODO We can't use func.max if an experiment set URN's numeric part will ever have anything other than 8 digits,
    # because we rely on the order guaranteed by zero-padding. This assumption is valid until we have 99999999
    # experiment sets.
    row = db.query(func.max(ExperimentSet.urn)).filter(ExperimentSet.urn.op("~")("^urn:mavedb:[0-9]+$")).one_or_none()
    max_urn_number = 0
    if row and row[0]:
        max_urn = row[0]
        match = re.search("^urn:mavedb:([0-9]+)$", max_urn)
        assert match is not None
        max_urn_number = int(match.groups(1)[0])
    next_urn_number = max_urn_number + 1
    return f"urn:mavedb:{next_urn_number:08}"


def generate_experiment_urn(db: Session, experiment_set: ExperimentSet, experiment_is_meta_analysis: bool):
    """
    Generate a new URN for an experiment.

    Experiment URNs include a two sequentially-assigned parts: a numeric part from the parent experiment set and a
    lowercase alphabetic part identifying the experiment within its set. The alphabetic part is assigned as follows:
    ```
    a, b, ..., z, aa, ab, ..., az, ba, ... bz, ... zz, aaa, ..., zzz, aaaa, ...
    ```
    This function looks at the database records for other experiments in the set and finds the maximum value of the
    alphabetic part, then increments it to form the new URN. To ensure atomicity, it should be called in the context of
    a database transaction to ensure atomicity.

    For meta-analyses, the suffix is always 0. There can only be one meta-analysis per experiment set.

    :param db: An active database session
    :param experiment_set: The experiment set to which this experiment belongs
    :param experiment_is_meta_analysis: Whether the experiment is a meta-analysis
    :return: The next available experiment URN
    """

    experiment_set_urn = experiment_set.urn
    assert experiment_set_urn is not None

    if experiment_is_meta_analysis:
        # Do not increment for meta-analysis, since this is a singleton
        next_suffix = "0"
    else:
        published_experiments_query = (
            db.query(Experiment)
            .filter(Experiment.experiment_set_id == experiment_set.id)
            .filter(Experiment.urn.op("~")(f"^{re.escape(experiment_set_urn)}-[a-z]+$"))
        )
        max_suffix = None
        for experiment in published_experiments_query:
            assert experiment.urn is not None
            match = re.search(f"^{re.escape(experiment_set_urn)}-([a-z]+)$", experiment.urn)
            if match is not None:
                suffix = match.group(1)
                if suffix and (max_suffix is None or len(max_suffix) < len(suffix) or max_suffix < suffix):
                    max_suffix = suffix
        if max_suffix is None:
            next_suffix = "a"
        else:
            max_suffix_number = 0
            while len(max_suffix) > 0:
                max_suffix_number *= 26
                max_suffix_number += string.ascii_lowercase.index(max_suffix[0]) + 1
                max_suffix = max_suffix[1:]
            next_suffix_number = max_suffix_number + 1
            next_suffix = ""
            x = next_suffix_number
            while x > 0:
                x, y = divmod(x - 1, len(string.ascii_lowercase))
                next_suffix = f"{string.ascii_lowercase[y]}{next_suffix}"
    return f"{experiment_set_urn}-{next_suffix}"


def generate_score_set_urn(db: Session, experiment: Experiment):
    """
    Generate a new URN for a score set.

    Score set URNs append a sequentially-assigned numeric part to their parent experiment URNs. This numeric part is not
    zero-padded to a fixed width.

    This function looks at the database records for other scoresets belonging to the experiment and finds the maximum
    value of the numeric part, then increments it to form the new URN. To ensure atomicity, it should be called in the
    context of a database transaction.

    :param db: An active database session
    :param experiment: The experiment to which this score set belongs
    :return: The next available score set URN
    """

    experiment_urn = experiment.urn
    assert experiment_urn is not None

    published_score_sets_query = (
        db.query(ScoreSet)
        .filter(ScoreSet.experiment_id == experiment.id)
        .filter(ScoreSet.urn.op("~")(f"^{re.escape(experiment_urn)}-[0-9]+$"))
    )
    max_suffix_number = 0
    for score_set in published_score_sets_query:
        assert score_set.urn is not None
        match = re.search(f"^{re.escape(experiment_urn)}-([0-9]+)$", score_set.urn)
        if match is not None:
            suffix_number = int(match.group(1))
            if suffix_number > max_suffix_number:
                max_suffix_number = suffix_number
    next_suffix_number = max_suffix_number + 1
    return f"{experiment_urn}-{next_suffix_number}"


def generate_collection_urn():
    """
    Generate a new URN for a collection.

    Collection URNs include a 16-digit UUID.

    :return: A new collection URN
    """
    return f"urn:mavedb:collection-{uuid4()}"


def generate_calibration_urn():
    """
    Generate a new URN for a calibration.

    Calibration URNs include a 16-digit UUID.

    :return: A new calibration URN
    """
    return f"urn:mavedb:calibration-{uuid4()}"


def generate_pipeline_urn():
    """
    Generate a new URN for a pipeline.

    Pipeline URNs include a 16-digit UUID.

    :return: A new pipeline URN
    """
    return f"urn:mavedb:pipeline-{uuid4()}"


def generate_job_run_urn():
    """
    Generate a new URN for a job run.

    Job run URNs include a 16-digit UUID.

    :return: A new job run URN
    """
    return f"urn:mavedb:job-{uuid4()}"


# MaveDB URNs do not sort into assignment order as strings: score-set and variant suffixes are unpadded
# (`-a-10` < `-a-2`, `#10` < `#2`) and experiment suffixes run a..z then aa..az (`aa` < `b`). Only the
# experiment-set digits are padded, which is why a lexical sort looks right until double digits. The keys
# below are the read side of the rule `generate_experiment_urn` already applies when assigning.


_SCORE_SET_URN_PARTS_RE = re.compile(
    rf"^(?P<experiment_set>urn:{MAVEDB_URN_NAMESPACE}:\d{{{MAVEDB_EXPERIMENT_SET_URN_DIGITS}}})"
    r"-(?P<experiment>[a-z]+|0)"
    r"-(?P<score_set>[1-9]\d*)$"
)

_VARIANT_URN_PARTS_RE = re.compile(r"^(?P<score_set>.+)#(?P<number>[1-9]\d*)$")

_UNPARSED = 1
"""Leading element for an undecomposable URN, so it sorts after every well-formed one.

Unpublished records carry ``tmp:<uuid>`` URNs; they still order stably, by the URN itself. Returning a key
rather than raising keeps a temporary URN from turning into a query error.
"""

_PARSED = 0


def score_set_urn_sort_key(urn: Optional[str]) -> tuple[int, str, int, str, int]:
    """Sort key ordering score set URNs the way their parts were assigned."""
    if not urn:
        return (_UNPARSED, "", 0, "", 0)

    match = _SCORE_SET_URN_PARTS_RE.match(urn)
    if match is None:
        return (_UNPARSED, urn, 0, "", 0)

    experiment = match["experiment"]
    return (_PARSED, match["experiment_set"], len(experiment), experiment, int(match["score_set"]))


def variant_urn_sort_key(urn: Optional[str]) -> tuple[int, str, int]:
    """Sort key ordering variant URNs by score set, then by numeric suffix.

    ``...#10`` is the tenth variant of a score set, not something between ``#1`` and ``#2``.
    """
    if not urn:
        return (_UNPARSED, "", 0)

    match = _VARIANT_URN_PARTS_RE.match(urn)
    if match is None:
        return (_UNPARSED, urn, 0)

    return (_PARSED, match["score_set"], int(match["number"]))
