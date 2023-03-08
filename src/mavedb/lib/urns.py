import re
import string

from sqlalchemy import func
from sqlalchemy.orm import Session

from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.scoreset import Scoreset


def generate_experiment_set_urn(db: Session):
    # PostgreSQL-specific operator: ~
    # TODO Provide an alternative for use with SQLite in unit tests.
    # TODO We can't use func.max if an experiment set URN's numeric part will ever have anything other than 8 digits,
    # because we rely on the order guaranteed by zero-padding. This assumption is valid until we have 99999999
    # experiment sets.
    row = db.query(func.max(ExperimentSet.urn)) \
        .filter(ExperimentSet.urn.op('~')('^urn:mavedb:[0-9]+$')) \
        .one_or_none()
    max_urn_number = 0
    if row and row[0]:
        max_urn = row[0]
        max_urn_number = int(re.search('^urn:mavedb:([0-9]+)$', max_urn).groups(1)[0])
    next_urn_number = max_urn_number + 1
    return f'urn:mavedb:{next_urn_number:08}'


def generate_experiment_urn(db: Session, experiment_set: ExperimentSet, experiment_is_meta_analysis: bool):
    experiment_set_urn = experiment_set.urn

    if experiment_is_meta_analysis:
        # Do not increment for meta-analysis, since this is a singleton
        next_suffix = '0'
    else:
        # PostgreSQL-specific operator: ~
        # TODO Provide an alternative for use with SQLite in unit tests.
        published_experiments_query = db.query(Experiment) \
            .filter(Experiment.experiment_set_id == experiment_set.id) \
            .filter(Experiment.urn.op('~')(f'^{re.escape(experiment_set_urn)}-[a-z]+$'))
        max_suffix = None
        for experiment in published_experiments_query:
            suffix = re.search(f'^{re.escape(experiment.urn)}-([a-z]+)$').group(1)
            if suffix and (max_suffix is None or len(max_suffix) < len(experiment.urn) or max_suffix < suffix):
                max_suffix = suffix
        if max_suffix is None:
            next_suffix = 'a'
        else:
            max_suffix_number = 0
            while len(max_suffix) > 0:
                max_suffix_number *= 26
                max_suffix_number += string.ascii_lowercase.index(max_suffix[0]) + 1
                max_suffix = max_suffix[1:]
            next_suffix_number = max_suffix_number + 1
            next_suffix = ''
            x = next_suffix_number
            while x > 0:
                x, y = divmod(x - 1, len(string.ascii_lowercase))
                next_suffix = f'{string.ascii_lowercase[y]}{next_suffix}'
    return f'{experiment_set_urn}-{next_suffix}'


def generate_scoreset_urn(db: Session, experiment: Experiment):
    experiment_urn = experiment.urn

    # PostgreSQL-specific operator: ~
    # TODO Provide an alternative for use with SQLite in unit tests.
    published_scoresets_query = db.query(Scoreset) \
        .filter(Scoreset.experiment_id == experiment.id) \
        .filter(Scoreset.urn.op('~')(f'^{re.escape(experiment_urn)}-[0-9]+$'))
    max_suffix_number = 0
    for scoreset in published_scoresets_query:
        suffix_number = int(re.search(f'^{re.escape(experiment.urn)}-([0-9]+)$', scoreset.urn).group(1))
        if suffix_number > max_suffix_number:
            max_suffix_number = suffix_number
    next_suffix_number = max_suffix_number + 1
    return f'{experiment_urn}-{next_suffix_number}'
