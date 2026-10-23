"""Search for the external identifiers that records cite, limited to records the caller may read.

This is kept apart from :mod:`mavedb.lib.identifiers` because it needs the permission checks, which import the
web framework, and view models import that module.
"""

from typing import Any, Sequence, Union

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from mavedb.lib.permissions.experiment import ExperimentViewer
from mavedb.lib.permissions.experiment_set import ExperimentSetViewer
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_set import ScoreSetViewer
from mavedb.lib.permissions.viewer import Viewer
from mavedb.models.doi_identifier import ExperimentDoiIdentifier, ExperimentSetDoiIdentifier, ScoreSetDoiIdentifier
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.mixins.external_identifier import ExternalIdentifier
from mavedb.models.raw_read_identifier import (
    ExperimentRawReadIdentifier,
    ExperimentSetRawReadIdentifier,
    ScoreSetRawReadIdentifier,
)
from mavedb.models.score_set import ScoreSet

IDENTIFIER_SEARCH_LIMIT = 50

# A table to search for identifiers: its link model, the viewer for the record that governs each row's
# visibility, and a query selecting ``(link row, record)``.
IdentifierSource = tuple[type[ExternalIdentifier], type[Viewer[Any]], Select]


def search_identifiers_on_readable_records(
    db: Session,
    principal: Principal,
    text: str,
    sources: Sequence[IdentifierSource],
) -> list[ExternalIdentifier]:
    """Find identifiers containing ``text`` that are attached to records the caller may read.

    Identifiers are stored on the rows that link them to their records, so one identifier can appear on many
    rows. This returns one row per distinct identifier, sorted by identifier.

    Args:
        db: An active database session.
        principal: The caller.
        text: The text to search for, matched case-insensitively anywhere in the identifier.
        sources: The tables to search.

    Returns:
        Up to ``IDENTIFIER_SEARCH_LIMIT`` link rows, one per distinct identifier.
    """
    # Read access to a record is decided in Python, so the filter runs before the limit. These tables hold a
    # few hundred rows. Once row-level security enforces reads (#808), the database does this filtering.
    found: dict[str, ExternalIdentifier] = {}
    for link, viewer_class, query in sources:
        viewer = principal.viewer_for(viewer_class)
        rows = db.execute(query.where(func.lower(link.identifier).contains(text.lower(), autoescape=True))).all()
        for link_row, record in rows:
            if link_row.identifier in found:
                continue
            if viewer.may_read(record):
                found[link_row.identifier] = link_row

    return [found[identifier] for identifier in sorted(found)][:IDENTIFIER_SEARCH_LIMIT]


def identifier_sources_on_records(
    score_set_link: Union[type[ScoreSetDoiIdentifier], type[ScoreSetRawReadIdentifier]],
    experiment_link: Union[type[ExperimentDoiIdentifier], type[ExperimentRawReadIdentifier]],
    experiment_set_link: Union[type[ExperimentSetDoiIdentifier], type[ExperimentSetRawReadIdentifier]],
) -> list[IdentifierSource]:
    """The sources for an identifier that score sets, experiments and experiment sets each cite.

    Args:
        score_set_link: The model linking the identifier to score sets.
        experiment_link: The model linking the identifier to experiments.
        experiment_set_link: The model linking the identifier to experiment sets.

    Returns:
        A source for each record type, for :func:`search_identifiers_on_readable_records`.
    """
    return [
        (
            score_set_link,
            ScoreSetViewer,
            select(score_set_link, ScoreSet).join(ScoreSet, ScoreSet.id == score_set_link.score_set_id),
        ),
        (
            experiment_link,
            ExperimentViewer,
            select(experiment_link, Experiment).join(Experiment, Experiment.id == experiment_link.experiment_id),
        ),
        (
            experiment_set_link,
            ExperimentSetViewer,
            select(experiment_set_link, ExperimentSet).join(
                ExperimentSet, ExperimentSet.id == experiment_set_link.experiment_set_id
            ),
        ),
    ]
