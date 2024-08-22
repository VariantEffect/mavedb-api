import logging
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.view_models.search import ExperimentsSearch
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation

logger = logging.getLogger(__name__)


def search_experiments(db: Session, owner: Optional[User], search: ExperimentsSearch) -> list[Experiment]:
    query = db.query(Experiment)
    # .filter(ScoreSet.private.is_(False))

    if owner is not None:
        query = query.filter(Experiment.created_by_id == owner.id)

    if search.published is not None:
        if search.published:
            query = query.filter(Experiment.published_date.isnot(None))
        else:
            query = query.filter(Experiment.published_date.is_(None))

    if search.text:
        lower_search_text = search.text.lower()
        query = query.filter(
            or_(
                Experiment.urn.icontains(lower_search_text),
                Experiment.title.icontains(lower_search_text),
                Experiment.short_description.icontains(lower_search_text),
                Experiment.abstract_text.icontains(lower_search_text),
                Experiment.publication_identifiers.any(
                    func.lower(PublicationIdentifier.identifier).icontains(lower_search_text)
                ),
                Experiment.publication_identifiers.any(
                    func.lower(PublicationIdentifier.abstract).icontains(lower_search_text)
                ),
                Experiment.publication_identifiers.any(
                    func.lower(PublicationIdentifier.title).icontains(lower_search_text)
                ),
                Experiment.publication_identifiers.any(
                    func.lower(PublicationIdentifier.publication_journal).icontains(lower_search_text)
                ),
                Experiment.publication_identifiers.any(
                    func.jsonb_path_exists(
                        PublicationIdentifier.authors, f"""$[*].name ? (@ like_regex "{lower_search_text}" flag "i")"""
                    )
                ),
            )
        )

    if search.publication_identifiers:
        query = query.filter(
            ScoreSet.publication_identifiers.any(PublicationIdentifier.identifier.in_(search.publication_identifiers))
        )

    if search.authors:
        query = query.filter(
            Experiment.publication_identifiers.any(
                func.jsonb_path_query_array(PublicationIdentifier.authors, "$.name").op("?|")(search.authors)
            )
        )

    if search.databases:
        query = query.filter(
            Experiment.publication_identifiers.any(PublicationIdentifier.db_name.in_(search.databases))
        )

    if search.journals:
        query = query.filter(
            Experiment.publication_identifiers.any(PublicationIdentifier.publication_journal.in_(search.journals))
        )

    if search.keywords:
        query = query.filter(
            Experiment.keyword_objs.any(
                ExperimentControlledKeywordAssociation.controlled_keyword.has(
                    ControlledKeyword.value.in_(search.keywords)
                )
            )
        )

    items: list[Experiment] = query.order_by(Experiment.title).all()
    if not items:
        items = []
    return items
