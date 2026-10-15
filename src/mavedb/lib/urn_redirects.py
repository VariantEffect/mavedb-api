"""Forwarding of URNs that publication has retired.

A dataset is created with a ``tmp:<uuid>`` URN and keeps it until it is published, at which point
:func:`mavedb.routers.score_sets.publish_score_set` overwrites the URN in place with a permanent one.
Any link already shared to the unpublished record then stops resolving, which is what
https://github.com/VariantEffect/mavedb-ui/issues/617 reports: the record is still there, under a name
the caller has no way to guess.

Publication records what each retired URN became, and every read is checked here before it reaches its
route. A request naming a retired URN is answered with ``308 Permanent Redirect`` to the same path
under the record's current URN.

Three limits:

  - Only a public target is forwarded to. A ``Location`` header names the record it points at, and
    forwarding happens before any route checks a permission, so the header would disclose that URN to
    an anonymous caller. See _target_is_public.
  - Forwarding is one hop. Nothing in the application renames a published record, so a chain cannot
    arise; if one ever could, resolution here would need to follow it.
  - URNs retired before this was added are unrecoverable. Publication overwrote them and no history of
    them was kept, so links to records published earlier stay broken.
"""

import logging
from typing import Optional
from urllib.parse import quote

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.requests import Request

from mavedb.deps import get_db
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.core import has_permission
from mavedb.lib.validation.urn_re import (
    MAVEDB_EXPERIMENT_SET_URN_RE,
    MAVEDB_EXPERIMENT_URN_RE,
    MAVEDB_SCORE_SET_URN_RE,
    MAVEDB_TMP_URN_RE,
)
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.models.urn_redirect import UrnRedirect

logger = logging.getLogger(__name__)

# Methods whose requests are forwarded. See forward_retired_urns for why a write is not.
SAFE_METHODS = frozenset({"GET", "HEAD"})


def record_urn_redirect(db: Session, old_urn: Optional[str], new_urn: str) -> None:
    """
    Record that a record's URN has changed, so that requests naming the old one can be forwarded.

    Staged on the session rather than committed, so that a caller which reassigns several URNs -- as
    publication does, across an experiment set, an experiment and a score set -- commits the redirects
    together with the renames they describe.

    :param db: An active database session.
    :param old_urn: The URN being retired. A record that never had one, or a rename that is not a
        change, is not worth a row and is ignored.
    :param new_urn: The URN replacing it.
    """
    if not old_urn or old_urn == new_urn:
        return

    db.add(UrnRedirect(old_urn=old_urn, new_urn=new_urn))  # type: ignore[call-arg]


def _target_is_public(db: Session, urn: str) -> bool:
    """
    Report whether the record a redirect points to is one that may be named to any caller.

    A ``Location`` header discloses the URN it carries, to whoever asked -- including an anonymous
    caller, since forwarding happens before a route checks anything. Publication only ever records a
    redirect onto a record it is making public, and nothing in the application returns a published
    record to private, so a private target should not arise; a row written out of band, or by some
    later feature, would be enough for one to. Deferring to ``Action.READ`` with no user keeps that
    from becoming a disclosure, and keeps this answer in step with the rules a route would apply.

    A target that no longer exists is likewise not public: a deleted record leaves its redirect row
    behind, and forwarding to it would answer a permanent redirect with a 404.

    :param db: An active database session.
    :param urn: The URN a redirect points to.
    :return: True only if a record under this URN exists and an anonymous caller may read it.
    """
    target: Optional[ScoreSet | Experiment | ExperimentSet] = None
    if MAVEDB_SCORE_SET_URN_RE.fullmatch(urn):
        target = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    elif MAVEDB_EXPERIMENT_URN_RE.fullmatch(urn):
        target = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    elif MAVEDB_EXPERIMENT_SET_URN_RE.fullmatch(urn):
        target = db.query(ExperimentSet).filter(ExperimentSet.urn == urn).one_or_none()

    return target is not None and has_permission(None, target, Action.READ).permitted


def forwarded_path(db: Session, path: str) -> Optional[str]:
    """
    Rewrite a request path so that any retired URN in it names the record's current URN instead.

    Substitution is by substring, not by path segment, so a variant URN -- ``{score_set_urn}#{n}`` --
    is carried along by its score set's redirect.

    :param db: An active database session.
    :param path: The decoded request path.
    :return: The rewritten path, or None if the path should be served as it is: it names no retired URN,
        or one whose target this caller must not be told about. See _target_is_public.
    """
    # A live temporary URN belongs to an unpublished record and matches nothing in the table, so the
    # lookup below distinguishes the two cases and no separate check for publication is needed.
    candidate_urns = set(MAVEDB_TMP_URN_RE.findall(path))
    if not candidate_urns:
        return None

    redirects = db.execute(
        select(UrnRedirect.old_urn, UrnRedirect.new_urn).where(UrnRedirect.old_urn.in_(candidate_urns))
    ).all()
    if not redirects:
        return None

    forwarded = path
    for old_urn, new_urn in redirects:
        if not _target_is_public(db, new_urn):
            return None
        forwarded = forwarded.replace(old_urn, new_urn)

    return forwarded


def forward_retired_urns(request: Request, db: Session = Depends(get_db)) -> None:
    """
    Forward a request that names a retired URN to the same resource under its current URN.

    Installed as an application-wide dependency in :mod:`mavedb.server_main`, which is what makes one
    implementation cover every route that takes a URN, sub-resources included: a stale link to a score
    set's scores CSV or mapped variants is forwarded on the same terms as a link to the score set.

    Only reads are forwarded. A write to a retired URN keeps getting the 404 it gets today, which
    tells the client to look the record up again.
    """
    if request.scope["method"] not in SAFE_METHODS:
        return

    # The ASGI scope rather than request.url: Starlette builds request.url by re-parsing the decoded
    # path, so a variant URN's '#' starts a fragment there and everything after it -- the variant
    # number, the sub-resource, the query -- is silently dropped.
    path = request.scope["path"]
    query = request.scope.get("query_string", b"").decode("ascii")

    forwarded = forwarded_path(db, path)
    if forwarded is None:
        return

    # The scope's path is percent-decoded, so a '#' in it has to be re-encoded or it would open a
    # fragment in the header. Relative, so that a proxy's scheme and host survive.
    location = quote(forwarded, safe="/:")
    if query:
        location = f"{location}?{query}"

    save_to_logging_context({"requested_resource": path, "forwarded_to": forwarded})
    logger.info(msg="Forwarding a request that named a retired URN.", extra=logging_context())

    raise HTTPException(
        status_code=308,
        detail="This URN was replaced when the record was published; the record has moved permanently.",
        headers={"Location": location},
    )
