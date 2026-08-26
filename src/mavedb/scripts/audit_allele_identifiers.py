"""
Audit stored allele identifiers against their own content.

Usage:
```
python3 -m mavedb.scripts.audit_allele_identifiers audit
python3 -m mavedb.scripts.audit_allele_identifiers audit-locations
```

Every row in `alleles` asserts that `vrs_digest` is the GA4GH computed identifier of `post_mapped`.
Nothing enforced that until the mapping ingest path began recomputing identity on arrival, so rows
written earlier can carry an identifier minted before their own normalization. `audit` reports them, and `--repair` corrects the ones that can be corrected safely.

Run this before and after
`alembic/manual_migrations/migrate_mapped_variants_to_allele_substrate.py`. That migration now
recomputes identity as it writes, so it will not import drift — but `vrs_digest` is a UNIQUE dedup key
on an immutable, ValidTime-less table, so a wrong value there cannot be corrected in place afterwards.
Measuring before and expecting zero after is how that is kept honest.

`--repair` renames only: it rewrites `vrs_digest` where the corrected identifier is unused. Where the
corrected identifier already exists, the row is a duplicate of one that was always right, and merging
it means repointing five tables (`annotation_events`, `clinvar_allele_links`, `gnomad_allele_links`,
`mapping_record_alleles`, `vep_allele_consequences`), four of which carry a unique index including
`allele_id` and can therefore collide. That is deliberately not automated. A duplicate appearing now
means a writer got past the ingest guard in `worker/jobs/variant_processing/mapping.py`, and that wants
investigating rather than papering over.

`audit-pre-mapped` covers `mapping_records.vrs_digest` against `pre_mapped`. Not a dedup key, so a wrong
value is less destructive, but it is the same identity claim with the same cause and the backfill
republishes it.

`audit-locations` covers the same defect one level down, on the nested `SequenceLocation.id`, which
`audit` does not look at and which no repair here can fix. The two are separate because the causes and
the remedies are separate — see that command.

Read-only by default. Safe to run against production.
"""

import logging
from collections import Counter
from typing import Any, Optional

from ga4gh.core import ga4gh_identify
from ga4gh.vrs.models import SequenceLocation

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.lib.vrs_utils import identify_variation
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


def recomputed_identifier(post_mapped: dict[str, Any]) -> Optional[str]:
    """The identifier this document's own content implies, or ``None`` if it cannot be hydrated.

    Hydration goes through ``vrs_object_from_mapped_variant`` rather than the strict VRS models
    because older rows carry fields the current models forbid (``type`` on ``Extension``, ``label`` on
    ``SequenceReference``). Neither affects the digest — it is computed over coordinates and state — so
    the shim's tolerance costs nothing here.
    """
    try:
        return identify_variation(vrs_object_from_mapped_variant(dict(post_mapped)).root)
    except Exception:
        logger.debug("Could not hydrate an allele for identification", exc_info=True)
        return None


@script_environment.command()
@click.option("--limit", type=int, default=None, help="Examine at most this many alleles.")
@click.option("--repair", is_flag=True, help="Rewrite vrs_digest where the corrected identifier is unused.")
@with_database_session
def audit(db: Session, limit: Optional[int], repair: bool) -> None:
    """Report — and with ``--repair``, rename — alleles whose identifier disagrees with their content."""
    query = select(Allele).where(Allele.post_mapped.isnot(None)).order_by(Allele.id)
    if limit is not None:
        query = query.limit(limit)

    known = {digest for (digest,) in db.execute(select(Allele.vrs_digest)).all()}
    counts: Counter[str] = Counter()
    renames: list[tuple[Allele, str]] = []
    duplicates: list[tuple[Allele, str]] = []

    for allele in db.scalars(query):
        counts["examined"] += 1
        computed = recomputed_identifier(allele.post_mapped)

        if computed is None:
            counts["could not hydrate"] += 1
            continue
        if computed == allele.vrs_digest:
            counts["identifier matches content"] += 1
            continue

        counts["DRIFTED"] += 1
        # A corrected identifier that already exists means the allele is a duplicate of a row that was
        # always right: the drifted key simply failed to dedup against it. Those merge; the rest rename.
        (duplicates if computed in known else renames).append((allele, computed))

    for label, count in counts.most_common():
        logger.info("%8d  %s", count, label)
    logger.info("%8d  drifted, corrected identifier is unused (rename)", len(renames))
    logger.info("%8d  drifted, corrected identifier already exists (duplicate)", len(duplicates))

    if repair and renames:
        _rename(db, renames)
    elif renames:
        logger.warning("Run with --repair to correct %d rename(s).", len(renames))

    if duplicates:
        logger.error(
            "%d allele(s) duplicate a row that was always right. Not repaired here — merging repoints "
            "five tables, four with a unique index on allele_id. Investigate how they were written.",
            len(duplicates),
        )
        for allele, computed in duplicates[:20]:
            logger.error("  allele id=%s stored=%s content=%s", allele.id, allele.vrs_digest, computed)


def _rename(db: Session, renames: list[tuple["Allele", str]]) -> None:
    """Rewrite ``vrs_digest`` in place for rows whose corrected identifier is unused.

    Safe because the corrected identifier is known unused at classification time and this runs in one
    transaction: a concurrent writer taking it in between surfaces as a ``uq_alleles_vrs_digest``
    violation on flush rather than a silent overwrite. Only the key changes — ``post_mapped`` is left
    exactly as stored, since the body was never in question and allele bodies are immutable.
    """
    for allele, computed in renames:
        logger.info("renaming allele id=%s  %s -> %s", allele.id, allele.vrs_digest, computed)
        allele.vrs_digest = computed

    db.flush()
    logger.info("renamed %d allele(s)", len(renames))


@script_environment.command()
@click.option("--limit", type=int, default=None, help="Examine at most this many mapping records.")
@with_database_session
def audit_pre_mapped(db: Session, limit: Optional[int]) -> None:
    """Report mapping records whose ``vrs_digest`` disagrees with their ``pre_mapped`` content.

    Read-only, and no repair: ``mapping_records.vrs_digest`` is indexed but not unique, so a wrong value
    does not collapse two records into one the way it does on ``alleles``. It is still an identity claim
    about content the backfill republishes, and it has the same cause — an id minted before VRS
    normalization moved a del/dup span. The pre-mapped side had never been measured before 2026-08-25.
    """
    query = select(MappingRecord).where(MappingRecord.pre_mapped.isnot(None)).order_by(MappingRecord.id)
    if limit is not None:
        query = query.limit(limit)

    counts: Counter[str] = Counter()
    drifted: list[tuple[int, Optional[str], str]] = []

    for record in db.scalars(query):
        counts["examined"] += 1
        computed = recomputed_identifier(record.pre_mapped)

        if computed is None:
            counts["could not hydrate"] += 1
        elif computed == record.vrs_digest:
            counts["identifier matches content"] += 1
        elif record.vrs_digest is None:
            counts["no stored identifier"] += 1
        else:
            counts["DRIFTED"] += 1
            drifted.append((record.id, record.vrs_digest, computed))

    for label, count in counts.most_common():
        logger.info("%8d  %s", count, label)

    for record_id, stored, computed in drifted[:20]:
        logger.warning("  mapping_record id=%s stored=%s content=%s", record_id, stored, computed)
    if len(drifted) > 20:
        logger.warning("  ... and %d more", len(drifted) - 20)


@script_environment.command()
@click.option("--limit", type=int, default=None, help="Examine at most this many alleles.")
@with_database_session
def audit_locations(db: Session, limit: Optional[int]) -> None:
    """Report nested ``SequenceLocation`` identifiers that disagree with their own coordinates.

    Separate from :func:`audit` because it is a separate defect with a separate cause. ``audit`` checks
    ``vrs_digest`` against ``post_mapped`` — the allele's own identifier. ``ga4gh_identify`` writes only
    the ``id`` of the object it is handed; for sub-objects it calls ``get_or_create_digest`` and never
    ``get_or_create_ga4gh_identifier``. So an allele can carry a correct identifier over a location whose
    ``id`` was minted before normalization moved the span, and the allele-level audit reports it clean.

    Read-only. A drifted location is repaired by re-mapping the score set, since both writers now
    restamp the location — see ``lib/vrs_utils.py::identify_allele``.
    """
    query = select(Allele).where(Allele.post_mapped.isnot(None)).order_by(Allele.id)
    if limit is not None:
        query = query.limit(limit)

    counts: Counter[str] = Counter()
    drifted: set[int] = set()

    for allele in db.scalars(query):
        counts["alleles examined"] += 1
        for path, location in _stored_locations(allele.post_mapped):
            stored = location.get("id")
            computed = _recomputed_location_identifier(location)

            if computed is None:
                counts["could not hydrate location"] += 1
            elif stored is None:
                counts["location carries no id"] += 1
            elif stored == computed:
                counts["location id matches content"] += 1
            else:
                counts["location id DRIFTED"] += 1
                drifted.add(allele.id)
                logger.debug("Allele %s %s: %s -> %s", allele.id, path, stored, computed)

    for label, count in counts.most_common():
        logger.info("%9d  %s", count, label)
    logger.info("%9d  distinct allele rows carrying a drifted location", len(drifted))

    if drifted:
        logger.warning(
            "Re-map the affected score sets. A drifted location id is not repairable in place here — the "
            "allele's own identifier is unaffected, so nothing in this audit's scope is wrong."
        )


def _stored_locations(post_mapped: Any) -> list[tuple[str, dict[str, Any]]]:
    """Every ``SequenceLocation`` in a stored document, with a label saying where it sat.

    A ``CisPhasedBlock`` embeds one allele per member, each with its own location, and those member
    copies are how one writer's convention leaked into another's rows.
    """
    if not isinstance(post_mapped, dict):
        return []

    locations = []
    if isinstance(post_mapped.get("location"), dict):
        locations.append(("location", post_mapped["location"]))
    for index, member in enumerate(post_mapped.get("members") or []):
        if isinstance(member, dict) and isinstance(member.get("location"), dict):
            locations.append((f"members[{index}].location", member["location"]))

    return locations


def _recomputed_location_identifier(location: dict[str, Any]) -> Optional[str]:
    """The identifier this location's own coordinates imply, ignoring what it currently claims."""
    try:
        content = {key: value for key, value in location.items() if key not in ("id", "digest")}
        return ga4gh_identify(SequenceLocation(**content), in_place="always")
    except Exception:
        logger.debug("Could not hydrate a location for identification", exc_info=True)
        return None
