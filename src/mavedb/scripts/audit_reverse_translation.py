"""
Audit the allele graph that mapping and reverse translation (RT) write.

Usage:
```
python3 -m mavedb.scripts.audit_reverse_translation invariants [--score-set URN ...] [--skip-seqrepo]
python3 -m mavedb.scripts.audit_reverse_translation outcomes [--score-set URN ...] [--max-failed-pct 5]
python3 -m mavedb.scripts.audit_reverse_translation round-trip [--score-set URN ...] [--per-score-set 50]
```

Each check guards a property the serving layer assumes but no constraint enforces. The class of bug
this exists for is the NM_007294.3 refget divergence: the mapper and RT built the same variant on two
sequences, both digests were internally consistent, every existing check passed, and convergent alleles
silently read as projections.

`invariants` runs the structural checks in SQL, plus a comparison of every stored refget against
SeqRepo. `outcomes` rolls the latest RT result per variant up by score set, so an outlier score set
stands out. `round-trip` re-translates a sample of projection pairs with the same coordinate mapper RT
uses and checks that each pair and its protein apex identify as the same VRS alleles RT stored.

Each finding has a severity. `error` is a broken invariant; `warning` is a shape that is suspicious but
has legitimate causes, so read the samples; `info` is a count for context. `invariants` and `round-trip`
exit with status 1 when any error-level finding is non-zero.

Read-only. Safe to run against production, though `round-trip` makes one data-provider call per
translation and is paced by it.
"""

import json
import logging
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

import asyncclick as click
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from mavedb.lib.hgvs import strip_protein_prediction_parens
from mavedb.lib.seqrepo import AmbiguousSequenceError, SequenceNotFoundError, resolve_refget
from mavedb.lib.vrs_utils import CANONICAL_ACCESSION_PREFIXES, identify_variation, translate_hgvs_to_variation
from mavedb.models.score_set import ScoreSet
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)

ERROR = "error"
WARNING = "warning"
INFO = "info"

REFGET_PATH = "{location,sequenceReference,refgetAccession}"


@dataclass
class Finding:
    """The result of one check: how many rows violate it, and a few of them to read."""

    name: str
    severity: str
    description: str
    count: int
    samples: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class SqlCheck:
    """A check expressed as a query whose rows are the violations.

    ``sql`` may contain ``{allele_filter}``, ``{record_filter}`` or ``{link_filter}``, which expand to a
    score-set restriction when the audit is scoped and to nothing otherwise. ``scoped=False`` marks a
    check that has no meaning inside one score set (orphaned alleles belong to none) and is skipped then.
    """

    name: str
    severity: str
    description: str
    sql: str
    scoped: bool = True


SQL_CHECKS: tuple[SqlCheck, ...] = (
    SqlCheck(
        name="accession_with_multiple_refgets",
        severity=ERROR,
        description="Alleles on one accession carry more than one refget, so the same variant can mint two digests.",
        sql=f"""
            SELECT split_part(coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p), ':', 1) AS accession,
                   array_agg(DISTINCT a.post_mapped #>> '{REFGET_PATH}') AS refgets,
                   count(*) AS alleles
            FROM alleles a
            WHERE a.post_mapped #>> '{REFGET_PATH}' IS NOT NULL {{allele_filter}}
            GROUP BY 1
            HAVING count(DISTINCT a.post_mapped #>> '{REFGET_PATH}') > 1
        """,
    ),
    SqlCheck(
        name="level_disagrees_with_hgvs",
        severity=ERROR,
        description="An allele's level disagrees with the HGVS column it fills or that expression's type.",
        sql="""
            SELECT a.id AS allele_id, a.level, a.hgvs_g, a.hgvs_c, a.hgvs_p
            FROM alleles a
            WHERE (num_nonnulls(a.hgvs_g, a.hgvs_c, a.hgvs_p) <> 1
                   OR NOT ((a.level = 'genomic' AND split_part(coalesce(a.hgvs_g, ''), ':', 2) LIKE 'g.%')
                        OR (a.level = 'cdna' AND split_part(coalesce(a.hgvs_c, ''), ':', 2) ~ '^[cn][.]')
                        OR (a.level = 'protein' AND split_part(coalesce(a.hgvs_p, ''), ':', 2) LIKE 'p.%')))
              {allele_filter}
        """,
    ),
    SqlCheck(
        name="accession_disagrees_with_level",
        severity=WARNING,
        description="An allele's accession is not the kind its level implies (NC_ genomic, NM_/ENST cdna, NP_/ENSP protein).",
        sql="""
            SELECT a.id AS allele_id, a.level, coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p) AS hgvs
            FROM alleles a
            WHERE NOT ((a.level = 'genomic' AND coalesce(a.hgvs_g, '') ~ '^(NC|NG|NT|NW)_')
                    OR (a.level = 'cdna' AND coalesce(a.hgvs_c, '') ~ '^(NM_|NR_|XM_|XR_|ENST)')
                    OR (a.level = 'protein' AND coalesce(a.hgvs_p, '') ~ '^(NP_|XP_|ENSP)'))
              {allele_filter}
        """,
    ),
    SqlCheck(
        name="clingen_allele_id_on_twin_alleles",
        severity=ERROR,
        description=(
            "One ClinGen allele id sits on more than one allele on the same accession, so two writers built one "
            "change differently. A CA/PA id spans reference sequences, so the same id across accessions is expected."
        ),
        sql="""
            SELECT a.clingen_allele_id, split_part(coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p), ':', 1) AS accession,
                   count(*) AS alleles, array_agg(coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p) ORDER BY a.id) AS hgvs
            FROM alleles a
            WHERE a.clingen_allele_id IS NOT NULL {allele_filter}
            GROUP BY 1, 2
            HAVING count(*) > 1
        """,
    ),
    SqlCheck(
        name="mapped_record_without_authoritative_link",
        severity=ERROR,
        description="A live mapping record whose latest mapping succeeded has no live authoritative allele.",
        sql="""
            SELECT mr.id AS mapping_record_id, mr.variant_id, mr.score_set_id
            FROM mapping_records mr
            WHERE mr.valid_to IS NULL {record_filter}
              AND NOT EXISTS (SELECT 1 FROM mapping_record_alleles l
                              WHERE l.mapping_record_id = mr.id AND l.is_authoritative AND l.valid_to IS NULL)
              AND (SELECT e.disposition FROM annotation_event e
                   WHERE e.variant_id = mr.variant_id AND e.annotation_type = 'vrs_mapping'
                   ORDER BY e.id DESC LIMIT 1) = 'present'
        """,
    ),
    SqlCheck(
        name="live_link_on_retired_record",
        severity=ERROR,
        description="An allele link is live but its mapping record was retired; retiring a record retires its links.",
        sql="""
            SELECT l.id AS link_id, l.mapping_record_id, l.allele_id, mr.valid_to AS record_retired_at
            FROM mapping_record_alleles l
            JOIN mapping_records mr ON mr.id = l.mapping_record_id
            WHERE l.valid_to IS NULL AND mr.valid_to IS NOT NULL {link_filter}
        """,
    ),
    SqlCheck(
        name="measured_allele_outside_projection_group",
        severity=ERROR,
        description=(
            "RT attached derived nucleotide alleles to a record, but the measured nucleotide allele is in no "
            "projection group: the fold-in missed it, so its convergent siblings read as projections."
        ),
        sql="""
            SELECT auth.mapping_record_id, auth.allele_id, a.level, coalesce(a.hgvs_g, a.hgvs_c) AS hgvs
            FROM mapping_record_alleles auth
            JOIN alleles a ON a.id = auth.allele_id
            WHERE auth.is_authoritative AND auth.valid_to IS NULL AND auth.projection_group IS NULL
              AND a.level IN ('cdna', 'genomic')
              {auth_filter}
              AND EXISTS (SELECT 1 FROM mapping_record_alleles d
                          JOIN alleles da ON da.id = d.allele_id
                          WHERE d.mapping_record_id = auth.mapping_record_id
                            AND NOT d.is_authoritative AND d.valid_to IS NULL
                            AND da.level IN ('cdna', 'genomic'))
        """,
    ),
    SqlCheck(
        name="malformed_projection_group",
        severity=WARNING,
        description=(
            "A live projection group is not exactly one cdna allele and one genomic allele. A member RT could not "
            "translate leaves a group of one; the variant's cross_level_translation event lists it under "
            "failed_candidates with the error."
        ),
        sql="""
            SELECT l.mapping_record_id, l.projection_group, count(*) AS members,
                   string_agg(a.level, ',' ORDER BY a.level) AS levels
            FROM mapping_record_alleles l
            JOIN alleles a ON a.id = l.allele_id
            WHERE l.valid_to IS NULL AND l.projection_group IS NOT NULL {link_filter}
            GROUP BY 1, 2
            HAVING string_agg(a.level, ',' ORDER BY a.level) <> 'cdna,genomic'
        """,
    ),
    SqlCheck(
        name="record_with_multiple_protein_alleles",
        severity=WARNING,
        description="A live mapping record links more than one protein allele; RT shares a single protein apex per record.",
        sql="""
            SELECT l.mapping_record_id, count(*) AS protein_alleles,
                   array_agg(coalesce(a.hgvs_p, a.vrs_digest) ORDER BY a.id) AS hgvs
            FROM mapping_record_alleles l
            JOIN alleles a ON a.id = l.allele_id
            WHERE l.valid_to IS NULL AND a.level = 'protein' {link_filter}
            GROUP BY 1
            HAVING count(*) > 1
        """,
    ),
    SqlCheck(
        name="mapped_record_never_reverse_translated",
        severity=INFO,
        description="A live record with an authoritative allele has no RT event; expected until RT has run on its score set.",
        sql="""
            SELECT mr.id AS mapping_record_id, mr.variant_id, mr.score_set_id
            FROM mapping_records mr
            WHERE mr.valid_to IS NULL {record_filter}
              AND EXISTS (SELECT 1 FROM mapping_record_alleles l
                          WHERE l.mapping_record_id = mr.id AND l.is_authoritative AND l.valid_to IS NULL)
              AND NOT EXISTS (SELECT 1 FROM annotation_event e
                              WHERE e.variant_id = mr.variant_id AND e.annotation_type = 'cross_level_translation')
        """,
    ),
    SqlCheck(
        name="allele_without_live_link",
        severity=INFO,
        description=(
            "An allele no live mapping record links to. Superseded mappings, including every non-current legacy "
            "row the backfill imported, leave alleles with retired links only; one with no link at all was never linked."
        ),
        sql="""
            SELECT a.id AS allele_id, a.level, coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p) AS hgvs
            FROM alleles a
            WHERE NOT EXISTS (SELECT 1 FROM mapping_record_alleles l WHERE l.allele_id = a.id AND l.valid_to IS NULL)
        """,
        scoped=False,
    ),
)


def _filters(scoped: bool) -> dict[str, str]:
    """Score-set restrictions for each alias a check's SQL can use, or empty strings when unscoped."""
    if not scoped:
        return {"allele_filter": "", "record_filter": "", "link_filter": "", "auth_filter": ""}

    return {
        "allele_filter": (
            "AND EXISTS (SELECT 1 FROM mapping_record_alleles sl "
            "WHERE sl.allele_id = a.id AND sl.score_set_id = ANY(:score_set_ids))"
        ),
        "record_filter": "AND mr.score_set_id = ANY(:score_set_ids)",
        "link_filter": "AND l.score_set_id = ANY(:score_set_ids)",
        "auth_filter": "AND auth.score_set_id = ANY(:score_set_ids)",
    }


def run_sql_check(db: Session, check: SqlCheck, score_set_ids: Optional[list[int]], samples: int) -> Finding:
    """Count a check's violations and fetch the first few, optionally within some score sets."""
    # str.replace rather than str.format: the SQL holds JSON paths whose braces format would consume.
    sql = check.sql
    for placeholder, fragment in _filters(score_set_ids is not None).items():
        sql = sql.replace(f"{{{placeholder}}}", fragment)
    params: dict[str, Any] = {"samples": samples}
    if score_set_ids is not None:
        params["score_set_ids"] = score_set_ids

    count = db.execute(text(f"SELECT count(*) FROM ({sql}) violations"), params).scalar_one()
    rows = (
        db.execute(text(f"SELECT * FROM ({sql}) violations LIMIT :samples"), params).mappings().all() if count else []
    )
    return Finding(check.name, check.severity, check.description, count, [dict(row) for row in rows])


def check_refgets_against_seqrepo(
    db: Session,
    resolve: Callable[[str], str],
    score_set_ids: Optional[list[int]],
    samples: int,
) -> list[Finding]:
    """Compare every stored (accession, refget) pair with the refget SeqRepo holds for the accession.

    ``accession_with_multiple_refgets`` misses an accession whose alleles all agree on the wrong
    sequence; this catches it. Only accessions with a canonical prefix are checked, matching the
    insert-time guard in :func:`mavedb.lib.vrs_utils.verify_allele_refget`.

    ``resolve`` is :func:`mavedb.lib.seqrepo.resolve_refget` bound to a SeqRepo; it is a parameter so tests
    need not open one.
    """
    pairs_sql = f"""
        SELECT split_part(coalesce(a.hgvs_g, a.hgvs_c, a.hgvs_p), ':', 1) AS accession,
               a.post_mapped #>> '{REFGET_PATH}' AS refget,
               count(*) AS alleles
        FROM alleles a
        WHERE a.post_mapped #>> '{REFGET_PATH}' IS NOT NULL {_filters(score_set_ids is not None)["allele_filter"]}
        GROUP BY 1, 2
    """
    params = {"score_set_ids": score_set_ids} if score_set_ids is not None else {}

    mismatched: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    expected_by_accession: dict[str, Optional[str]] = {}

    for accession, refget, alleles in db.execute(text(pairs_sql), params).all():
        if not accession.startswith(CANONICAL_ACCESSION_PREFIXES):
            continue

        if accession not in expected_by_accession:
            try:
                expected_by_accession[accession] = resolve(accession)
            except (SequenceNotFoundError, AmbiguousSequenceError) as e:
                expected_by_accession[accession] = None
                unresolved.append({"accession": accession, "error": str(e)})

        expected = expected_by_accession[accession]
        if expected is not None and refget != expected:
            mismatched.append({"accession": accession, "stored": refget, "seqrepo": expected, "alleles": alleles})

    return [
        Finding(
            "refget_disagrees_with_seqrepo",
            ERROR,
            "Alleles sit on a refget other than the one SeqRepo holds for their accession.",
            sum(row["alleles"] for row in mismatched),
            mismatched[:samples],
        ),
        Finding(
            "accession_unresolved_in_seqrepo",
            WARNING,
            "SeqRepo has no sequence, or more than one, for an accession alleles use.",
            len(unresolved),
            unresolved[:samples],
        ),
    ]


def report(findings: Sequence[Finding]) -> int:
    """Log each finding with its samples and return how many error-level findings are non-zero."""
    for finding in findings:
        status = "ok" if finding.count == 0 else finding.severity.upper()
        logger.info("%-8s %10d  %s", status, finding.count, finding.name)
        if finding.count:
            logger.info("         %s", finding.description)
            for sample in finding.samples:
                logger.info("           %s", json.dumps(sample, default=str))

    return sum(1 for finding in findings if finding.severity == ERROR and finding.count)


def _score_set_ids(db: Session, urns: Sequence[str]) -> Optional[list[int]]:
    """Resolve ``--score-set`` URNs to ids, or ``None`` when the audit is unscoped."""
    if not urns:
        return None

    found: dict[str, int] = {
        urn: id for urn, id in db.execute(select(ScoreSet.urn, ScoreSet.id).where(ScoreSet.urn.in_(urns))).all() if urn
    }
    missing = sorted(set(urns) - set(found))
    if missing:
        raise click.BadParameter(f"No score set with URN: {', '.join(missing)}", param_hint="--score-set")

    return list(found.values())


score_set_option = click.option(
    "--score-set", "urns", multiple=True, help="Restrict the audit to this score set. Repeatable."
)


@script_environment.command()
@score_set_option
@click.option("--samples", type=int, default=5, show_default=True, help="Violating rows to print per check.")
@click.option("--skip-seqrepo", is_flag=True, help="Skip the refget comparison, which needs the SeqRepo mount.")
@with_database_session
def invariants(db: Session, urns: tuple[str, ...], samples: int, skip_seqrepo: bool) -> None:
    """Check the structural invariants of the mapping and reverse-translation allele graph."""
    score_set_ids = _score_set_ids(db, urns)

    findings: list[Finding] = []
    for check in SQL_CHECKS:
        if score_set_ids is not None and not check.scoped:
            continue
        logger.info("running %s", check.name)
        findings.append(run_sql_check(db, check, score_set_ids, samples))

    if not skip_seqrepo:
        from mavedb.data_providers.services import seqrepo

        sr = seqrepo()
        logger.info("running refget_disagrees_with_seqrepo")
        findings.extend(check_refgets_against_seqrepo(db, lambda acc: resolve_refget(sr, acc), score_set_ids, samples))

    if errors := report(findings):
        logger.error("%d error-level check(s) failed.", errors)
        sys.exit(1)


RT_OUTCOMES_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (e.variant_id) e.variant_id, e.score_set_id, e.disposition, e.reason
        FROM annotation_event e
        WHERE e.annotation_type = 'cross_level_translation' {event_filter}
        ORDER BY e.variant_id, e.id DESC
    )
    SELECT s.urn, latest.disposition, latest.reason, count(*) AS variants
    FROM latest
    JOIN scoresets s ON s.id = latest.score_set_id
    GROUP BY 1, 2, 3
"""


@dataclass
class ScoreSetOutcome:
    """The latest RT result of every variant in one score set, by disposition and by failure reason."""

    urn: str
    by_disposition: Counter = field(default_factory=Counter)
    failure_reasons: Counter = field(default_factory=Counter)

    @property
    def total(self) -> int:
        return sum(self.by_disposition.values())

    @property
    def failed_pct(self) -> float:
        return 100.0 * self.by_disposition["failed"] / self.total if self.total else 0.0


def collect_outcomes(rows: Sequence[tuple[str, str, str, int]]) -> list[ScoreSetOutcome]:
    """Fold ``(urn, disposition, reason, variants)`` rows into one outcome per score set, worst first."""
    outcomes: dict[str, ScoreSetOutcome] = {}
    for urn, disposition, reason, variants in rows:
        outcome = outcomes.setdefault(urn, ScoreSetOutcome(urn))
        outcome.by_disposition[disposition] += variants
        if disposition == "failed":
            outcome.failure_reasons[reason] += variants

    return sorted(outcomes.values(), key=lambda o: (-o.failed_pct, o.urn))


@script_environment.command()
@score_set_option
@click.option(
    "--max-failed-pct",
    type=float,
    default=5.0,
    show_default=True,
    help="Flag score sets where more than this share of variants failed RT.",
)
@with_database_session
def outcomes(db: Session, urns: tuple[str, ...], max_failed_pct: float) -> None:
    """Summarize the latest reverse-translation result per variant, by score set."""
    score_set_ids = _score_set_ids(db, urns)
    event_filter = "AND e.score_set_id = ANY(:score_set_ids)" if score_set_ids is not None else ""
    params = {"score_set_ids": score_set_ids} if score_set_ids is not None else {}

    rows = db.execute(text(RT_OUTCOMES_SQL.format(event_filter=event_filter)), params).all()
    results = collect_outcomes([tuple(row) for row in rows])  # type: ignore[misc]

    logger.info("%-28s %9s %9s %9s %9s  %s", "score set", "variants", "present", "n/a", "failed %", "failure reasons")
    for outcome in results:
        flag = "  <-- above threshold" if outcome.failed_pct > max_failed_pct else ""
        reasons = ", ".join(f"{reason} {n}" for reason, n in outcome.failure_reasons.most_common(3))
        logger.info(
            "%-28s %9d %9d %9d %8.1f%%  %s%s",
            outcome.urn,
            outcome.total,
            outcome.by_disposition["present"],
            outcome.by_disposition["not_applicable"],
            outcome.failed_pct,
            reasons,
            flag,
        )

    flagged = [o.urn for o in results if o.failed_pct > max_failed_pct]
    logger.info(
        "%d score set(s) reverse translated; %d above %.1f%% failed.", len(results), len(flagged), max_failed_pct
    )


ROUND_TRIP_SQL = """
    WITH pairs AS (
        SELECT l.score_set_id, l.mapping_record_id, l.projection_group,
               max(a.hgvs_c) FILTER (WHERE a.level = 'cdna') AS hgvs_c,
               max(a.vrs_digest) FILTER (WHERE a.level = 'cdna') AS cdna_digest,
               max(a.hgvs_g) FILTER (WHERE a.level = 'genomic') AS hgvs_g,
               max(a.vrs_digest) FILTER (WHERE a.level = 'genomic') AS genomic_digest
        FROM mapping_record_alleles l
        JOIN alleles a ON a.id = l.allele_id
        WHERE l.valid_to IS NULL AND l.projection_group IS NOT NULL {link_filter}
        GROUP BY 1, 2, 3
        HAVING count(*) FILTER (WHERE a.level = 'cdna') = 1 AND count(*) FILTER (WHERE a.level = 'genomic') = 1
    ),
    sampled AS (
        SELECT pairs.*,
               row_number() OVER (PARTITION BY score_set_id ORDER BY mapping_record_id, projection_group) AS n
        FROM pairs
    )
    SELECT sampled.mapping_record_id, sampled.projection_group, sampled.hgvs_c, sampled.cdna_digest,
           sampled.hgvs_g, sampled.genomic_digest,
           apex.digests AS protein_digests, apex.hgvs AS protein_hgvs
    FROM sampled
    LEFT JOIN LATERAL (
        SELECT array_agg(pa.vrs_digest ORDER BY pa.id) AS digests, array_agg(pa.hgvs_p ORDER BY pa.id) AS hgvs
        FROM mapping_record_alleles pl
        JOIN alleles pa ON pa.id = pl.allele_id
        WHERE pl.mapping_record_id = sampled.mapping_record_id AND pl.valid_to IS NULL AND pa.level = 'protein'
    ) apex ON true
    WHERE sampled.n <= :per_score_set
    ORDER BY sampled.mapping_record_id, sampled.projection_group
"""


@dataclass(frozen=True)
class ProjectionPair:
    """One live projection group as stored, plus the protein alleles live on its record."""

    mapping_record_id: int
    projection_group: int
    hgvs_c: str
    cdna_digest: str
    hgvs_g: str
    genomic_digest: str
    protein_digests: Sequence[str]
    protein_hgvs: Sequence[Optional[str]]


def round_trip_pair(
    pair: ProjectionPair,
    c_to_g: Callable[[str], str],
    c_to_p: Callable[[str], str],
    digest_of: Callable[[str], str],
) -> list[tuple[str, dict[str, Any]]]:
    """Re-derive a pair's genomic member and protein apex from its cdna member and compare digests.

    Comparing digests rather than strings makes the check indifferent to how an expression is written
    (3' shifting, prediction parentheses): two expressions for the same change identify to one VRS
    allele. Returns ``(outcome, detail)`` tuples, one for the genomic side and one for the protein side.

    The callables are RT's coordinate mapper and HGVS-to-digest translation; parameters so tests can
    supply fakes instead of a data provider and SeqRepo.
    """
    results: list[tuple[str, dict[str, Any]]] = []
    base = {
        "mapping_record_id": pair.mapping_record_id,
        "projection_group": pair.projection_group,
        "hgvs_c": pair.hgvs_c,
    }

    try:
        derived_g = c_to_g(pair.hgvs_c)
        derived_g_digest = digest_of(derived_g)
    except Exception as e:
        results.append(("genomic: could not re-derive", {**base, "error": str(e)}))
    else:
        if derived_g_digest == pair.genomic_digest:
            results.append(("genomic: matches", base))
        else:
            results.append(
                (
                    "GENOMIC: DISAGREES",
                    {**base, "stored": pair.hgvs_g, "derived": derived_g, "derived_digest": derived_g_digest},
                )
            )

    digests = [d for d in pair.protein_digests if d is not None]
    if not digests:
        results.append(("protein: no apex on record", base))
        return results

    try:
        derived_p = strip_protein_prediction_parens(c_to_p(pair.hgvs_c))
        derived_p_digest = digest_of(derived_p)
    except Exception as e:
        results.append(("protein: could not re-derive", {**base, "error": str(e)}))
        return results

    if derived_p_digest in digests:
        results.append(("protein: matches", base))
    else:
        results.append(
            (
                "PROTEIN: DISAGREES",
                {**base, "stored": list(pair.protein_hgvs), "derived": derived_p, "derived_digest": derived_p_digest},
            )
        )

    return results


@script_environment.command()
@score_set_option
@click.option(
    "--per-score-set",
    type=int,
    default=50,
    show_default=True,
    help="Projection pairs to re-translate in each score set.",
)
@click.option("--samples", type=int, default=10, show_default=True, help="Rows to print per disagreeing outcome.")
@with_database_session
def round_trip(db: Session, urns: tuple[str, ...], per_score_set: int, samples: int) -> None:
    """Re-translate sampled projection pairs and check they identify as the alleles RT stored."""
    from ga4gh.vrs.extras.translator import AlleleTranslator

    from mavedb.data_providers.services import cdot_rest, seqrepo_data_proxy
    from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator

    score_set_ids = _score_set_ids(db, urns)
    link_filter = "AND l.score_set_id = ANY(:score_set_ids)" if score_set_ids is not None else ""
    params: dict[str, Any] = {"per_score_set": per_score_set}
    if score_set_ids is not None:
        params["score_set_ids"] = score_set_ids

    coordinates = WorkerCoordinateTranslator(cdot_rest())
    translator = AlleleTranslator(seqrepo_data_proxy())

    def digest_of(hgvs: str) -> str:
        return identify_variation(translate_hgvs_to_variation(hgvs, translator))

    counts: Counter[str] = Counter()
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rows = db.execute(text(ROUND_TRIP_SQL.format(link_filter=link_filter)), params).mappings().all()
    logger.info("re-translating %d projection pair(s)", len(rows))

    for row in rows:
        pair = ProjectionPair(
            **{**row, "protein_digests": row["protein_digests"] or [], "protein_hgvs": row["protein_hgvs"] or []}
        )
        for outcome, detail in round_trip_pair(pair, coordinates.c_to_g, coordinates.c_to_p, digest_of):
            counts[outcome] += 1
            if len(examples[outcome]) < samples:
                examples[outcome].append(detail)

    for outcome, count in sorted(counts.items()):
        logger.info("%10d  %s", count, outcome)
        if not outcome.endswith("matches"):
            for detail in examples[outcome]:
                logger.info("            %s", json.dumps(detail, default=str))

    disagreements = counts["GENOMIC: DISAGREES"] + counts["PROTEIN: DISAGREES"]
    if disagreements:
        logger.error("%d re-translation(s) disagree with the stored alleles.", disagreements)
        sys.exit(1)


if __name__ == "__main__":
    script_environment()
