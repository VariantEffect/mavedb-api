"""Shared pieces for advertising CSV columns: the entry a picker renders, the label builders, and the
two questions about a score set that decide whether a namespace is offerable.

Each export owns its own discovery function, since what counts as "available" differs: the score-set CSV
asks about one score set, the variant CSV widens across every score set measuring the same allele.
"""

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from mavedb.lib.annotation.util import score_calibration_may_be_used_for_annotation
from mavedb.lib.csv.namespaces import (
    CLINVAR_DB_NAME,
    STATIC_CSV_NAMESPACE_LABELS,
    CsvNamespaceGroup,
    calibration_namespace_for_urn,
    clinvar_namespace_for_db_version,
    clinvar_namespace_label,
    clinvar_namespace_sort_key,
)
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant


@dataclass(frozen=True)
class AvailableCsvNamespaceEntry:
    """A namespace a record has data for, labeled and grouped for a picker."""

    namespace: str
    label: str
    group: CsvNamespaceGroup

    score_set: Optional[ScoreSet] = None
    """Owning score set, set for calibration namespaces only.

    A calibration means nothing against another score set's scores, and the variant CSV widens across
    several score sets, so a picker needs this to tell their calibrations apart.
    """

    selected_by_default: bool = True
    """Whether a picker should open with this group checked.

    False for research-use-only calibrations and for calibrations with no ranges. Answers only "what
    should a dialog open on" . Do not read this as a publish/include policy.
    """

    research_use_only: bool = False
    """Whether the data comes from a research-use-only calibration.

    Separate from ``selected_by_default`` so a consumer deciding what may be published can ask directly.
    """


def static_namespace_entry(namespace: str) -> AvailableCsvNamespaceEntry:
    """Build the labeled entry for a static namespace."""
    label, group = STATIC_CSV_NAMESPACE_LABELS[namespace]
    return AvailableCsvNamespaceEntry(namespace=namespace, label=label, group=group)


def clinvar_namespace_entries(namespaces: Iterable[str]) -> list[AvailableCsvNamespaceEntry]:
    """Build labeled entries for ClinVar release namespaces, newest first, newest selected by default."""
    entries: list[AvailableCsvNamespaceEntry] = []
    # Chronological key, not string order: this sort decides which release opens checked.
    for namespace in sorted(set(namespaces), key=clinvar_namespace_sort_key, reverse=True):
        label = clinvar_namespace_label(namespace)
        if label is not None:
            entries.append(
                AvailableCsvNamespaceEntry(
                    namespace=namespace,
                    label=label,
                    group=CsvNamespaceGroup.ANNOTATION,
                    selected_by_default=not entries,  # first to survive labelling wins the default
                )
            )

    return entries


def calibration_viewer(viewer: Optional[ScoreCalibrationViewer]) -> ScoreCalibrationViewer:
    """Resolve an omitted viewer to the anonymous one.

    A calibration carries its own ``private`` flag, and its READ permission is stricter than its score
    set's: a private one is readable only by its owner, by contributors when it is investigator-provided,
    or by an admin. Reading the score set is not enough, so every CSV path that names a calibration has to
    ask separately.

    This is the single place the CSV package decides what an absent viewer means, and it means the public
    subset: a call site that forgets to thread one serves what anyone could already see rather than
    everything. The rule itself lives in ``ScoreCalibrationViewer``, so it is never restated here.
    """
    return viewer if viewer is not None else ScoreCalibrationViewer()


def calibration_can_annotate(calibration: ScoreCalibration) -> bool:
    """Whether a calibration can support either kind of annotation, and so fill any of its columns.

    False for a calibration with no score ranges, whose every cell would be NA. Research-use-only standing
    is excluded from this question — it asks what a calibration *could* say, while who may see it is
    ``ScoreCalibrationViewer``'s job.
    """
    return any(
        score_calibration_may_be_used_for_annotation(
            calibration,
            annotation_type=annotation_type,  # type: ignore[arg-type]
            allow_research_use_only_calibrations=True,
        )
        for annotation_type in ("functional", "pathogenicity")
    )


def calibration_namespace_entries(calibrations: Iterable[ScoreCalibration]) -> list[AvailableCsvNamespaceEntry]:
    """Build labeled entries for calibrations, named by title so a picker can identify them.

    Research-use-only calibrations (labelled with a prefix) and rangeless ones are offered but excluded
    from the default selection.
    """
    entries = []
    for calibration in sorted(calibrations, key=lambda c: (str(c.title or ""), str(c.urn or ""))):
        if not calibration.urn:
            continue

        title = str(calibration.title) if calibration.title else str(calibration.urn)
        research_use_only = bool(calibration.research_use_only)
        entries.append(
            AvailableCsvNamespaceEntry(
                namespace=calibration_namespace_for_urn(str(calibration.urn)),
                label=f"Research Use Only: {title}" if research_use_only else title,
                group=CsvNamespaceGroup.CALIBRATION,
                score_set=calibration.score_set,
                research_use_only=research_use_only,
                selected_by_default=not research_use_only and calibration_can_annotate(calibration),
            )
        )

    return entries


def score_sets_have_current_mappings(db: Session, score_set_ids: Sequence[int]) -> bool:
    """Whether any variant in these score sets has a current mapping.

    Gates the mapping-derived namespaces: any mapping in a score set means the variant CSV
    should offer the namespaces, even if the variant in question is unmapped.
    """
    if not score_set_ids:
        return False

    return (
        db.scalars(
            select(MappedVariant.id)
            .join(MappedVariant.variant)
            .where(and_(Variant.score_set_id.in_(score_set_ids), MappedVariant.current.is_(True)))
            .limit(1)
        ).first()
        is not None
    )


def clinvar_release_namespaces(db: Session, score_set_ids: Sequence[int]) -> list[str]:
    """Every ClinVar release namespace these score sets have data for.

    Scoped to the score set, not the measurement, so a variant with no record still gets NA columns —
    an omitted column would read as "never consulted". Keyed on score set ids because deriving them
    inside the query measured slower.
    """
    if not score_set_ids:
        return []

    db_versions = db.scalars(
        select(ClinicalControl.db_version)
        .join(ClinicalControl.mapped_variants.of_type(MappedVariant))
        .join(MappedVariant.variant)
        .where(
            and_(
                Variant.score_set_id.in_(score_set_ids),
                MappedVariant.current.is_(True),
                ClinicalControl.db_name == CLINVAR_DB_NAME,
            )
        )
        .distinct()
    ).all()

    namespaces = [clinvar_namespace_for_db_version(str(version)) for version in db_versions]
    return [namespace for namespace in namespaces if namespace is not None]
