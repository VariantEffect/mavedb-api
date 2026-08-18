"""
Script that generates a dump of published MaveDB data.

Usage:
```
python3 -m mavedb.scripts.export_public_data
```

This generates a ZIP archive named `mavedb-dump.YYYYMMDDHHMMSS.zip` in the working directory.
See `src/mavedb/scripts/resources/README.md` for a full description of the archive contents and file formats.

Unpublished data and data sets licensed other than under the Creative Commons Zero license are not included in the dump,
and user details are limited to ORCID IDs and names of contributors to published data sets.

RELEASING A NEW VERSION: Before publishing a new version of this archive to Zenodo, add an entry describing what
changed to `src/mavedb/scripts/resources/CHANGELOG.md` (it is bundled into the archive). The full release procedure
is documented in `deployment/docs/zenodo-release.md`.
"""

import json
import logging
import os
from datetime import datetime, timezone
from itertools import chain
from typing import Callable, Iterable, Iterator, Optional, TypeVar
from zipfile import ZipFile

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, lazyload

from mavedb.lib.annotation.annotate import variant_highest_level_annotation
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.score_set import (
    available_score_set_csv_namespaces,
    get_score_set_variants_as_csv,
)
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_sets import get_current_mapped_variants_for_annotation
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import script_environment, with_database_session
from mavedb.view_models import mapped_variant as mapped_variant_vm
from mavedb.view_models.experiment_set import ExperimentSetPublicDump

logger = logging.getLogger(__name__)

S = TypeVar("S")
T = TypeVar("T")


SCORE_EXPORT_NAMESPACES: list[str] = [CsvNamespace.SCORES, CsvNamespace.SCORES_CUSTOM]
"""The namespaces behind `csv/{urn}.scores.csv`."""

PUBLIC_DUMP_LICENSE = "CC0"
"""The only license whose data the dump may carry."""


def annotation_export_namespaces(db: Session, score_set: ScoreSet, viewer: ScoreCalibrationViewer) -> list[str]:
    """The namespaces the public annotations CSV should carry for this score set.

    *viewer* has no default on purpose. Discovery resolves an omitted viewer to the public subset, which
    is the right answer for the dump but the wrong way to arrive at it: the archive's audience is a
    decision this script makes, so it says so rather than inheriting it.

    Asks discovery what the score set actually has rather than naming groups by hand. The previous
    hand-maintained list enumerated ClinVar releases one by one, so it emitted all-NA columns for releases
    never ingested, needed a code change for every new release, and was fragile to schema changes.

    The archive carries everything MaveDB holds about the score set, so this takes what discovery found
    and subtracts from it rather than opting groups in.

    In particular it does not filter on `selected_by_default`. That flag answers "what should a download
    dialog open on", which is a question about attention rather than about what exists, and the reasons a
    group opens unchecked are not interchangeable. An archive is about completeness, not about what a user
    should be nudged to look at first.

    Nor does it filter on `research_use_only`. A research-use-only calibration is public data, and every
    group it produces carries a `research_use_only` column stating its standing, so a consumer can filter
    on the data itself. VA-Spec NDJSON follows a different rule currently, see TODO(#803).

    Subtractions:

    - Every score and count group, and the score set's own identity: scores and counts get their own
      files, and the URN is in the filename, so repeating either would be noise.
    """
    excluded = {
        CsvNamespace.SCORES,
        CsvNamespace.SCORES_CUSTOM,
        CsvNamespace.COUNTS,
        CsvNamespace.SCORE_SET,
    }
    return [
        entry.namespace
        for entry in available_score_set_csv_namespaces(db, score_set, viewer=viewer)
        if entry.namespace not in excluded
    ]


def flatmap(f: Callable[[S], Iterable[T]], items: Iterable[S]) -> Iterable[T]:
    return chain.from_iterable(map(f, items))


def archive_path_base(score_set_urn: str) -> str:
    """The filename stem a score set's artifacts share, e.g. ``urn-mavedb-00000001-a-1``.

    Colons are not portable in archive member names on every platform, so the URN is hyphenated. The
    README documents the substitution as the way back to the URN, which makes it part of the published
    contract rather than an implementation detail.
    """
    return score_set_urn.replace(":", "-")


def score_set_has_current_mappings(db: Session, score_set: ScoreSet) -> bool:
    """Whether any variant in the score set has a current mapping.

    Gates the three mapping-derived artifacts. A score set whose mappings are all superseded yields no
    annotations, so emitting empty files for it would advertise absence as data.
    """
    return (
        db.scalars(
            select(ScoreSet)
            .where(ScoreSet.id == score_set.id)
            .join(Variant)
            .join(MappedVariant)
            .where(MappedVariant.current.is_(True))
            .limit(1)
        ).one_or_none()
        is not None
    )


def scores_csv(db: Session, score_set: ScoreSet) -> str:
    """`csv/{urn}.scores.csv` — every score column the investigator uploaded."""
    return get_score_set_variants_as_csv(db, score_set, SCORE_EXPORT_NAMESPACES, namespaced=True)


def counts_csv(db: Session, score_set: ScoreSet) -> Optional[str]:
    """`csv/{urn}.counts.csv`, or None for a score set that defines no count columns."""
    dataset_columns = score_set.dataset_columns if isinstance(score_set.dataset_columns, dict) else {}
    if not dataset_columns.get("count_columns"):
        return None

    return get_score_set_variants_as_csv(db, score_set, [CsvNamespace.COUNTS], namespaced=True)


def annotations_csv(db: Session, score_set: ScoreSet, viewer: ScoreCalibrationViewer) -> str:
    """`csv/{urn}.annotations.csv` — every annotation namespace discovery offers for the score set.

    The same *viewer* selects the namespaces and resolves their cells. Threading one viewer through both
    is what keeps a calibration from being offered as a column group and then withheld as data, or the
    reverse.
    """
    return get_score_set_variants_as_csv(
        db,
        score_set,
        annotation_export_namespaces(db, score_set, viewer),
        namespaced=True,
        viewer=viewer,
    )


def mapped_variants_json(db: Session, score_set: ScoreSet) -> str:
    """`mapped/{urn}.mapped-variants.json` — the score set's current mapped variants.

    Same shape as GET /api/v1/score-sets/{urn}/mapped-variants, but narrower: that endpoint also
    returns superseded mappings, while this dump includes only each variant's current mapping.
    """
    mapped_variants = db.scalars(
        select(MappedVariant)
        .join(Variant, Variant.id == MappedVariant.variant_id)
        .options(joinedload(MappedVariant.variant))
        .where(Variant.score_set_id == score_set.id)
        .where(MappedVariant.current.is_(True))
    ).all()

    views = [mapped_variant_vm.MappedVariant.model_validate(mv) for mv in mapped_variants]
    return json.dumps(jsonable_encoder(views))


def va_ndjson(db: Session, score_set: ScoreSet, principal: Principal) -> str:
    """`va/{urn}.va.ndjson` — one record per current mapped variant at its highest materialized VA level.

    Mirrors the GET /api/v1/score-sets/{urn}/annotated-variants/* streams. Every record is
    newline-terminated, the last one included, so a line-based consumer needs no special case.
    """
    lines = []
    for mv in get_current_mapped_variants_for_annotation(db, score_set):
        annotation = variant_highest_level_annotation(mv, principal=principal)
        record = {
            "variant_urn": mv.variant.urn,
            "annotation": annotation.model_dump(exclude_none=True) if annotation else None,
        }
        lines.append(json.dumps(record, default=str))

    return "".join(line + "\n" for line in lines)


def score_set_artifacts(db: Session, score_set: ScoreSet, principal: Principal) -> Iterator[tuple[str, str]]:
    """Every archive entry one score set contributes, as ``(path within the zip, content)`` pairs.

    Scores are unconditional. Counts appear only where count columns are defined, and the three
    mapping-derived artifacts only where a current mapping exists — see the README's caveats, which
    promise exactly this and are what a consumer checks a missing file against.

    A generator rather than a dict so the caller writes each artifact and lets it go. Returning them
    together would hold all four of a score set's payloads in memory at once, and one score set's
    ``va.ndjson`` alone runs to tens of kilobytes per variant once a pathogenicity layer materializes.
    """
    base = archive_path_base(str(score_set.urn))
    viewer = principal.viewer_for(ScoreCalibrationViewer)

    yield f"csv/{base}.scores.csv", scores_csv(db, score_set)

    if score_set_has_current_mappings(db, score_set):
        yield f"csv/{base}.annotations.csv", annotations_csv(db, score_set, viewer)
        yield f"mapped/{base}.mapped-variants.json", mapped_variants_json(db, score_set)
        yield f"va/{base}.va.ndjson", va_ndjson(db, score_set, principal)

    counts = counts_csv(db, score_set)
    if counts is not None:
        yield f"csv/{base}.counts.csv", counts


def public_experiment_set(
    experiment_set_view: ExperimentSetPublicDump, visible_calibration_ids: set[int]
) -> Optional[ExperimentSetPublicDump]:
    """
    Narrow a validated experiment set to what belongs in the public dump.

    Drops calibrations an anonymous caller may not read, then experiments left with no score sets, and
    returns None for an experiment set left with no experiments. The score sets themselves need no filter:
    the loading query already restricts them to published, CC0-licensed ones.

    Narrowing the validated view rather than the ORM graph is deliberate. ``ExperimentSet.experiments`` and
    ``ScoreSet.score_calibrations`` are both mapped with ``cascade="all, delete-orphan"``, so removing a
    member from either ORM collection marks the removed row as an orphan and the next flush deletes it.
    This script can flush: ``with_database_session`` commits when invoked with ``--commit``.

    Args:
        experiment_set_view (ExperimentSetPublicDump): The validated experiment set to narrow.
        visible_calibration_ids (set[int]): Ids of the calibrations an anonymous caller may read.

    Returns:
        Optional[ExperimentSetPublicDump]: The narrowed experiment set, or None if nothing public remains.
    """
    experiments = []
    for experiment_view in experiment_set_view.experiments:
        if not experiment_view.score_sets:
            continue

        score_sets = [
            score_set_view.model_copy(
                update={
                    "score_calibrations": [
                        calibration
                        for calibration in (score_set_view.score_calibrations or [])
                        if calibration.id in visible_calibration_ids
                    ]
                }
            )
            for score_set_view in experiment_view.score_sets
        ]
        experiments.append(experiment_view.model_copy(update={"score_sets": score_sets}))

    if not experiments:
        return None

    return experiment_set_view.model_copy(update={"experiments": experiments})


def published_experiment_sets(db: Session) -> list[ExperimentSet]:
    """Every published experiment set, with its members narrowed to what the dump may carry.

    The narrowing is in the loader rather than applied afterwards, so an unpublished experiment or a
    non-CC0 score set is never loaded onto the graph the metadata view is validated from. An experiment
    set can survive this with no members left; ``public_experiment_set`` drops those.
    """
    return list(
        db.scalars(
            select(ExperimentSet)
            .where(ExperimentSet.published_date.is_not(None))
            .options(
                lazyload(ExperimentSet.experiments.and_(Experiment.published_date.is_not(None))).options(
                    lazyload(
                        Experiment.score_sets.and_(
                            ScoreSet.published_date.is_not(None),
                            ScoreSet.license.has(License.short_name == PUBLIC_DUMP_LICENSE),
                        )
                    )
                )
            )
            .execution_options(populate_existing=True)
            .order_by(ExperimentSet.urn)
        ).all()
    )


def public_dump_metadata(db: Session, principal: Principal) -> tuple[dict, list[str]]:
    """The `main.json` payload, and the score-set URNs whose artifacts the archive carries.

    One function for both because they are one decision: a score set is in the archive exactly when its
    metadata survived narrowing, so deriving the URN list from the narrowed views rather than from the
    query keeps the two from disagreeing.

    Publishing a score set does not publish its calibrations — a calibration keeps its own `private` flag
    and a stricter READ rule — so which calibrations appear is asked of *principal* rather than inferred
    from the score set's own visibility.
    """
    experiment_sets = published_experiment_sets(db)

    viewer = principal.viewer_for(ScoreCalibrationViewer)
    all_calibrations = [
        calibration
        for score_set_orm in flatmap(lambda es: flatmap(lambda e: e.score_sets, es.experiments), experiment_sets)
        for calibration in (score_set_orm.score_calibrations or [])
    ]

    # TODO(#372): Nullable ids.
    visible_calibration_ids: set[int] = {calibration.id for calibration in viewer.visible(all_calibrations)}  # type: ignore
    if len(all_calibrations) > len(visible_calibration_ids):
        logger.info(
            f"Withholding {len(all_calibrations) - len(visible_calibration_ids)} non-public score "
            "calibration(s) from the dump."
        )

    # TODO To support very large data sets, we may want to use custom code for JSON-encoding an iterator.
    # Issue: https://github.com/VariantEffect/mavedb-api/issues/192
    # See, for instance, https://stackoverflow.com/questions/12670395/json-encoding-very-long-iterators.

    experiment_set_views = [
        narrowed
        for narrowed in (
            public_experiment_set(ExperimentSetPublicDump.model_validate(es), visible_calibration_ids)
            for es in experiment_sets
        )
        if narrowed is not None
    ]
    logger.info(f"Found {len(experiment_set_views)} published experiment sets with CC0-licensed score sets.")

    metadata = {
        "title": "MaveDB public data",
        "asOf": datetime.now(timezone.utc).isoformat(),
        "experimentSets": experiment_set_views,
    }
    score_set_urns = list(
        flatmap(
            lambda es: flatmap(lambda e: map(lambda ss: ss.urn, e.score_sets), es.experiments), experiment_set_views
        )
    )

    return metadata, score_set_urns


def write_public_dump(db: Session, principal: Principal, archive: ZipFile) -> list[str]:
    """Write every member of the public dump into *archive*, and report the score sets carried.

    Takes the archive rather than a filename so the whole composition — metadata, resources, and each
    score set's artifacts — can be exercised without touching the filesystem.
    """
    metadata, score_set_urns = public_dump_metadata(db, principal)

    # Metadata for all data sets goes in a single JSON file.
    archive.writestr("main.json", json.dumps(jsonable_encoder(metadata)))

    # Copy the CC0 license and README.
    resources_dir = os.path.join(os.path.dirname(__file__), "resources")
    archive.write(os.path.join(resources_dir, "CC0_license.txt"), "LICENSE.txt")
    archive.write(os.path.join(resources_dir, "README.md"), "README.md")
    archive.write(os.path.join(resources_dir, "CHANGELOG.md"), "CHANGELOG.md")

    num_score_sets = len(score_set_urns)
    for i, score_set_urn in enumerate(score_set_urns):
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one_or_none()
        if score_set is None:
            # `main.json` already names this score set, so skipping it silently would leave the archive
            # advertising files it does not contain. Reachable only if the row disappears mid-run.
            logger.warning(
                f"[{i + 1}/{num_score_sets}] {score_set_urn} is named in main.json but could no longer be "
                "loaded; the archive will carry no files for it."
            )
            continue

        logger.info(f"[{i + 1}/{num_score_sets}] Exporting score set {score_set_urn}")
        written = []
        for path, content in score_set_artifacts(db, score_set, principal):
            archive.writestr(path, content)
            written.append(path)
        logger.info(f"[{i + 1}/{num_score_sets}]   Wrote {', '.join(sorted(written))}")

    return score_set_urns


@script_environment.command()
@with_database_session
def export_public_data(db: Session):
    # The dump is built for an anonymous principal, so every artifact carries what any member of the
    # public could already see.
    public_principal = Principal()

    timestamp_format = "%Y%m%d%H%M%S"
    zip_file_name = f"mavedb-dump.{datetime.now().strftime(timestamp_format)}.zip"

    logger.info(f"Writing {zip_file_name}.")
    with ZipFile(zip_file_name, "w") as archive:
        write_public_dump(db, public_principal, archive)

    logger.info(f"Export complete: {zip_file_name}")


if __name__ == "__main__":
    export_public_data()
