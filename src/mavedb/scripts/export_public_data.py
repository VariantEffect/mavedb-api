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
from mavedb.lib.annotation.context import variant_annotation_context
from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.cat_vrs import build_categorical_variant
from mavedb.lib.csv.entries import score_sets_have_current_mappings
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.score_set import (
    available_score_set_csv_namespaces,
    get_score_set_variants_as_csv,
)
from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_sets import get_annotatable_variants
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


def _emit_legacy_mapped_variants_artifact() -> bool:
    """Whether to emit the deprecated ``mapped/{urn}.mapped-variants.json`` artifact.

    Superseded by ``vrs/{urn}.vrs.ndjson`` and removed with the MappedVariant table drop; kept for a
    deprecation window until then. Set ``EXPORT_LEGACY_MAPPED_VARIANTS=0`` (or ``false``/``no``/``off``) to
    stop emitting it before the boundary. Defaults to on.
    """
    return os.getenv("EXPORT_LEGACY_MAPPED_VARIANTS", "1").strip().lower() not in {"0", "false", "no", "off"}


def annotation_export_namespaces(db: Session, score_set: ScoreSet, viewer: ScoreCalibrationViewer) -> list[str]:
    """The namespaces the public annotations CSV should carry for this score set.

    *viewer* has no default: the dump always targets the anonymous/public audience, and that should be
    explicit here rather than inherited from discovery's default.

    Derived from discovery rather than a hand-maintained list, so a new release needs no code change here.
    Includes everything discovery finds regardless of `selected_by_default` (a UI attention flag, not a
    completeness one) or `research_use_only` (each group carries its own `research_use_only` column, so
    consumers can filter on the data itself — VA-Spec NDJSON follows a different rule, see TODO(#803)).

    Excludes the score, count, and score-set-identity groups: those get their own files, and the URN is
    already in the filename.
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
    """The filename stem shared by a score set's artifacts, e.g. ``urn-mavedb-00000001-a-1``.

    Colons aren't portable in archive member names on every platform, so the URN is hyphenated. The
    README documents the substitution as the way back to the URN.
    """
    return score_set_urn.replace(":", "-")


def score_set_has_current_mappings(db: Session, score_set: ScoreSet) -> bool:
    """Whether any variant in the score set has a live mapping on the allele substrate.

    Gates the artifacts built from that substrate, so a score set whose mappings are all superseded
    doesn't emit empty files. Reads the same predicate CSV discovery uses rather than the frozen
    ``MappedVariant`` table, which no longer gets writes and would report every score set mapped since
    the migration as unmapped.
    """
    # TODO(#372): non-null id fields
    return score_sets_have_current_mappings(db, [score_set.id])  # type: ignore


def score_set_has_legacy_mapped_variants(db: Session, score_set: ScoreSet) -> bool:
    """Whether the frozen ``MappedVariant`` table still holds current rows for this score set.

    Gates ``mapped/{urn}.mapped-variants.json``, the one artifact still sourced from that table. Score
    sets mapped after the allele-substrate migration have no such rows.
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

    The same *viewer* selects the namespaces and resolves their cells, so a calibration can't be offered
    as a column group and then withheld as data, or vice versa.
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

    Legacy, and the last artifact sourced from the frozen ``MappedVariant`` table. The endpoint this
    once mirrored is gone — GET /score-sets/{urn}/mapped-variants now returns 410, and its replacement
    serves a different field shape (see ``get_score_set_mapped_variants_removed``).
    ``vrs/{urn}.vrs.ndjson`` supersedes this file in the archive. Only each variant's current mapping
    is included, never a superseded one.
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
    """`va/{urn}.va.ndjson` — one record per annotatable variant at its highest materialized VA level.

    Mirrors the GET /api/v1/score-sets/{urn}/annotated-variants/* streams, which select the same set.
    Every record is newline-terminated, including the last, so a line-based consumer needs no special
    case. A variant the pipeline could not place carries no post-mapped allele, is therefore not
    annotatable, and contributes no line; ``annotation`` is null where a placed variant yields no
    VA-Spec layer.
    """
    lines = []
    for variant in get_annotatable_variants(db, score_set):
        context = variant_annotation_context(db, variant)
        annotation = variant_highest_level_annotation(context, principal=principal) if context is not None else None
        record = {
            "variant_urn": variant.urn,
            "annotation": annotation.model_dump(exclude_none=True) if annotation else None,
        }
        lines.append(json.dumps(record, default=str))

    return "".join(line + "\n" for line in lines)


def vrs_ndjson(db: Session, score_set: ScoreSet) -> str:
    """`vrs/{urn}.vrs.ndjson` — GA4GH VRS objects for each mapped variant, one line each.

    Carries the VRS pair plus the Cat-VRS categorical variant, built from the same live allele links
    `GET /variants/{urn}` serves. Its own artifact rather than CSV columns because these are nested
    objects; join to `annotations.csv` on `mavedb.post_mapped_vrs_id`.

    `pre_mapped` is the assayed-level VRS on the target's own reference; `post_mapped` is the measured
    allele lifted to a genomic or transcript reference. `categorical_variant` is spec-pure Cat-VRS (no
    MaveDB fields), anchored on the measured allele with the derived alleles as members — null when
    there's no hydratable authoritative allele.

    Emitted for every mapped variant regardless of calibration visibility, since none of this is
    calibration-derived.
    """
    lines = []
    for variant in get_annotatable_variants(db, score_set):
        links = get_live_record_allele_links(db, variant.id)
        authoritative = next((link.allele for link in links if link.is_authoritative), None)
        record = next((link.mapping_record for link in links), None)
        transit = build_categorical_variant(links, name=variant.urn or "")

        lines.append(
            json.dumps(
                {
                    "variant_urn": variant.urn,
                    "pre_mapped": record.pre_mapped if record is not None else None,
                    "post_mapped": authoritative.post_mapped if authoritative is not None else None,
                    "categorical_variant": (
                        transit.categorical_variant.model_dump(mode="json", exclude_none=True)
                        if transit is not None
                        else None
                    ),
                },
                default=str,
            )
        )

    return "".join(line + "\n" for line in lines)


def score_set_artifacts(db: Session, score_set: ScoreSet, principal: Principal) -> Iterator[tuple[str, str]]:
    """Every archive entry one score set contributes, as ``(path within the zip, content)`` pairs.

    Scores are unconditional; counts and the mapping-derived artifacts appear only when the score set
    has them — see the README's caveats. A generator rather than a dict so the caller writes and
    releases each artifact, instead of holding a score set's full payload in memory at once.
    """
    base = archive_path_base(str(score_set.urn))
    viewer = principal.viewer_for(ScoreCalibrationViewer)

    yield f"csv/{base}.scores.csv", scores_csv(db, score_set)

    if score_set_has_current_mappings(db, score_set):
        yield f"csv/{base}.annotations.csv", annotations_csv(db, score_set, viewer)
        yield f"vrs/{base}.vrs.ndjson", vrs_ndjson(db, score_set)
        yield f"va/{base}.va.ndjson", va_ndjson(db, score_set, principal)

    if _emit_legacy_mapped_variants_artifact() and score_set_has_legacy_mapped_variants(db, score_set):
        yield f"mapped/{base}.mapped-variants.json", mapped_variants_json(db, score_set)

    counts = counts_csv(db, score_set)
    if counts is not None:
        yield f"csv/{base}.counts.csv", counts


def public_experiment_set(
    experiment_set_view: ExperimentSetPublicDump,
    visible_calibration_ids: set[int],
    readable_superseding_urns: set[str],
) -> Optional[ExperimentSetPublicDump]:
    """Narrow a validated experiment set to what belongs in the public dump.

    Drops calibrations the caller may not READ, blanks a superseding score set the caller may not READ,
    drops experiments left with no score sets, and returns None if no experiments remain. The score sets
    themselves need no filter — the loading query already restricts them to published, CC0-licensed rows.

    Blanking supersession matters because a published score set is often superseded by a *private*
    in-progress replacement; without it, `main.json` would name an unreleased score set's URN and title
    in an archive published to Zenodo, which can't be recalled.

    `readable_superseding_urns` gates on READ, the rule `_score_set_response` applies. It is
    defense-in-depth rather than the live gate, and it can only subtract: `published_experiment_sets`
    loads members through a published+CC0 filter that propagates to `superseding_score_set`, so a
    private replacement is already `None` before this runs — and so is a *published* successor whose
    license keeps it out of the archive. That second case is unresolved: the consumer reads a superseded
    score set as current. Naming it would need the successor fetched by a separate unfiltered query and
    injected into the view, not merely left un-blanked; whether it should be named is an open policy
    question. Both behaviors are pinned by
    `test_supersession_is_named_only_when_the_archive_carries_the_successor`.

    Narrows the validated view rather than the ORM graph because `ExperimentSet.experiments` and
    `ScoreSet.score_calibrations` cascade-delete orphans, and this script can flush (`--commit`).
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
                    ],
                    "superseding_score_set": (
                        score_set_view.superseding_score_set
                        if score_set_view.superseding_score_set is not None
                        and score_set_view.superseding_score_set.urn in readable_superseding_urns
                        else None
                    ),
                }
            )
            for score_set_view in experiment_view.score_sets
        ]
        experiments.append(experiment_view.model_copy(update={"score_sets": score_sets}))

    if not experiments:
        return None

    return experiment_set_view.model_copy(update={"experiments": experiments})


def published_experiment_sets(db: Session) -> list[ExperimentSet]:
    """Every published experiment set, with members narrowed to what the dump may carry.

    Narrowing happens in the loader so an unpublished experiment or non-CC0 score set is never loaded
    onto the graph the metadata view validates from. An experiment set can still end up with no members
    left; `public_experiment_set` drops those.
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

    One function for both: a score set is in the archive exactly when its metadata survives narrowing,
    so deriving the URN list from the narrowed views (not the query) keeps the two in sync.

    Calibration visibility is asked of *principal* rather than inferred from score-set visibility, since
    publishing a score set doesn't publish its calibrations.
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

    # Asked of the ORM graph, not the validated views: a successor is typically outside the loading
    # query's published+CC0 filter, so checking the view would miss it. READ is the gate, but this set
    # only ever subtracts — the loader nulls a non-CC0 successor on the view first, so being readable
    # here is not enough to get one named. See `public_experiment_set`.
    readable_superseding_urns: set[str] = {
        str(score_set_orm.superseding_score_set.urn)
        for score_set_orm in flatmap(lambda es: flatmap(lambda e: e.score_sets, es.experiments), experiment_sets)
        if score_set_orm.superseding_score_set is not None
        and score_set_orm.superseding_score_set.urn is not None
        and has_permission(principal.user_data, score_set_orm.superseding_score_set, Action.READ).permitted
    }

    experiment_set_views = [
        narrowed
        for narrowed in (
            public_experiment_set(
                ExperimentSetPublicDump.model_validate(es), visible_calibration_ids, readable_superseding_urns
            )
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

    Takes the archive rather than a filename so the whole composition can be exercised without touching
    the filesystem.
    """
    metadata, score_set_urns = public_dump_metadata(db, principal)
    archive.writestr("main.json", json.dumps(jsonable_encoder(metadata)))

    resources_dir = os.path.join(os.path.dirname(__file__), "resources")
    archive.write(os.path.join(resources_dir, "CC0_license.txt"), "LICENSE.txt")
    archive.write(os.path.join(resources_dir, "README.md"), "README.md")
    archive.write(os.path.join(resources_dir, "CHANGELOG.md"), "CHANGELOG.md")

    num_score_sets = len(score_set_urns)
    for i, score_set_urn in enumerate(score_set_urns):
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one_or_none()
        if score_set is None:
            # main.json already names this score set, so skip-silently would advertise files it doesn't
            # contain. Reachable only if the row disappears mid-run.
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
