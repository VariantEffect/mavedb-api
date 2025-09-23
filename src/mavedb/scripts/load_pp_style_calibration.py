from typing import Callable
import json
import math
import click
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session

from mavedb.scripts.environment import with_database_session
from mavedb.models.score_set import ScoreSet
from mavedb.view_models.score_range import (
    ZeibergCalibrationScoreRangeCreate,
    ZeibergCalibrationScoreRangesCreate,
    ScoreSetRangesCreate,
)

# Evidence strength ordering definitions
PATH_STRENGTHS: List[int] = [1, 2, 3, 4, 8]
BENIGN_STRENGTHS: List[int] = [-1, -2, -3, -4, -8]


def _not_nan(v: Any) -> bool:
    return v is not None and not (isinstance(v, float) and math.isnan(v))


def _collapse_duplicate_thresholds(m: dict[int, Optional[float]], comparator: Callable) -> dict[int, float]:
    collapsed: dict[int, float] = {}

    for strength, threshold in m.items():
        if threshold is None:
            continue

        if threshold in collapsed.values():
            # If the value is already present, we need to find the key it's associated with
            current_strongest_strength = next(s for s, t in collapsed.items() if t == threshold)

            # If the keys are different, we need to merge them. Keep the strongest one as decided
            # by the provided comparator.
            if current_strongest_strength != strength:
                new_strongest_evidence = comparator(current_strongest_strength, strength)
                collapsed.pop(current_strongest_strength)
                collapsed[new_strongest_evidence] = threshold

        else:
            collapsed[strength] = threshold

    return collapsed


def build_pathogenic_ranges(
    thresholds: List[Optional[float]], inverted: bool
) -> List[ZeibergCalibrationScoreRangeCreate]:
    raw_mapping = {
        strength: thresholds[idx]
        for idx, strength in enumerate(PATH_STRENGTHS)
        if idx < len(thresholds) and _not_nan(thresholds[idx])
    }
    mapping = _collapse_duplicate_thresholds(raw_mapping, max)

    # Only retain strengths if they are in the mapping. In inverted mode, upper is 'more pathogenic', which is
    # the opposite of how the pathogenic ranges are given to us. Therefore if the inverted flag is false, we must reverse the
    # order in which we handle ranges.
    available = [s for s in PATH_STRENGTHS if s in mapping]
    ordering = available[::-1] if not inverted else available

    ranges: List[ZeibergCalibrationScoreRangeCreate] = []
    for i, s in enumerate(ordering):
        lower: Optional[float]
        upper: Optional[float]

        if inverted:
            lower = mapping[s]
            upper = mapping[ordering[i + 1]] if i + 1 < len(ordering) else None
        else:
            lower = None if i == 0 else mapping[ordering[i - 1]]
            upper = mapping[s]

        ranges.append(
            ZeibergCalibrationScoreRangeCreate(
                label=str(s),
                classification="abnormal",
                evidence_strength=s,
                range=(lower, upper),
                # Whichever bound interacts with infinity will always be exclusive, with the opposite always inclusive.
                inclusive_lower_bound=False if not inverted else True,
                inclusive_upper_bound=False if inverted else True,
            )
        )
    return ranges


def build_benign_ranges(thresholds: List[Optional[float]], inverted: bool) -> List[ZeibergCalibrationScoreRangeCreate]:
    raw_mapping = {
        strength: thresholds[idx]
        for idx, strength in enumerate(BENIGN_STRENGTHS)
        if idx < len(thresholds) and _not_nan(thresholds[idx])
    }
    mapping = _collapse_duplicate_thresholds(raw_mapping, min)

    # Only retain strengths if they are in the mapping. In inverted mode, lower is 'more normal', which is
    # how the benign ranges are given to us. Therefore if the inverted flag is false, we must reverse the
    # order in which we handle ranges.
    available = [s for s in BENIGN_STRENGTHS if s in mapping]
    ordering = available[::-1] if inverted else available

    ranges: List[ZeibergCalibrationScoreRangeCreate] = []
    for i, s in enumerate(ordering):
        lower: Optional[float]
        upper: Optional[float]

        if not inverted:
            lower = mapping[s]
            upper = mapping[ordering[i + 1]] if i + 1 < len(ordering) else None
        else:
            lower = None if i == 0 else mapping[ordering[i - 1]]
            upper = mapping[s]

        ranges.append(
            ZeibergCalibrationScoreRangeCreate(
                label=str(s),
                classification="normal",
                evidence_strength=s,
                range=(lower, upper),
                # Whichever bound interacts with infinity will always be exclusive, with the opposite always inclusive.
                inclusive_lower_bound=False if inverted else True,
                inclusive_upper_bound=False if not inverted else True,
            )
        )
    return ranges


@click.command()
@with_database_session
@click.argument("json_path", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument("score_set_urn", type=str)
@click.option("--overwrite", is_flag=True, default=False, help="Overwrite existing score_ranges if present.")
def main(db: Session, json_path: str, score_set_urn: str, overwrite: bool) -> None:
    """Load pillar project calibration JSON into a score set's zeiberg_calibration score ranges."""
    score_set: Optional[ScoreSet] = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()
    if not score_set:
        raise click.ClickException(f"Score set with URN {score_set_urn} not found")

    if score_set.score_ranges and score_set.score_ranges["zeiberg_calibration"] and not overwrite:
        raise click.ClickException(
            "pillar project score ranges already present for this score set. Use --overwrite to replace them."
        )

    if not score_set.score_ranges:
        existing_score_ranges = ScoreSetRangesCreate()
    else:
        existing_score_ranges = ScoreSetRangesCreate(**score_set.score_ranges)

    with open(json_path, "r") as fh:
        data: Dict[str, Any] = json.load(fh)

    path_thresholds = data.get("final_pathogenic_thresholds") or []
    benign_thresholds = data.get("final_benign_thresholds") or []
    # Lower is 'more normal' in inverted mode
    inverted = data.get("inverted") == "inverted"

    path_ranges = build_pathogenic_ranges(path_thresholds, inverted)
    benign_ranges = build_benign_ranges(benign_thresholds, inverted)

    if not path_ranges and not benign_ranges:
        raise click.ClickException("No valid thresholds found to build ranges.")

    existing_score_ranges.zeiberg_calibration = ZeibergCalibrationScoreRangesCreate(ranges=path_ranges + benign_ranges)
    score_set.score_ranges = existing_score_ranges.model_dump(exclude_none=True)

    db.add(score_set)
    click.echo(
        f"Loaded {len(path_ranges)} pathogenic and {len(benign_ranges)} benign ranges into score set {score_set_urn} (inverted={inverted})."
    )


if __name__ == "__main__":  # pragma: no cover
    main()
