"""add score calibration table

Revision ID: 002f6f9ec7ac
Revises: 019eb75ad9ae
Create Date: 2025-10-08 08:59:10.563528

"""

from typing import Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.dialects import postgresql

from mavedb.models.score_set import ScoreSet
from mavedb.models.score_calibration import ScoreCalibration as ScoreCalibrationDBModel
from mavedb.models.enums.score_calibration_kind import ScoreCalibrationKind
from mavedb.models.publication_identifier import PublicationIdentifier

from mavedb.view_models.score_calibration import ScoreRangeCreate
from mavedb.view_models.score_range import (
    ScoreSetRangesAdminCreate,
    ZeibergCalibrationScoreRangesAdminCreate,
    ScottScoreRangesAdminCreate,
    InvestigatorScoreRangesAdminCreate,
    IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate,
    IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate,
)

# revision identifiers, used by Alembic.
revision = "002f6f9ec7ac"
down_revision = "019eb75ad9ae"
branch_labels = None
depends_on = None


score_range_kinds: dict[
    str,
    Union[
        type[ZeibergCalibrationScoreRangesAdminCreate],
        type[ScottScoreRangesAdminCreate],
        type[InvestigatorScoreRangesAdminCreate],
        type[IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate],
        type[IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate],
    ],
] = {
    "zeiberg_calibration": ZeibergCalibrationScoreRangesAdminCreate,
    "scott_calibration": ScottScoreRangesAdminCreate,
    "investigator_provided": InvestigatorScoreRangesAdminCreate,
    "cvfg_all_variants": IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate,
    "cvfg_missense_variants": IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate,
}


def upgrade_score_ranges():
    conn = op.get_bind()
    session = Session(bind=conn)

    score_sets_with_ranges = (
        session.execute(sa.select(ScoreSet).where(ScoreSet.score_ranges.isnot(None))).scalars().all()
    )

    for score_set in score_sets_with_ranges:
        if not score_set.score_ranges:
            continue

        score_set_ranges = ScoreSetRangesAdminCreate.model_validate(score_set.score_ranges)

        for field in score_set_ranges.model_fields_set:
            if field == "record_type":
                continue

            ranges = getattr(score_set_ranges, field)
            if not ranges:
                continue

            range_model = score_range_kinds.get(field)
            inferred_ranges = range_model.model_validate(ranges)

            model_thresholds = []
            model_odds_paths = {}
            for range in inferred_ranges.ranges:
                model_thresholds.append(ScoreRangeCreate.model_validate(range.__dict__).model_dump())

                if "odds_path" in range.model_fields_set and range.odds_path:
                    model_odds_paths[range.classification] = range.odds_path.model_dump()

            # Reliant on existing behavior that these sources have been created already.
            # If not present, no sources will be associated.
            if "odds_path_source" in inferred_ranges.model_fields_set and inferred_ranges.odds_path_source:
                odds_path_sources = (
                    session.execute(
                        sa.select(PublicationIdentifier).where(
                            PublicationIdentifier.identifier.in_(
                                [src.identifier for src in (inferred_ranges.odds_path_source or [])]
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            else:
                odds_path_sources = []

            if "source" in inferred_ranges.model_fields_set and inferred_ranges.source:
                range_sources = (
                    session.execute(
                        sa.select(PublicationIdentifier).where(
                            PublicationIdentifier.identifier.in_(
                                [src.identifier for src in (inferred_ranges.source or [])]
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            else:
                range_sources = []

            sources = []
            for publication in odds_path_sources:
                setattr(publication, "relation", "odds_paths")
                sources.append(publication)
            for publication in range_sources:
                setattr(publication, "relation", "ranges")
                sources.append(publication)

            score_calibration = ScoreCalibrationDBModel(
                score_set_id=score_set.id,
                title=inferred_ranges.title,
                name=field,
                kind=ScoreCalibrationKind.CLINICAL if field == "zeiberg_calibration" else ScoreCalibrationKind.BRNICH,
                research_use_only=inferred_ranges.research_use_only,
                primary=inferred_ranges.primary,
                investigator_provided=True if field == "investigator_calibration" else False,
                baseline_score=inferred_ranges.baseline_score
                if "baseline_score" in inferred_ranges.model_fields_set
                else None,
                baseline_score_description=inferred_ranges.baseline_score_description
                if "baseline_score_description" in inferred_ranges.model_fields_set
                else None,
                functional_ranges=None if not model_thresholds else model_thresholds,
                odds_paths=None if not model_odds_paths else model_odds_paths,
                calibration_metadata=None,
                publication_identifiers=sources,
            )
            session.add(score_calibration)

    session.commit()
    session.close()


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "score_calibrations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("score_set_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "kind",
            sa.Enum("BRNICH", "CLINICAL", name="scorecalibrationkind", native_enum=False, length=32),
            nullable=False,
        ),
        sa.Column("research_use_only", sa.Boolean(), nullable=False),
        sa.Column("primary", sa.Boolean(), nullable=False),
        sa.Column("investigator_provided", sa.Boolean(), nullable=False),
        sa.Column("baseline_score", sa.Float(), nullable=True),
        sa.Column("baseline_score_description", sa.String(), nullable=True),
        sa.Column("functional_ranges", postgresql.JSONB(astext_type=sa.Text(), none_as_null=True), nullable=True),
        sa.Column("odds_paths", postgresql.JSONB(astext_type=sa.Text(), none_as_null=True), nullable=True),
        sa.Column("calibration_metadata", postgresql.JSONB(astext_type=sa.Text(), none_as_null=True), nullable=True),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(
            ["score_set_id"],
            ["scoresets.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "score_calibration_publication_identifiers",
        sa.Column("score_calibration_id", sa.Integer(), nullable=False),
        sa.Column("publication_identifier_id", sa.Integer(), nullable=False),
        sa.Column(
            "relation",
            sa.Enum("ranges", "odds_paths", name="scorecalibrationrelation", native_enum=False, length=32),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["publication_identifier_id"],
            ["publication_identifiers.id"],
        ),
        sa.ForeignKeyConstraint(
            ["score_calibration_id"],
            ["score_calibrations.id"],
        ),
        sa.PrimaryKeyConstraint("score_calibration_id", "publication_identifier_id"),
    )
    # ### end Alembic commands ###

    upgrade_score_ranges()


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("score_calibration_publication_identifiers")
    op.drop_table("score_calibrations")
    # ### end Alembic commands ###
