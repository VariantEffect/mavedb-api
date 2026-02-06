"""
Migration script to convert JSONB functional_ranges to the new row-based implementation.

This script migrates data from ScoreCalibration.functional_ranges (JSONB column) 
to the new ScoreCalibrationFunctionalClassification table with proper foreign key relationships.
"""
from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *
from mavedb.db.session import SessionLocal
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table
)
from mavedb.models.variant import Variant
from mavedb.view_models.acmg_classification import ACMGClassificationCreate

configure_mappers()


def populate_variant_associations(
    db: Session,
    functional_classification: ScoreCalibrationFunctionalClassification,
    calibration: ScoreCalibration,
) -> int:
    """Populate the association table with variants that fall within this functional range."""
    # Create a view model instance to use the existing range checking logic
    if not functional_classification or not functional_classification.range:
        print(f"    Skipping variant association - no valid range or view model")
        return 0
    
    print(f"    Finding variants within range {functional_classification.range} (lower_inclusive={functional_classification.inclusive_lower_bound}, upper_inclusive={functional_classification.inclusive_upper_bound})")
    
    # Get all variants for this score set and their scores
    variants_query = db.execute(sa.select(Variant).where(
        Variant.score_set_id == calibration.score_set_id,
    )).scalars().all()
    
    variants_in_range = []
    total_variants = 0
    
    for variant in variants_query:
        total_variants += 1
        
        # Extract score from JSONB data
        try:
            score_data = variant.data.get("score_data", {}).get("score") if variant.data else None
            if score_data is not None:
                variant_score = float(score_data)
                
                # Use the existing view model method for range checking
                if functional_classification.score_is_contained_in_range(variant_score):
                    variants_in_range.append(variant)
                    
        except (ValueError, TypeError) as e:
            print(f"      Warning: Could not parse score for variant {variant.id}: {e}")
            continue
    
    print(f"    Found {len(variants_in_range)} variants in range out of {total_variants} total variants")
    
    # Bulk insert associations
    if variants_in_range:
        associations = [
            {
                "functional_classification_id": functional_classification.id,
                "variant_id": variant.id
            }
            for variant in variants_in_range
        ]
        
        db.execute(
            score_calibration_functional_classification_variants_association_table.insert(),
            associations
        )
    
    return len(variants_in_range)


def migrate_functional_range_to_row(
    db: Session,
    calibration: ScoreCalibration, 
    functional_range: Dict[str, Any],
    acmg_classification_cache: Dict[str, ACMGClassification]
) -> ScoreCalibrationFunctionalClassification:
    """Convert a single functional range from JSONB to table row."""

    # Handle ACMG classification if present
    acmg_classification_id = None
    acmg_data = functional_range.get("acmg_classification")
    if acmg_data:
        # Create a cache key for the ACMG classification
        criterion = acmg_data.get("criterion").upper() if acmg_data.get("criterion") else None
        evidence_strength = acmg_data.get("evidence_strength").upper() if acmg_data.get("evidence_strength") else None
        points = acmg_data.get("points")

        classification = ACMGClassificationCreate(
            criterion=ACMGCriterion(criterion) if criterion else None,
            evidence_strength=StrengthOfEvidenceProvided(evidence_strength) if evidence_strength else None,
            points=points
        )
        
        cache_key = f"{classification.criterion}_{classification.evidence_strength}_{classification.points}"
        
        if cache_key not in acmg_classification_cache:
            # Create new ACMG classification
            acmg_classification = ACMGClassification(
                criterion=classification.criterion,
                evidence_strength=classification.evidence_strength,
                points=classification.points
            )
            db.add(acmg_classification)
            db.flush()  # Get the ID
            acmg_classification_cache[cache_key] = acmg_classification
        
        acmg_classification_id = acmg_classification_cache[cache_key].id
    
    # Create the functional classification row
    functional_classification = ScoreCalibrationFunctionalClassification(
        calibration_id=calibration.id,
        label=functional_range.get("label", ""),
        description=functional_range.get("description"),
        functional_classification=FunctionalClassification(functional_range.get("classification", "not_specified")),
        range=functional_range.get("range"),
        inclusive_lower_bound=functional_range.get("inclusive_lower_bound"),
        inclusive_upper_bound=functional_range.get("inclusive_upper_bound"),
        oddspaths_ratio=functional_range.get("oddspaths_ratio"),
        positive_likelihood_ratio=functional_range.get("positive_likelihood_ratio"),
        acmg_classification_id=acmg_classification_id
    )
    
    return functional_classification


def do_migration(db: Session):
    """Main migration function."""
    print("Starting migration of JSONB functional_ranges to table rows...")
    
    # Find all calibrations with functional_ranges
    calibrations_with_ranges = db.scalars(
        sa.select(ScoreCalibration).where(ScoreCalibration.functional_ranges_deprecated_json.isnot(None))
    ).all()
    
    print(f"Found {len(calibrations_with_ranges)} calibrations with functional ranges to migrate.")
    
    # Cache for ACMG classifications to avoid duplicates
    acmg_classification_cache: Dict[str, ACMGClassification] = {}
    
    migrated_count = 0
    error_count = 0
    
    for calibration in calibrations_with_ranges:
        try:
            print(f"Migrating calibration {calibration.id} (URN: {calibration.urn})...")
            
            functional_ranges_data = calibration.functional_ranges_deprecated_json
            if not functional_ranges_data or not isinstance(functional_ranges_data, list):
                print(f"  Skipping calibration {calibration.id} - no valid functional ranges data")
                continue
            
            # Create functional classification rows for each range
            functional_classifications = []
            for i, functional_range in enumerate(functional_ranges_data):
                try:
                    functional_classification = migrate_functional_range_to_row(
                        db, calibration, functional_range, acmg_classification_cache
                    )
                    db.add(functional_classification)
                    functional_classifications.append(functional_classification)
                    print(f"  Created functional classification row {i+1}/{len(functional_ranges_data)}")
                    
                except Exception as e:
                    print(f"  Error migrating functional range {i+1} for calibration {calibration.id}: {e}")
                    error_count += 1
                    continue
            
            # Flush to get IDs for the functional classifications
            db.flush()
            
            # Populate variant associations for each functional classification
            total_associations = 0
            for functional_classification in functional_classifications:
                try:
                    associations_count = populate_variant_associations(
                        db, functional_classification, calibration
                    )
                    total_associations += associations_count
                    
                except Exception as e:
                    print(f"  Error populating variant associations for functional classification {functional_classification.id}: {e}")
                    error_count += 1
                    continue
            
            print(f"  Created {total_associations} variant associations")
            
            # Commit the changes for this calibration
            db.commit()
            migrated_count += 1
            print(f"  Successfully migrated calibration {calibration.id}")
            
        except Exception as e:
            print(f"Error migrating calibration {calibration.id}: {e}")
            db.rollback()
            error_count += 1
            continue
    
    # Final statistics
    total_functional_classifications = db.scalar(
        sa.select(sa.func.count(ScoreCalibrationFunctionalClassification.id))
    )
    
    total_associations = db.scalar(
        sa.select(sa.func.count()).select_from(
            score_calibration_functional_classification_variants_association_table
        )
    ) or 0
    
    print(f"\nMigration completed:")
    print(f"  Successfully migrated: {migrated_count} calibrations")
    print(f"  Functional classification rows created: {total_functional_classifications}")
    print(f"  Variant associations created: {total_associations}")
    print(f"  ACMG classifications created: {len(acmg_classification_cache)}")
    print(f"  Errors encountered: {error_count}")


def verify_migration(db: Session):
    """Verify that the migration was successful."""
    print("\nVerifying migration...")
    
    # Count original calibrations with functional ranges
    original_count = db.scalar(
        sa.select(sa.func.count(ScoreCalibration.id)).where(
            ScoreCalibration.functional_ranges_deprecated_json.isnot(None)
        )
    )
    
    # Count migrated functional classifications
    migrated_count = db.scalar(
        sa.select(sa.func.count(ScoreCalibrationFunctionalClassification.id))
    )
    
    # Count ACMG classifications
    acmg_count = db.scalar(
        sa.select(sa.func.count(ACMGClassification.id))
    )
    
    # Count variant associations
    association_count = db.scalar(
        sa.select(sa.func.count()).select_from(
            score_calibration_functional_classification_variants_association_table
        )
    )
    
    print(f"Original calibrations with functional ranges: {original_count}")
    print(f"Migrated functional classification rows: {migrated_count}")
    print(f"ACMG classification records: {acmg_count}")
    print(f"Variant associations created: {association_count}")
    
    # Sample verification - check that relationships work
    sample_classification = db.scalar(
        sa.select(ScoreCalibrationFunctionalClassification).limit(1)
    )
    
    if sample_classification:
        print(f"\nSample verification:")
        print(f"  Functional classification ID: {sample_classification.id}")
        print(f"  Label: {sample_classification.label}")
        print(f"  Classification: {sample_classification.classification}")
        print(f"  Range: {sample_classification.range}")
        print(f"  Calibration ID: {sample_classification.calibration_id}")
        print(f"  ACMG classification ID: {sample_classification.acmg_classification_id}")
        
        # Count variants associated with this classification
        variant_count = db.scalar(
            sa.select(sa.func.count()).select_from(
                score_calibration_functional_classification_variants_association_table
            ).where(
                score_calibration_functional_classification_variants_association_table.c.functional_classification_id == sample_classification.id
            )
        )
        print(f"  Associated variants: {variant_count}")
    
    # Functional classifications by type
    classification_stats = db.execute(
        sa.select(
            ScoreCalibrationFunctionalClassification.classification,
            sa.func.count().label('count')
        ).group_by(ScoreCalibrationFunctionalClassification.classification)
    ).all()
    
    for classification, count in classification_stats:
        print(f"{classification}: {count} ranges")
    


def rollback_migration(db: Session):
    """Rollback the migration by deleting all migrated data."""
    print("Rolling back migration...")
    
    # Count records before deletion
    functional_count = db.scalar(
        sa.select(sa.func.count(ScoreCalibrationFunctionalClassification.id))
    )
    
    acmg_count = db.scalar(
        sa.select(sa.func.count(ACMGClassification.id))
    )
    
    association_count = db.scalar(
        sa.select(sa.func.count()).select_from(
            score_calibration_functional_classification_variants_association_table
        )
    )
    
    # Delete in correct order (associations first, then functional classifications, then ACMG)
    db.execute(sa.delete(score_calibration_functional_classification_variants_association_table))
    db.execute(sa.delete(ScoreCalibrationFunctionalClassification))
    db.execute(sa.delete(ACMGClassification))
    db.commit()
    
    print(f"Deleted {association_count} variant associations")
    print(f"Deleted {functional_count} functional classification rows")
    print(f"Deleted {acmg_count} ACMG classification rows")


def show_usage():
    """Show usage information."""
    print("""
Usage: python migrate_jsonb_ranges_to_table_rows.py [command]

Commands:
  migrate  (default) - Migrate JSONB functional_ranges to table rows
  verify             - Verify migration without running it
  rollback           - Remove all migrated data (destructive!)
  
Examples:
  python migrate_jsonb_ranges_to_table_rows.py          # Run migration
  python migrate_jsonb_ranges_to_table_rows.py verify   # Check status
  python migrate_jsonb_ranges_to_table_rows.py rollback # Undo migration
""")


if __name__ == "__main__":
    import sys
    
    command = sys.argv[1] if len(sys.argv) > 1 else "migrate"
    
    if command == "help" or command == "--help" or command == "-h":
        show_usage()
    elif command == "rollback":
        print("WARNING: This will delete all migrated functional classification data!")
        response = input("Are you sure you want to continue? (y/N): ")
        if response.lower() == 'y':
            with SessionLocal() as db:
                rollback_migration(db)
        else:
            print("Rollback cancelled.")
    elif command == "verify":
        with SessionLocal() as db:
            verify_migration(db)
    elif command == "migrate":
        with SessionLocal() as db:
            do_migration(db)
            verify_migration(db)
    else:
        print(f"Unknown command: {command}")
        show_usage()
