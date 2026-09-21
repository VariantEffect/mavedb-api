"""Utilities for building and mutating score calibration ORM objects."""

import logging
import math
from collections import Counter
from datetime import date
from typing import Optional, Sequence, Union, cast

import pandas as pd
from sqlalchemy import Float, and_, select
from sqlalchemy.orm import Session

from mavedb.lib.acmg import find_or_create_acmg_classification
from mavedb.lib.identifiers import find_or_create_publication_identifier
from mavedb.lib.mondo_ols import resolve_disease_term
from mavedb.lib.types.score_calibrations import (
    CalibrationControlSnapshot,
    CalibrationVariantLinkSnapshot,
    CalibrationVariantRelinkReport,
    ClassificationDict,
    VariantIdentity,
)
from mavedb.lib.validation.constants.general import (
    calibration_class_column_name,
    calibration_variant_column_name,
    hgvs_nt_column,
    hgvs_pro_column,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)
from mavedb.models.score_calibration_publication_identifier import ScoreCalibrationPublicationIdentifierAssociation
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models import score_calibration

logger = logging.getLogger(__name__)


def _fetch_control_variants_by_urn(
    db: Session,
    score_set: ScoreSet,
    controls: Sequence[score_calibration.CalibrationControlCreate],
) -> dict[str, Variant]:
    """Fetch the ``Variant`` rows for a submission's control URNs, scoped to the score set, keyed by URN.

    A single scoped query serves both membership validation (a URN absent from the result does not
    belong to the score set) and row construction, so validating and building a submission share one
    lookup rather than each issuing its own.
    """
    submitted_urns = [control.variant_urn for control in controls]
    return {
        variant.urn: variant
        for variant in db.scalars(
            select(Variant).where(Variant.score_set_id == score_set.id, Variant.urn.in_(submitted_urns))
        ).all()
        # Every returned row matched an IN over the submitted (non-null) URNs, so its urn is non-null;
        # the guard both states that and narrows the key type from Optional[str].
        if variant.urn is not None
    }


def _validate_control_submission(
    controls: Sequence[score_calibration.CalibrationControlCreate],
    variants_by_urn: dict[str, Variant],
) -> None:
    """Duplicate and score-set-membership checks for a control submission, given its resolved variants.

    A control earns its role as calibration evidence from a variant the assay actually scored, so each
    must reference a variant belonging to the calibration's own score set. Duplicate URNs within one
    submission are caught here with a readable message rather than deferred to the
    ``UNIQUE(calibration_id, variant_id)`` database constraint.

    Raises:
        ValidationError: If any control URN is duplicated within the submission, absent from MaveDB,
            or belongs to a different score set.
    """
    counts = Counter(control.variant_urn for control in controls)

    duplicate_urns = {urn for urn, count in counts.items() if count > 1}
    if duplicate_urns:
        raise ValidationError(
            f"Duplicate control variant URNs detected within the submission: {', '.join(sorted(duplicate_urns))}."
        )

    missing_urns = counts.keys() - variants_by_urn.keys()
    if missing_urns:
        raise ValidationError(
            "The following control variants do not belong to the calibration's score set: "
            f"{', '.join(sorted(missing_urns))}."
        )


def validate_calibration_controls_in_score_set(
    db: Session,
    score_set: ScoreSet,
    controls: Optional[Sequence[score_calibration.CalibrationControlCreate]],
) -> list[str]:
    """Validate calibration controls against a score set and return their variant URNs.

    A thin wrapper over :func:`_fetch_control_variants_by_urn` and :func:`_validate_control_submission`
    for callers that only need the validated URNs; :func:`build_calibration_controls` shares the same
    two steps to avoid re-fetching the variants.

    Args:
        db: Database session used for the lookup.
        score_set: The score set the calibration belongs to.
        controls: Controls submitted on create or modify. ``None`` or empty returns ``[]``.

    Returns:
        The submitted and validated variant URNs, in order; empty for ``None``/empty input.

    Raises:
        ValidationError: If any control URN is duplicated within the submission, absent from
            MaveDB, or belongs to a different score set.
    """
    if not controls:
        return []

    variants_by_urn = _fetch_control_variants_by_urn(db, score_set, controls)
    _validate_control_submission(controls, variants_by_urn)
    return [control.variant_urn for control in controls]


def build_calibration_controls(
    db: Session,
    score_set: ScoreSet,
    controls: Optional[Sequence[score_calibration.CalibrationControlCreate]],
    user: User,
) -> list[CalibrationControl]:
    """Validate and construct transient ``CalibrationControl`` rows for a calibration.

    Fetches the submission's ``Variant`` rows once, validates them (duplicates + score-set membership,
    see :func:`_validate_control_submission`), then turns each control into an unattached
    ``CalibrationControl`` with audit fields set. The single fetch is reused for validation and
    construction, so this issues one variant query rather than one to validate and another to build.
    The caller assigns the returned rows to the calibration's ``controls`` collection and commits.
    Returns an empty list for ``None`` or empty input.

    Args:
        db: Database session.
        score_set: The score set the calibration belongs to; controls resolve within it.
        controls: Submitted controls, or ``None``/empty for none.
        user: The acting user, recorded on each control's audit fields.
    """
    if not controls:
        return []

    variants_by_urn = _fetch_control_variants_by_urn(db, score_set, controls)
    _validate_control_submission(controls, variants_by_urn)

    return [
        CalibrationControl(
            variant=variants_by_urn[control.variant_urn],
            clinical_status=control.clinical_status,
            created_by=user,
            modified_by=user,
        )
        for control in controls
    ]


def create_functional_classification(
    db: Session,
    functional_range_create: Union[
        score_calibration.FunctionalClassificationCreate, score_calibration.FunctionalClassificationModify
    ],
    containing_calibration: ScoreCalibration,
    variant_classes: Optional[ClassificationDict] = None,
) -> ScoreCalibrationFunctionalClassification:
    """
    Create a functional classification entity for score calibration.
    This function creates a new ScoreCalibrationFunctionalClassification object
    based on the provided functional range data. It optionally creates or finds
    an associated ACMG classification if one is specified in the input data.

    Args:
        db (Session): Database session for performing database operations.
        functional_range_create (score_calibration.FunctionalClassificationCreate):
            Input data containing the functional range parameters including label,
            description, range bounds, inclusivity flags, and optional ACMG
            classification information.
        containing_calibration (ScoreCalibration): The ScoreCalibration instance.
        variant_classes (Optional[ClassificationDict]): Optional dictionary mapping variant classes
            to their corresponding variant identifiers.

    Returns:
        ScoreCalibrationFunctionalClassification: The newly created functional
            classification entity that has been added to the database session.

    Note:
        The function adds the created functional classification to the database
        session but does not commit the transaction. The caller is responsible
        for committing the changes.
    """
    acmg_classification = None
    if functional_range_create.acmg_classification:
        acmg_classification = find_or_create_acmg_classification(
            db,
            criterion=functional_range_create.acmg_classification.criterion,
            evidence_strength=functional_range_create.acmg_classification.evidence_strength,
            points=functional_range_create.acmg_classification.points,
        )
    else:
        acmg_classification = None

    functional_classification = ScoreCalibrationFunctionalClassification(
        label=functional_range_create.label,
        description=functional_range_create.description,
        range=functional_range_create.range,
        class_=functional_range_create.class_,
        inclusive_lower_bound=functional_range_create.inclusive_lower_bound,
        inclusive_upper_bound=functional_range_create.inclusive_upper_bound,
        acmg_classification=acmg_classification,  # type: ignore[arg-type]
        functional_classification=functional_range_create.functional_classification,
        oddspaths_ratio=functional_range_create.oddspaths_ratio,  # type: ignore[arg-type]
        positive_likelihood_ratio=functional_range_create.positive_likelihood_ratio,  # type: ignore[arg-type]
        acmg_classification_id=acmg_classification.id if acmg_classification else None,
        calibration=containing_calibration,
    )

    contained_variants = variants_for_functional_classification(
        db, functional_classification, variant_classes=variant_classes, use_sql=True
    )
    functional_classification.variants = contained_variants

    return functional_classification


async def _create_score_calibration(
    db: Session,
    calibration_create: score_calibration.ScoreCalibrationCreate,
    user: User,
    variant_classes: Optional[ClassificationDict] = None,
    containing_score_set: Optional[ScoreSet] = None,
) -> ScoreCalibration:
    """
    Create a ScoreCalibration ORM instance (not yet persisted) together with its
    publication identifier associations.

    For each publication source listed in the incoming ScoreCalibrationCreate model
    (threshold_sources, evidence_sources, method_sources), this function
    ensures a corresponding PublicationIdentifier row exists (via
    find_or_create_publication_identifier) and creates a
    ScoreCalibrationPublicationIdentifierAssociation that links the identifier to
    the new calibration under the appropriate relation type
    (ScoreCalibrationRelation.threshold / .evidence / .method).

    Fields in calibration_create that represent source lists or audit metadata
    (threshold_sources, evidence_sources, method_sources, created_at,
    created_by, modified_at, modified_by) are excluded when instantiating the
    ScoreCalibration; audit fields created_by and modified_by are explicitly set
    from the provided user_data. The resulting ScoreCalibration object includes
    the assembled publication_identifier_associations collection but is not added
    to the session nor committed—callers are responsible for persisting it.

    Parameters
    ----------
    db : Session
        SQLAlchemy database session used to look up or create publication
        identifiers.
    calibration_create : score_calibration.ScoreCalibrationCreate
        Pydantic (or similar) schema containing the calibration attributes and
        optional lists of publication source identifiers grouped by relation type.
    user : User
        Authenticated user context; the user to be recorded for audit
    variant_classes (Optional[ClassificationDict]):
        Optional dictionary mapping variant classes to their corresponding variant identifiers.
    containing_score_set : Optional[ScoreSet]
        If provided, the ScoreSet instance to which the new calibration will belong.

    Returns
    -------
    ScoreCalibration
        A new, transient ScoreCalibration ORM instance populated with associations
        to publication identifiers and audit metadata set.

    Side Effects
    ------------
    May read from or write to the database when resolving publication identifiers
    (via find_or_create_publication_identifier). Does not flush, add, or commit the
    returned calibration instance.

    Notes
    -----
    - Duplicate identifiers across different source lists result in distinct
      association objects (no deduplication is performed here).
    - The function is async because it awaits the underlying publication
      identifier retrieval/creation calls.
    """
    relation_sources = (
        (ScoreCalibrationRelation.threshold, calibration_create.threshold_sources or []),
        (ScoreCalibrationRelation.evidence, calibration_create.evidence_sources or []),
        (ScoreCalibrationRelation.method, calibration_create.method_sources or []),
    )

    calibration_pub_assocs = []
    for relation, sources in relation_sources:
        for identifier in sources:
            pub = await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            calibration_pub_assocs.append(
                ScoreCalibrationPublicationIdentifierAssociation(
                    publication=pub,
                    relation=relation,
                )
            )

            # Ensure newly created publications are persisted for future loops to avoid duplicates.
            db.add(pub)
            db.flush()

    calibration = ScoreCalibration(
        **calibration_create.model_dump(
            by_alias=False,
            exclude={
                "functional_classifications",
                "controls",
                "disease",
                "threshold_sources",
                "evidence_sources",
                "method_sources",
                "score_set_urn",
            },
        ),
        publication_identifier_associations=calibration_pub_assocs,
        functional_classifications=[],
        created_by=user,
        modified_by=user,
    )  # type: ignore[call-arg]

    calibration.disease_term = await resolve_disease_term(db, getattr(calibration_create, "disease", None))

    if containing_score_set:
        calibration.score_set = containing_score_set
        calibration.score_set_id = containing_score_set.id

    for functional_range_create in calibration_create.functional_classifications or []:
        persisted_functional_range = create_functional_classification(
            db, functional_range_create, containing_calibration=calibration, variant_classes=variant_classes
        )
        db.add(persisted_functional_range)
        calibration.functional_classifications.append(persisted_functional_range)

    return calibration


async def create_score_calibration_in_score_set(
    db: Session,
    calibration_create: score_calibration.ScoreCalibrationCreate,
    user: User,
    variant_classes: Optional[ClassificationDict] = None,
) -> ScoreCalibration:
    """
    Create a new score calibration and associate it with an existing score set.

    This coroutine ensures that the provided ScoreCalibrationCreate payload includes a
    score_set_urn, loads the corresponding ScoreSet from the database, delegates creation
    of the ScoreCalibration to an internal helper, and then links the created calibration
    to the fetched score set.

    Parameters:
        db (Session): An active SQLAlchemy session used for database access.
        calibration_create (score_calibration.ScoreCalibrationCreate): Pydantic (or schema)
            object containing the fields required to create a score calibration. Must include
            a non-empty score_set_urn.
        user (User): Authenticated user information used for auditing
        variant_classes (Optional[ClassificationDict]): Optional dictionary mapping variant classes
            to their corresponding variant identifiers.

    Returns:
        ScoreCalibration: The newly created and persisted score calibration object with its
        score_set relationship populated.

    Raises:
        ValueError: If calibration_create.score_set_urn is missing or falsy.
        sqlalchemy.orm.exc.NoResultFound: If no ScoreSet exists with the provided URN.
        sqlalchemy.orm.exc.MultipleResultsFound: If multiple ScoreSets share the provided URN
            (should not occur if URNs are unique).

    Notes:
        - This function is async because it awaits the internal _create_score_calibration
          helper, which may perform asynchronous operations (e.g., I/O or async ORM tasks).
        - The passed Session is expected to be valid for the lifetime of this call; committing
          or flushing is assumed to be handled externally (depending on the surrounding
          transaction management strategy).
    """
    if not calibration_create.score_set_urn:
        raise ValueError("score_set_urn must be provided to create a score calibration within a score set.")

    containing_score_set = db.query(ScoreSet).where(ScoreSet.urn == calibration_create.score_set_urn).one()
    calibration = await _create_score_calibration(db, calibration_create, user, variant_classes, containing_score_set)

    if user.username in [contributor.orcid_id for contributor in containing_score_set.contributors] + [
        containing_score_set.created_by.username,
        containing_score_set.modified_by.username,
    ]:
        calibration.investigator_provided = True
    else:
        calibration.investigator_provided = False

    calibration.controls = build_calibration_controls(
        db, containing_score_set, getattr(calibration_create, "controls", None), user
    )

    db.add(calibration)
    return calibration


async def create_score_calibration(
    db: Session,
    calibration_create: score_calibration.ScoreCalibrationCreate,
    user: User,
    variant_classes: Optional[ClassificationDict] = None,
) -> ScoreCalibration:
    """
    Asynchronously create and persist a new ScoreCalibration record.

    This is a thin wrapper that delegates to the internal _create_score_calibration
    implementation, allowing for separation of public API and internal logic.

    Parameters
    ----------
    db : sqlalchemy.orm.Session
        Active database session used for persisting the new calibration.
    calibration_create : score_calibration.ScoreCalibrationCreate
        Pydantic (or similar) schema instance containing the data required to
        instantiate a ScoreCalibration (e.g., method, parameters, target assay /
        score set identifiers).
    user : User
        Authenticated user context; the user to be recorded for audit
    variant_classes (Optional[ClassificationDict]): Optional dictionary mapping variant classes
        to their corresponding variant identifiers.

    Returns
    -------
    ScoreCalibration
        The newly created (but un-added and un-committed) ScoreCalibration
        ORM/model instance.

    Raises
    ------
    IntegrityError
        If database constraints (e.g., uniqueness, foreign keys) are violated.
    AuthorizationError
        If the provided user does not have permission to create the calibration.
    ValidationError
        If the supplied input schema fails validation (depending on schema logic).
    ValueError
        If calibration_create.score_set_urn is provided (must be None/absent here).

    Notes
    -----
    - Because this function is asynchronous, callers must await it. Any transaction
      management (commit / rollback) is expected to be handled by the session lifecycle
      manager in the calling context.
    - Because the calibration database model enforces that a calibration must belong
      to a ScoreSet, callers should perform this association themselves after creation
      (e.g., by assigning the calibration's score_set attribute to an existing ScoreSet
      instance) prior to flushing the session.
    """
    if calibration_create.score_set_urn:
        raise ValueError("score_set_urn must not be provided to create a score calibration outside a score set.")

    created_calibration = await _create_score_calibration(
        db, calibration_create, user, variant_classes, containing_score_set=None
    )

    db.add(created_calibration)
    return created_calibration


async def modify_score_calibration(
    db: Session,
    calibration: ScoreCalibration,
    calibration_update: score_calibration.ScoreCalibrationModify,
    user: User,
    variant_classes: Optional[ClassificationDict] = None,
) -> ScoreCalibration:
    """
    Asynchronously modify an existing ScoreCalibration record and its related publication
    identifier associations.

    This function:
    1. Validates that a score_set_urn is provided in the update model (raises ValueError if absent).
    2. Loads (via SELECT ... WHERE urn = :score_set_urn) the ScoreSet that will contain the calibration.
    3. Reconciles publication identifier associations for three relation categories:
        - threshold_sources  -> ScoreCalibrationRelation.threshold
        - evidence_sources -> ScoreCalibrationRelation.evidence
        - method_sources -> ScoreCalibrationRelation.method
        For each provided source identifier:
          * Calls find_or_create_publication_identifier to obtain (or persist) the identifier row.
          * Preserves an existing association if already present.
          * Creates a new association if missing.
        Any previously existing associations not referenced in the update are deleted from the session.
    4. Updates mutable scalar fields on the calibration instance from calibration_update, excluding:
        threshold_sources, evidence_sources, method_sources, created_at, created_by,
        modified_at, modified_by.
    5. Reassigns the calibration to the resolved ScoreSet, replaces its association collection,
        and stamps modified_by with the requesting user.
    6. Adds the modified calibration back into the SQLAlchemy session and returns it (no commit).

    Parameters
    ----------
     db : Session
         An active SQLAlchemy session (synchronous engine session used within an async context).
     calibration : ScoreCalibration
         The existing calibration ORM instance to be modified (must be persistent or pending).
     calibration_update : score_calibration.ScoreCalibrationModify
         - score_set_urn (required)
         - threshold_sources, evidence_sources, method_sources (iterables of identifier objects)
         - Additional mutable calibration attributes.
     user : User
         Context for the authenticated user; the user to be recorded for audit.
     variant_classes (Optional[ClassificationDict]): Optional dictionary mapping variant classes
         to their corresponding variant identifiers.

    Returns
    -------
    ScoreCalibration
         The in-memory (and session-added) updated calibration instance. Changes are not committed.

    Raises
    ------
    ValueError
         If score_set_urn is missing in the update model.
    sqlalchemy.orm.exc.NoResultFound
         If no ScoreSet exists with the provided URN.
    sqlalchemy.orm.exc.MultipleResultsFound
         If more than one ScoreSet matches the provided URN.
    Any exception raised by find_or_create_publication_identifier
         If identifier resolution/creation fails.

    Side Effects
    ------------
    - Issues SELECT statements for the ScoreSet and publication identifiers.
    - May INSERT new publication identifiers and association rows.
    - May DELETE association rows no longer referenced.
    - Mutates the provided calibration object in-place.

    Concurrency / Consistency Notes
    -------------------------------
    The reconciliation of associations assumes no concurrent modification of the same calibration's
    association set within the active transaction. To prevent races leading to duplicate associations,
    enforce appropriate transaction isolation or unique constraints at the database level.

    Commit Responsibility
    ---------------------
    This function does NOT call commit or flush explicitly; the caller is responsible for committing
    the session to persist changes.

    """
    if not calibration_update.score_set_urn:
        raise ValueError("score_set_urn must be provided to modify a score calibration.")

    containing_score_set = db.query(ScoreSet).where(ScoreSet.urn == calibration_update.score_set_urn).one()

    relation_sources = (
        (ScoreCalibrationRelation.threshold, calibration_update.threshold_sources or []),
        (ScoreCalibrationRelation.evidence, calibration_update.evidence_sources or []),
        (ScoreCalibrationRelation.method, calibration_update.method_sources or []),
    )

    # Build a map of existing associations by (relation, publication_identifier_id) for easy lookup.
    existing_assocs_map = {
        (assoc.relation, assoc.publication_identifier_id): assoc
        for assoc in calibration.publication_identifier_associations
    }

    updated_assocs = []
    for relation, sources in relation_sources:
        for identifier in sources:
            pub = await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            assoc_key = (relation, pub.id)
            if assoc_key in existing_assocs_map:
                # Keep existing association
                updated_assocs.append(existing_assocs_map.pop(assoc_key))
            else:
                # Create new association
                updated_assocs.append(
                    ScoreCalibrationPublicationIdentifierAssociation(
                        publication=pub,
                        relation=relation,
                    )
                )

            # Ensure newly created publications are persisted for future loops to avoid duplicates.
            db.add(pub)
            db.flush()

    # Remove associations and calibrations that are no longer present
    for assoc in existing_assocs_map.values():
        db.delete(assoc)
    for functional_classification in calibration.functional_classifications:
        db.delete(functional_classification)
    calibration.functional_classifications.clear()

    db.flush()
    db.refresh(calibration)

    for attr, value in calibration_update.model_dump().items():
        if attr not in {
            "functional_classifications",
            "controls",
            "disease",
            # controls_not_phi carries re-acknowledgment semantics; set it explicitly so an
            # unrelated edit cannot silently wipe a prior affirmation.
            "controls_not_phi",
            "threshold_sources",
            "evidence_sources",
            "method_sources",
            "created_at",
            "created_by",
            "modified_at",
            "modified_by",
            "score_set_urn",
        }:
            setattr(calibration, attr, value)

    calibration.disease_term = await resolve_disease_term(db, getattr(calibration_update, "disease", None))

    calibration.score_set = containing_score_set
    calibration.score_set_id = containing_score_set.id
    calibration.publication_identifier_associations = updated_assocs
    calibration.modified_by = user

    for functional_range_update in calibration_update.functional_classifications or []:
        persisted_functional_range = create_functional_classification(
            db, functional_range_update, variant_classes=variant_classes, containing_calibration=calibration
        )
        db.add(persisted_functional_range)
        calibration.functional_classifications.append(persisted_functional_range)

    # Replace semantics: a provided controls list (even empty) replaces all existing controls, while
    # None leaves them untouched.
    submitted_controls = getattr(calibration_update, "controls", None)
    if submitted_controls is not None:
        for control in list(calibration.controls):
            db.delete(control)
        calibration.controls.clear()
        db.flush()
        calibration.controls = build_calibration_controls(db, containing_score_set, submitted_controls, user)

    # Re-acknowledgment: an explicit controls_not_phi in the request always wins; otherwise, replacing the
    # controls invalidates any prior affirmation, while leaving the controls untouched preserves it.
    if "controls_not_phi" in calibration_update.model_fields_set:
        calibration.controls_not_phi = calibration_update.controls_not_phi
    elif submitted_controls is not None:
        calibration.controls_not_phi = None

    db.add(calibration)
    return calibration


def publish_score_calibration(db: Session, calibration: ScoreCalibration, user: User) -> ScoreCalibration:
    """Publish a private ScoreCalibration, marking it as publicly accessible.

    Parameters
    ----------
    db : Session
        Active SQLAlchemy session used to stage the update.
    calibration : ScoreCalibration
        The calibration instance to publish. Must currently be private.
    user : User
        The user performing the publish action; recorded in `modified_by`.

    Returns
    -------
    ScoreCalibration
        The updated calibration instance with `private` set to False.

    Raises
    ------
    ValueError
        If the calibration is already published (i.e., `private` is False).

    Notes
    -----
    This function adds the modified calibration to the session but does not commit;
    the caller is responsible for committing the transaction.
    """
    if not calibration.private:
        raise ValueError("Calibration is already published.")

    calibration.private = False
    calibration.modified_by = user

    db.add(calibration)
    return calibration


def promote_score_calibration_to_primary(
    db: Session, calibration: ScoreCalibration, user: User, force: bool = False
) -> ScoreCalibration:
    """
    Promote a non-primary score calibration to be the primary calibration for its score set.

    This function enforces several business rules before promotion:
    1. The calibration must not already be primary.
    2. It must not be marked as research-use-only.
    3. It must not be private.
    4. If another primary calibration already exists for the same score set, promotion is blocked
        unless force=True is provided. When force=True, any existing primary calibration(s) are
        demoted (their primary flag set to False) and updated with the acting user.

    Parameters:
         db (Session): An active SQLAlchemy session used for querying and persisting changes.
         calibration (ScoreCalibration): The calibration object to promote.
         user (User): The user performing the promotion; recorded as the modifier.
         force (bool, optional): If True, override an existing primary calibration by demoting it.
              Defaults to False.

    Returns:
         ScoreCalibration: The updated calibration instance now marked as primary.

    Raises:
         ValueError:
              - If the calibration is already primary.
              - If the calibration is research-use-only.
              - If the calibration is private.
              - If another primary calibration exists for the score set and force is False.

    Side Effects:
         - Marks the provided calibration as primary and updates its modified_by field.
         - When force=True, demotes any existing primary calibration(s) in the same score set.

    Notes:
         - The caller is responsible for committing the transaction after this function returns.
         - Multiple existing primary calibrations (should not normally occur) are all demoted if force=True.
    """
    if calibration.primary:
        raise ValueError("Calibration is already primary.")

    if calibration.research_use_only:
        raise ValueError("Cannot promote a research use only calibration to primary.")

    if calibration.private:
        raise ValueError("Cannot promote a private calibration to primary.")

    existing_primary_calibrations = (
        db.query(ScoreCalibration)
        .filter(
            ScoreCalibration.score_set_id == calibration.score_set_id,
            ScoreCalibration.primary.is_(True),
            ScoreCalibration.id != calibration.id,
        )
        .all()
    )

    if existing_primary_calibrations and not force:
        raise ValueError("Another primary calibration already exists for this score set. Use force=True to override.")
    elif force:
        for primary_calibration in existing_primary_calibrations:
            primary_calibration.primary = False
            primary_calibration.modified_by = user
            db.add(primary_calibration)

    calibration.primary = True
    calibration.modified_by = user

    db.add(calibration)
    return calibration


def demote_score_calibration_from_primary(db: Session, calibration: ScoreCalibration, user: User) -> ScoreCalibration:
    """
    Demote a score calibration from primary status.

    This function marks the provided ScoreCalibration instance as non-primary by
    setting its `primary` attribute to False and updating its `modified_by` field
    with the acting user. The updated calibration is added to the SQLAlchemy session
    but the session is not committed; callers are responsible for committing or
    rolling back the transaction.

    Parameters:
        db (Session): An active SQLAlchemy session used to persist the change.
        calibration (ScoreCalibration): The score calibration object currently marked as primary.
        user (User): The user performing the operation; recorded in `modified_by`.

    Returns:
        ScoreCalibration: The updated calibration instance with `primary` set to False.

    Raises:
        ValueError: If the provided calibration is not currently marked as primary.
    """
    if not calibration.primary:
        raise ValueError("Calibration is not primary.")

    calibration.primary = False
    calibration.modified_by = user

    db.add(calibration)
    return calibration


def delete_score_calibration(db: Session, calibration: ScoreCalibration) -> None:
    """
    Delete a non-primary score calibration record from the database.

    This function removes the provided ScoreCalibration instance from the SQLAlchemy
    session. Primary calibrations are protected from deletion and must be demoted
    (i.e., have their `primary` flag unset) before they can be deleted.

    Parameters:
        db (Session): An active SQLAlchemy session used to perform the delete operation.
        calibration (ScoreCalibration): The calibration object to be deleted.

    Raises:
        ValueError: If the calibration is marked as primary.

    Returns:
        None
    """
    if calibration.primary:
        raise ValueError("Cannot delete a primary calibration. Demote it first.")

    db.delete(calibration)
    return None


def variants_for_functional_classification(
    db: Session,
    functional_classification: ScoreCalibrationFunctionalClassification,
    variant_classes: Optional[ClassificationDict] = None,
    use_sql: bool = False,
) -> list[Variant]:
    """
    Return variants in the parent score set whose numeric score falls inside the
    functional classification's range.

    The variant score is extracted from the JSONB ``Variant.data`` field using
    ``score_json_path`` (default: ("score_data", "score") meaning
    ``variant.data['score_data']['score']``). The classification's existing
    ``score_is_contained_in_range`` method is used for interval logic, including
    inclusive/exclusive behaviors.

    Parameters
    ----------
    db : Session
        Active SQLAlchemy session.
    functional_classification : ScoreCalibrationFunctionalClassification
        The ORM row defining the interval to test against.
    variant_classes : Optional[ClassificationDict]
        If provided, a dictionary mapping variant classes to their corresponding variant identifiers
        to use for classification rather than the range property of the functional_classification.
    use_sql : bool
        When True, perform filtering in the database using JSONB extraction and
        range predicates; falls back to Python filtering if an error occurs.

    Returns
    -------
    list[Variant]
        Variants whose score falls within the specified range. Empty list if
        classification has no usable range.

    Notes
    -----
    * If use_sql=False (default) filtering occurs in Python after loading all
      variants for the score set. For large sets set use_sql=True to push
      comparison into Postgres.
    * Variants lacking a score or with non-numeric scores are skipped.
    * If ``functional_classification.range`` is ``None`` an empty list is
      returned immediately.
    """
    # Resolve score set id from attached calibration (relationship may be lazy)
    score_set_id = functional_classification.calibration.score_set_id  # type: ignore[attr-defined]

    if variant_classes and variant_classes["indexed_by"] not in [
        hgvs_nt_column,
        hgvs_pro_column,
        calibration_variant_column_name,
    ]:
        raise ValueError(f"Unsupported index column `{variant_classes['indexed_by']}` for variant classification.")

    if use_sql:
        try:
            # Build score extraction expression: data['score_data']['score']::text::float
            score_expr = Variant.data["score_data"]["score"].astext.cast(Float)

            conditions = [Variant.score_set_id == score_set_id]
            if variant_classes is not None and functional_classification.class_ is not None:
                index_element = variant_classes["classifications"].get(functional_classification.class_, set())

                if variant_classes["indexed_by"] == hgvs_nt_column:
                    conditions.append(Variant.hgvs_nt.in_(index_element))
                elif variant_classes["indexed_by"] == hgvs_pro_column:
                    conditions.append(Variant.hgvs_pro.in_(index_element))
                elif variant_classes["indexed_by"] == calibration_variant_column_name:
                    conditions.append(Variant.urn.in_(index_element))
                else:  # pragma: no cover
                    return []

            elif functional_classification.range is not None and len(functional_classification.range) == 2:
                lower_raw, upper_raw = functional_classification.range

                # Convert 'inf' sentinels (or None) to float infinities for condition omission.
                lower_bound = inf_or_float(lower_raw, lower=True)
                upper_bound = inf_or_float(upper_raw, lower=False)

                if not math.isinf(lower_bound):
                    if functional_classification.inclusive_lower_bound:
                        conditions.append(score_expr >= lower_bound)
                    else:
                        conditions.append(score_expr > lower_bound)
                if not math.isinf(upper_bound):
                    if functional_classification.inclusive_upper_bound:
                        conditions.append(score_expr <= upper_bound)
                    else:
                        conditions.append(score_expr < upper_bound)

            else:
                # No usable classification mechanism; return empty list.
                return []

            stmt = select(Variant).where(and_(*conditions))
            return list(db.execute(stmt).scalars())

        except Exception:  # noqa: BLE001
            # Fall back to Python filtering if casting/JSON path errors occur.
            pass

    # Python filtering fallback / default path
    variants = db.execute(select(Variant).where(Variant.score_set_id == score_set_id)).scalars().all()
    matches: list[Variant] = []
    for v in variants:
        if variant_classes is not None and functional_classification.class_ is not None:
            index_element = variant_classes["classifications"].get(functional_classification.class_, set())

            if variant_classes["indexed_by"] == hgvs_nt_column:
                if v.hgvs_nt in index_element:
                    matches.append(v)
            elif variant_classes["indexed_by"] == hgvs_pro_column:
                if v.hgvs_pro in index_element:
                    matches.append(v)
            elif variant_classes["indexed_by"] == calibration_variant_column_name:
                if v.urn in index_element:
                    matches.append(v)
            else:  # pragma: no cover
                continue

        elif functional_classification.range is not None and len(functional_classification.range) == 2:
            try:
                container = v.data.get("score_data") if isinstance(v.data, dict) else None
                if not container or not isinstance(container, dict):
                    continue

                raw = container.get("score")
                if raw is None:
                    continue

                score = float(raw)

            except Exception:  # noqa: BLE001
                continue

            if functional_classification.score_is_contained_in_range(score):
                matches.append(v)

    return matches


def variant_classification_df_to_dict(
    df: pd.DataFrame,
    index_column: str,
) -> ClassificationDict:
    """
    Convert a DataFrame of variant classifications into a dictionary mapping
    functional class labels to lists of distinct variant URNs.

    The input DataFrame is expected to have at least two columns:
    - The unique identifier for each variant (given by calibration_variant_column_name).
    - The functional classification label for each variant (given by calibration_class_column_name).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing variant classifications with 'variant_urn' and
        'functional_class' columns.

    Returns
    -------
    ClassificationDict
        A dictionary with two keys: 'indexed_by' indicating the index column name,
        and 'classifications' mapping each functional class label to a list of
        distinct variant URNs.
    """
    classifications: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        index_element = row[index_column]
        functional_class = row[calibration_class_column_name]

        if functional_class not in classifications:
            classifications[functional_class] = set()

        classifications[functional_class].add(index_element)

    return {"indexed_by": index_column, "classifications": classifications}


def snapshot_calibration_variant_links(db: Session, score_set: ScoreSet) -> list[CalibrationVariantLinkSnapshot]:
    """Record the calibration variant references a re-upload cannot reconstruct on its own.

    Re-uploading a score set's data deletes and recreates all of its ``Variant`` rows, which breaks
    the foreign keys calibrations hold into them: ``calibration_controls.variant_id`` and the
    functional-classification membership association. Neither carries an ``ON DELETE`` action, on
    purpose — the resulting ``RESTRICT`` protects hand-entered controls from being destroyed by an
    unrelated delete. Capturing the irreproducible references as :data:`VariantIdentity` tuples before
    the delete lets :func:`restore_calibration_variant_links` re-point the survivors afterwards.
    Variant URNs cannot serve as the key because they are renumbered on every upload.

    The rule for what to capture is whether the depositor asserted the reference or MaveDB derived it:

    * **Controls** carry clinical significance sourced from outside MaveDB, submitted inline or as a
      ``controls_file``. Nothing in a score upload can regenerate them, so they are remembered.
    * **Class-based bin membership** comes from the ``classes_file`` required of a class-based
      calibration. That file is not part of a score upload either, so it too survives only by identity.
    * **Range-based bin membership** is derived — purely a function of the variants' scores. It is
      left out and recomputed from the new upload instead, which avoids holding an entry per variant
      in memory and keeps membership honest when a re-upload moves a score across a threshold.

    The queries scope to variants in ``score_set``, which keeps the snapshot aligned with exactly the
    rows the caller deletes. Every reference is already so scoped — controls are validated against the
    calibration's own score set and bin membership is only ever populated from it — so the condition
    holds the two halves together rather than filtering anything out. Were they to diverge, restore
    would re-create a row whose original was never deleted and trip the unique constraint on
    ``(calibration_id, variant_id)``.

    This function only reads. The caller performs the deletes.

    Args:
        db: Database session.
        score_set: The score set whose variants are about to be replaced.

    Returns:
        One snapshot per calibration holding at least one irreproducible reference; empty when no
        calibration has one. A score set whose calibrations use only range-based bins yields an empty
        list and still needs :func:`restore_calibration_variant_links` called to re-bin them.
    """
    calibration_ids = [calibration.id for calibration in score_set.score_calibrations]
    if not calibration_ids:
        return []

    snapshots: dict[int, CalibrationVariantLinkSnapshot] = {}

    control_rows = db.execute(
        select(
            CalibrationControl.calibration_id,
            CalibrationControl.clinical_status,
            CalibrationControl.created_by_id,
            CalibrationControl.creation_date,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
        )
        .join(Variant, Variant.id == CalibrationControl.variant_id)
        .where(CalibrationControl.calibration_id.in_(calibration_ids), Variant.score_set_id == score_set.id)
    ).all()

    for calibration_id, clinical_status, created_by_id, creation_date, hgvs_nt, hgvs_pro, hgvs_splice in control_rows:
        snapshot = snapshots.setdefault(calibration_id, CalibrationVariantLinkSnapshot(calibration_id=calibration_id))
        snapshot.controls.append(
            CalibrationControlSnapshot(
                identity=(hgvs_nt, hgvs_pro, hgvs_splice),
                clinical_status=clinical_status,
                created_by_id=created_by_id,
                creation_date=creation_date,
            )
        )

    association = score_calibration_functional_classification_variants_association_table
    membership_rows = db.execute(
        select(
            ScoreCalibrationFunctionalClassification.calibration_id,
            ScoreCalibrationFunctionalClassification.id,
            Variant.hgvs_nt,
            Variant.hgvs_pro,
            Variant.hgvs_splice,
        )
        .join(association, association.c.functional_classification_id == ScoreCalibrationFunctionalClassification.id)
        .join(Variant, Variant.id == association.c.variant_id)
        .where(
            ScoreCalibrationFunctionalClassification.calibration_id.in_(calibration_ids),
            ScoreCalibrationFunctionalClassification.class_.is_not(None),
            Variant.score_set_id == score_set.id,
        )
    ).all()

    for calibration_id, classification_id, hgvs_nt, hgvs_pro, hgvs_splice in membership_rows:
        snapshot = snapshots.setdefault(calibration_id, CalibrationVariantLinkSnapshot(calibration_id=calibration_id))
        snapshot.classification_members.setdefault(classification_id, []).append((hgvs_nt, hgvs_pro, hgvs_splice))

    return list(snapshots.values())


def restore_calibration_variant_links(
    db: Session,
    score_set: ScoreSet,
    snapshots: Sequence[CalibrationVariantLinkSnapshot],
    updater: User,
) -> CalibrationVariantRelinkReport:
    """Re-establish a score set's calibration variant references after its variants are recreated.

    Each calibration is handled by the two mechanisms its references call for:

    * **Controls and class-based bin membership** are carried across by
      :data:`VariantIdentity`. A reference whose identity is absent from the new upload describes a
      variant the assay no longer scores, so it is dropped rather than guessed at; an identity
      matching more than one new variant is ambiguous and dropped for the same reason. Dropping
      rather than failing is deliberate: a re-upload that aborted on a vanished control would leave
      depositors unable to correct their own data.
    * **Range-based bin membership** is recomputed from the new scores via
      :func:`variants_for_functional_classification`, the same helper that populated it originally.
      Carrying the old membership across might leave a variant filed under a range its new score no
      longer falls in, so the recorded membership would simply be wrong.

    A classification always has exactly one of ``range`` or ``class_`` set (enforced by the view
    models), so every classification falls squarely into one branch or the other.

    Dropping a control changes the control set the submitter affirmed as free of protected health
    information, so ``controls_not_phi`` is reset to ``None`` on that calibration — the same
    re-acknowledgment rule applied when controls are replaced through the API (see
    :func:`modify_score_calibration`). A calibration whose controls all relink keeps its affirmation.
    Re-binning does not reset it: bin membership carries no clinical annotation and so no PHI.

    Changes are staged on the session; the caller commits.

    Args:
        db: Database session.
        score_set: The score set whose variants have just been recreated.
        snapshots: Output of :func:`snapshot_calibration_variant_links`, taken before the delete. May
            be empty while calibrations still need re-binning.
        updater: The user who triggered the re-upload, recorded on the rows this relink touches.

    Returns:
        Counts of relinked, dropped and re-binned references, for the caller's job log.
    """
    report = CalibrationVariantRelinkReport()
    if not score_set.score_calibrations:
        return report

    # Only controls and class-based bin membership are re-resolved by identity; range-based bins are
    # recomputed from the new scores below and never touch this map. Building it materializes every
    # variant of the score set, so skip that entirely when nothing needs identity resolution — the
    # common case of a re-upload whose calibrations use only range-based bins.
    variants_by_identity: dict[VariantIdentity, Optional[Variant]] = {}
    if any(snapshot.controls or snapshot.classification_members for snapshot in snapshots):
        for new_variant in db.scalars(select(Variant).where(Variant.score_set_id == score_set.id)).all():
            identity = (new_variant.hgvs_nt, new_variant.hgvs_pro, new_variant.hgvs_splice)
            # Upstream validation rejects duplicate variants, but a collision here would otherwise bind
            # every reference to whichever row happened to be seen first. Mark it unresolvable instead.
            variants_by_identity[identity] = None if identity in variants_by_identity else new_variant

    snapshots_by_calibration_id = {snapshot.calibration_id: snapshot for snapshot in snapshots}

    for calibration in score_set.score_calibrations:
        calibration_id = cast(int, calibration.id)
        snapshot = snapshots_by_calibration_id.get(
            calibration_id, CalibrationVariantLinkSnapshot(calibration_id=calibration_id)
        )

        # The linkage rows were removed with Core deletes, which leave any loaded collections holding
        # rows that no longer exist. Expire them so the ORM rebuilds from the post-delete state rather
        # than issuing deletes for rows that are already gone.
        db.expire(calibration, ["controls"])

        dropped_controls = 0
        relinked_controls: list[CalibrationControl] = []
        for control in snapshot.controls:
            control_variant = variants_by_identity.get(control.identity)
            if control_variant is None:
                dropped_controls += 1
                continue

            relinked_controls.append(
                CalibrationControl(
                    calibration_id=calibration.id,
                    variant_id=control_variant.id,
                    clinical_status=control.clinical_status,
                    created_by_id=control.created_by_id,
                    creation_date=control.creation_date,
                    modified_by_id=updater.id,
                    modification_date=date.today(),
                )
            )

        db.add_all(relinked_controls)
        report.controls_relinked += len(relinked_controls)
        report.controls_dropped += dropped_controls

        dropped_members = 0
        for classification in calibration.functional_classifications:
            db.expire(classification, ["variants"])

            if classification.class_ is not None:
                members: list[Variant] = []
                for identity in snapshot.classification_members.get(cast(int, classification.id), []):
                    member_variant = variants_by_identity.get(identity)
                    if member_variant is None:
                        dropped_members += 1
                        continue

                    members.append(member_variant)

                report.classification_members_relinked += len(members)
            else:
                members = variants_for_functional_classification(db, classification, use_sql=True)
                report.classifications_rebinned += 1
                report.classification_members_rebinned += len(members)

            classification.variants = members
            db.add(classification)

        report.classification_members_dropped += dropped_members

        if dropped_controls:
            calibration.controls_not_phi = None
            report.calibrations_pending_phi_reaffirmation.append(calibration_id)

        # Re-binning is the expected mechanical consequence of new scores, so it does not count as an
        # edit. Losing a hand-entered reference does.
        if dropped_controls or dropped_members:
            calibration.modified_by = updater
            db.add(calibration)

    if report.controls_dropped or report.classification_members_dropped:
        logger.warning(
            "Dropped %s calibration control(s) and %s class-based bin membership(s) from score set %s: their "
            "variants are absent from the new upload.",
            report.controls_dropped,
            report.classification_members_dropped,
            score_set.urn,
        )

    return report
