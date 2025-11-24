"""Utilities for building and mutating score calibration ORM objects."""

import math
from typing import Optional, Union

import pandas as pd
from sqlalchemy import Float, and_, select
from sqlalchemy.orm import Session

from mavedb.lib.acmg import find_or_create_acmg_classification
from mavedb.lib.identifiers import find_or_create_publication_identifier
from mavedb.lib.validation.constants.general import calibration_class_column_name, calibration_variant_column_name
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_publication_identifier import ScoreCalibrationPublicationIdentifierAssociation
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models import score_calibration


def create_functional_classification(
    db: Session,
    functional_range_create: Union[
        score_calibration.FunctionalClassificationCreate, score_calibration.FunctionalClassificationModify
    ],
    containing_calibration: ScoreCalibration,
    variant_classes: Optional[dict[str, list[str]]] = None,
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
        variant_classes (Optional[dict[str, list[str]]]): Optional dictionary mapping variant classes
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
        acmg_classification=acmg_classification,
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
    variant_classes: Optional[dict[str, list[str]]] = None,
    containing_score_set: Optional[ScoreSet] = None,
) -> ScoreCalibration:
    """
    Create a ScoreCalibration ORM instance (not yet persisted) together with its
    publication identifier associations.

    For each publication source listed in the incoming ScoreCalibrationCreate model
    (threshold_sources, classification_sources, method_sources), this function
    ensures a corresponding PublicationIdentifier row exists (via
    find_or_create_publication_identifier) and creates a
    ScoreCalibrationPublicationIdentifierAssociation that links the identifier to
    the new calibration under the appropriate relation type
    (ScoreCalibrationRelation.threshold / .classification / .method).

    Fields in calibration_create that represent source lists or audit metadata
    (threshold_sources, classification_sources, method_sources, created_at,
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
        (ScoreCalibrationRelation.classification, calibration_create.classification_sources or []),
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
                "threshold_sources",
                "classification_sources",
                "method_sources",
                "score_set_urn",
            },
        ),
        publication_identifier_associations=calibration_pub_assocs,
        functional_classifications=[],
        created_by=user,
        modified_by=user,
    )  # type: ignore[call-arg]

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
    variant_classes: Optional[dict[str, list[str]]] = None,
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
        variant_classes (Optional[dict[str, list[str]]]): Optional dictionary mapping variant classes
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

    db.add(calibration)
    return calibration


async def create_score_calibration(
    db: Session,
    calibration_create: score_calibration.ScoreCalibrationCreate,
    user: User,
    variant_classes: Optional[dict[str, list[str]]] = None,
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
    variant_classes (Optional[dict[str, list[str]]]): Optional dictionary mapping variant classes
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
    variant_classes: Optional[dict[str, list[str]]] = None,
) -> ScoreCalibration:
    """
    Asynchronously modify an existing ScoreCalibration record and its related publication
    identifier associations.

    This function:
    1. Validates that a score_set_urn is provided in the update model (raises ValueError if absent).
    2. Loads (via SELECT ... WHERE urn = :score_set_urn) the ScoreSet that will contain the calibration.
    3. Reconciles publication identifier associations for three relation categories:
        - threshold_sources  -> ScoreCalibrationRelation.threshold
        - classification_sources -> ScoreCalibrationRelation.classification
        - method_sources -> ScoreCalibrationRelation.method
        For each provided source identifier:
          * Calls find_or_create_publication_identifier to obtain (or persist) the identifier row.
          * Preserves an existing association if already present.
          * Creates a new association if missing.
        Any previously existing associations not referenced in the update are deleted from the session.
    4. Updates mutable scalar fields on the calibration instance from calibration_update, excluding:
        threshold_sources, classification_sources, method_sources, created_at, created_by,
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
         - threshold_sources, classification_sources, method_sources (iterables of identifier objects)
         - Additional mutable calibration attributes.
     user : User
         Context for the authenticated user; the user to be recorded for audit.
     variant_classes (Optional[dict[str, list[str]]]): Optional dictionary mapping variant classes
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
        (ScoreCalibrationRelation.classification, calibration_update.classification_sources or []),
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
            "threshold_sources",
            "classification_sources",
            "method_sources",
            "created_at",
            "created_by",
            "modified_at",
            "modified_by",
            "score_set_urn",
        }:
            setattr(calibration, attr, value)

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
    variant_classes: Optional[dict[str, list[str]]] = None,
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
    variant_classes : Optional[dict[str, list[str]]]
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
    if use_sql:
        try:
            # Build score extraction expression: data['score_data']['score']::text::float
            score_expr = Variant.data["score_data"]["score"].astext.cast(Float)

            conditions = [Variant.score_set_id == score_set_id]
            if variant_classes is not None and functional_classification.class_ is not None:
                variant_urns = variant_classes.get(functional_classification.class_, [])
                conditions.append(Variant.urn.in_(variant_urns))

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
            variant_urns = variant_classes.get(functional_classification.class_, [])
            if v.urn in variant_urns:
                matches.append(v)

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
) -> dict[str, list[str]]:
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
    dict[str, list[str]]
        A dictionary where keys are functional class labels and values are lists
        of distinct variant URNs belonging to each class.
    """
    classification_dict: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        variant_urn = row[calibration_variant_column_name]
        functional_class = row[calibration_class_column_name]

        if functional_class not in classification_dict:
            classification_dict[functional_class] = []

        classification_dict[functional_class].append(variant_urn)

    return {k: list(set(v)) for k, v in classification_dict.items()}
