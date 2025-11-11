"""Utilities for building and mutating score calibration ORM objects."""

from sqlalchemy.orm import Session

from mavedb.lib.identifiers import find_or_create_publication_identifier
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_calibration_publication_identifier import ScoreCalibrationPublicationIdentifierAssociation
from mavedb.models.user import User
from mavedb.view_models import score_calibration


async def _create_score_calibration(
    db: Session, calibration_create: score_calibration.ScoreCalibrationCreate, user: User
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
                "threshold_sources",
                "classification_sources",
                "method_sources",
                "score_set_urn",
            },
        ),
        publication_identifier_associations=calibration_pub_assocs,
        created_by=user,
        modified_by=user,
    )  # type: ignore[call-arg]

    return calibration


async def create_score_calibration_in_score_set(
    db: Session, calibration_create: score_calibration.ScoreCalibrationCreate, user: User
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
    calibration = await _create_score_calibration(db, calibration_create, user)
    calibration.score_set = containing_score_set

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
    db: Session, calibration_create: score_calibration.ScoreCalibrationCreate, user: User
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

    created_calibration = await _create_score_calibration(db, calibration_create, user)

    db.add(created_calibration)
    return created_calibration


async def modify_score_calibration(
    db: Session,
    calibration: ScoreCalibration,
    calibration_update: score_calibration.ScoreCalibrationModify,
    user: User,
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
    del carrying updated field values plus source identifier lists:
            - score_set_urn (required)
            - threshold_sources, classification_sources, method_sources (iterables of identifier objects)
            - Additional mutable calibration attributes.
       user : User
            Context for the authenticated user; the user to be recorded for audit.

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

    # Remove associations that are no longer present
    for assoc in existing_assocs_map.values():
        db.delete(assoc)

    for attr, value in calibration_update.model_dump().items():
        if attr not in {
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
    calibration.publication_identifier_associations = updated_assocs
    calibration.modified_by = user

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
