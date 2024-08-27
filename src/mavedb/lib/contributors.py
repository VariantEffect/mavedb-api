from sqlalchemy.orm import Session

from mavedb.lib import orcid
from mavedb.lib.exceptions import NonexistentOrcidUserError
from mavedb.models.contributor import Contributor


async def find_or_create_contributor(db: Session, orcid_id: str):
    """
    Find an existing contributor record with the specified ORCID ID, or create a new one and fetch the contributor name
    from ORCID.

    :param db: An active database session
    :param orcid_id: A valid ORCID ID
    :return: An existing Contributor with the specified ORCID ID, or a new, unsaved Contributor
    """
    contributor = db.query(Contributor).filter(Contributor.orcid_id == orcid_id).one_or_none()
    if contributor is None:
        # Don't import fetch_orcid_user above, because we want to be able to patch it for unit testing.
        orcid_user = orcid.fetch_orcid_user(orcid_id)
        if orcid_user is None:
            raise NonexistentOrcidUserError(f"No ORCID user was found for ORCID ID {orcid_id}.")
        contributor = Contributor(
            orcid_id=orcid_id, family_name=orcid_user.family_name, given_name=orcid_user.given_name
        )
    return contributor
