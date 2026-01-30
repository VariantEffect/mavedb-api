from typing import Optional

from mavedb.view_models.base.base import BaseModel


class ExternalLink(BaseModel):
    """
    Represents an external hyperlink for view models.

    Attributes:
        url (Optional[str]): Fully qualified URL for the external resource.
            May be None if no link is available or applicable.
    """

    url: Optional[str] = None
