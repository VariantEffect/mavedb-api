"""How the api answers failures of the outside services it looks records up in."""

from mavedb.lib.exception_handlers.responses import ErrorResponse, ErrorResponses, fixed, message
from mavedb.lib.exceptions import (
    AmbiguousIdentifierError,
    HGNCGeneNotFoundError,
    HGNCServiceError,
    MondoServiceError,
    NonexistentIdentifierError,
    PublicationServiceError,
    PublicationServiceTimeoutError,
)

_PUBLICATION_UNREACHABLE = "while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later."
"""The end of the detail for a publication service that failed or timed out, after its gateway status."""

EXTERNAL_SERVICE_ERROR_RESPONSES: ErrorResponses = {
    AmbiguousIdentifierError: ErrorResponse(status=400, body=message, alert=False),
    NonexistentIdentifierError: ErrorResponse(status=404, body=message, alert=False),
    HGNCGeneNotFoundError: ErrorResponse(status=404, body=message, alert=False),
    HGNCServiceError: ErrorResponse(
        status=503, body=fixed("message", "Gene information service temporarily unavailable"), alert=False
    ),
    MondoServiceError: ErrorResponse(
        status=503, body=fixed("message", "Disease ontology service temporarily unavailable"), alert=False
    ),
    # The publication errors answer with `detail`, which the ui's record editors show when a save fails.
    PublicationServiceError: ErrorResponse(
        status=502, body=fixed("detail", f"Bad Gateway {_PUBLICATION_UNREACHABLE}"), alert=False
    ),
    PublicationServiceTimeoutError: ErrorResponse(
        status=504, body=fixed("detail", f"Gateway Timeout {_PUBLICATION_UNREACHABLE}"), alert=False
    ),
}
"""How the api answers an identifier lookup that fails or an outside service that doesn't respond."""
