"""Build RFC 8594 deprecation headers for retired routes, parameters, and artifacts.

RFC 8594 defines ``Deprecation`` (the resource is deprecated) and ``Sunset`` (when it will stop working).
:func:`deprecation_headers` emits ``Deprecation: true`` and, where a replacement exists, a ``Link`` with
``rel="successor-version"``; a ``Warning: 299`` carries human-readable guidance for clients that do not
parse the structured headers. ``Sunset`` is included only when a removal date is supplied.
"""

import logging
from typing import Optional

from mavedb.lib.logging.context import logging_context, save_to_logging_context

logger = logging.getLogger(__name__)

# The removal date for the MappedVariant-retirement surfaces (the /mapped-variants redirects and the
# score-set tombstone), as an IMF-fixdate string (e.g. "Wed, 01 Jul 2026 00:00:00 GMT"), or None to emit no
# Sunset header. Unset until the team ratifies a date. Scoped to this one campaign — a different deprecation
# should not inherit this date, so pass it explicitly at each MappedVariant-retirement call site rather than
# relying on a builder-wide default.
MAPPED_VARIANT_SUNSET: Optional[str] = None

# Where an integrator should read about the migration. Points at the dump/API changelog channel.
DEPRECATION_DOCS_URL = "https://mavedb.org/docs/mavedb/changelog.html"


def deprecation_headers(
    *,
    successor: Optional[str] = None,
    warning: Optional[str] = None,
    sunset: Optional[str] = None,
    docs_url: Optional[str] = DEPRECATION_DOCS_URL,
) -> dict[str, str]:
    """Build the RFC 8594 header set for a deprecated resource.

    Args:
        successor: absolute path or URL of the 1:1 replacement resource, emitted as a
            ``Link: <successor>; rel="successor-version"``. Omit when no wire-compatible successor exists
            (a cardinality change), and rely on ``warning`` to point the caller at the nearest resource.
        warning: human-readable guidance, emitted as ``Warning: 299 - "<warning>"``.
        sunset: IMF-fixdate removal date, emitted as ``Sunset``. No default — each deprecation campaign has
            its own removal timeline, so callers pass their own date constant explicitly.
        docs_url: a documentation link, emitted as ``Link: <docs_url>; rel="deprecation"``. Pass None to omit.
    """
    headers: dict[str, str] = {"Deprecation": "true"}

    links = []
    if successor:
        links.append(f'<{successor}>; rel="successor-version"')
    if docs_url:
        links.append(f'<{docs_url}>; rel="deprecation"')
    if links:
        headers["Link"] = ", ".join(links)

    if sunset:
        headers["Sunset"] = sunset
    if warning:
        headers["Warning"] = f'299 - "{warning}"'

    return headers


def record_deprecated_usage(surface: str, *, successor: Optional[str] = None) -> None:
    """Record that a request touched a deprecated surface, under one queryable log marker.

    Every deprecated surface — a retired route, a deprecated query parameter, a future one — should show up
    the same way, so "who is still on a deprecated surface?" is a single query rather than a per-surface hunt.

    Stamps ``deprecation_marker: True`` into the logging context (the aggregation key), accumulates ``surface``
    into ``deprecated_surfaces`` so several deprecations on one request all show, and logs a warning naming the
    surface and its successor. ``surface`` is the thing used (a route path or a parameter name); ``successor``
    is its replacement, if one exists.
    """
    surfaces = sorted({*logging_context().get("deprecated_surfaces", []), surface})
    save_to_logging_context({"deprecated_surfaces": surfaces, "deprecation_marker": True})
    logger.warning(
        "Request used the deprecated surface %r; it will be removed in a future release.%s",
        surface,
        f" Use {successor} instead." if successor else "",
        extra={"deprecated_surface": surface, "deprecation_successor": successor},
    )
