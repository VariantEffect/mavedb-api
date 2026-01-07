"""Database materialized view refresh jobs.

This module contains jobs for refreshing materialized views used throughout
the MaveDB application. Materialized views provide optimized, pre-computed
data for complex queries and are refreshed periodically to maintain
data consistency and performance.
"""

import logging

from mavedb.db.view import refresh_all_mat_views
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.worker.jobs.utils.job_state import setup_job_state

logger = logging.getLogger(__name__)


# TODO#405: Refresh materialized views within an executor.
async def refresh_materialized_views(ctx: dict):
    logging_context = setup_job_state(ctx, None, None, None)
    logger.debug(msg="Began refresh materialized views.", extra=logging_context)
    refresh_all_mat_views(ctx["db"])
    ctx["db"].commit()
    logger.debug(msg="Done refreshing materialized views.", extra=logging_context)
    return {"success": True}


async def refresh_published_variants_view(ctx: dict, correlation_id: str):
    logging_context = setup_job_state(ctx, None, None, correlation_id)
    logger.debug(msg="Began refresh of published variants materialized view.", extra=logging_context)
    PublishedVariantsMV.refresh(ctx["db"])
    ctx["db"].commit()
    logger.debug(msg="Done refreshing published variants materialized view.", extra=logging_context)
    return {"success": True}
