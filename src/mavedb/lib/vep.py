"""VEP (Variant Effect Predictor) resolution and linking for molecular-consequence prediction.

The resolution itself — VEP querying batched by transcript set, transcript-matched consequence
selection, the Variant Recoder fallback, and how recoded forms combine — is the shared
``variant_annotation.lib.vep`` orchestration, run over an Ensembl REST client. The api keeps no second
copy of that flow, so this pipeline and any other consumer (the lab CLI) resolve identically — the same
answer *and* the same flow for the same input. This module owns only the api-side concerns: running
that (synchronous) orchestration off the worker's event loop on a job-scoped client, the Ensembl release
lookup that version-keys the skip, and writing resolved consequences against deduplicated
:class:`Allele` rows. The worker job composes these; it holds none of the resolution mechanics itself.
"""

import asyncio
import functools
import logging
import os
from datetime import date
from enum import Enum
from typing import Mapping, Optional, Sequence

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session
from variant_annotation.lib.clients.ensembl import EnsemblRestClient
from variant_annotation.lib.vep import (
    RESOLVER_VERSION,
    ConsequenceOutcome,
    ConsequenceResolution,
    ConsequenceSource,
    ReferenceSequence,
    VepConfig,
    VepInput,
)
from variant_annotation.lib.vep import resolve_consequences as resolve_consequences_kernel

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.enums.vep import VepConsequenceSource
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

logger = logging.getLogger(__name__)


ENSEMBL_API_URL = os.environ.get("ENSEMBL_API_URL", "https://rest.ensembl.org")


class VepLinkVerdict(str, Enum):
    """Per-allele outcome of a VEP linking run, returned for every allele whose status is decided.

    The single source of truth for what happened to an allele's consequence this run — the caller
    derives annotation status from this, never by re-querying consequence state. An allele absent from
    the map had no live consequence and resolved none this run (the caller reads that as "no result").

    - ``CREATED`` — a new or changed consequence was created/superseded this run.
    - ``UNCHANGED`` — a live consequence was re-confirmed (VEP resolved the same headline term).
    - ``RETAINED_ON_ABSENCE`` — VEP found *nothing* this run, but a prior live consequence was held in
      place rather than overwritten with a null. Kept distinct from ``UNCHANGED`` so the caller can
      surface it: the allele still has a consequence, but the current Ensembl release did not confirm
      it, which may mean a transient miss or a genuine upstream disappearance (transcript retirement).
    """

    CREATED = "created"
    UNCHANGED = "unchanged"
    RETAINED_ON_ABSENCE = "retained_on_absence"


async def get_ensembl_release() -> str:
    """Return the current Ensembl release the REST API is serving, e.g. ``"116"`` (``/info/software``).

    A release is coordinated — software, transcript set, and consequence vocabulary all bump together
    under one number — so this single value version-keys VEP results the way gnomAD keys on its data
    version. The job stamps it on each consequence and skips alleles already live at the current release
    (paired with ``RESOLVER_VERSION``). Delegates to the client's ``software_release`` so the release
    lookup shares the resolution transport rather than being a second HTTP path. The client's synchronous
    call runs off the event loop. Raises on failure: the version is load-bearing for the skip, so a job
    that cannot determine it must abort rather than mis-version its writes.
    """
    loop = asyncio.get_running_loop()
    with requests.Session() as session:
        client = EnsemblRestClient(api_url=ENSEMBL_API_URL, session=session)
        return await loop.run_in_executor(None, client.software_release)


async def resolve_consequences(
    inputs: Sequence[VepInput], *, reference: Optional[ReferenceSequence] = None
) -> dict[str, ConsequenceResolution]:
    """Resolve each input to a molecular consequence, keyed by its HGVS. One resolution per input.

    Delegates the full resolution flow to :func:`variant_annotation.lib.vep.resolve_consequences`, run
    over an :class:`EnsemblRestClient` that satisfies both the VEP-lookup and Variant-Recoder ports: VEP
    querying (batched by transcript set, resolved against each input's own transcript) and the Recoder
    fallback for misses. The api keeps no second copy of that flow.

    ``reference`` is the optional transcript-reference port that lights up reference-identical detection:
    a no-change input (an explicit ``c.=`` form, or a ``delins`` whose replacement bases equal what the
    transcript already reads) resolves to ``REFERENCE_IDENTICAL`` rather than ``ABSENT``. It is consulted
    only for inputs VEP and the Recoder both left unresolved — the wild-type controls and unparseables —
    so it costs nothing for the inputs VEP resolves normally. When omitted, those inputs stay ``ABSENT``.

    The library call is synchronous; we run it in one executor call so the worker's event loop stays
    free. It resolves serially (``VepConfig`` default ``max_workers=1``) to stay within Ensembl's
    per-client rate limit. There is no per-input progress hook, so a caller reporting progress on a long
    run chunks its inputs and awaits this once per chunk.

    The Ensembl client runs on a session we own and close when the call finishes, so its pooled
    connections do not linger idle; the ``reference`` port's lifecycle is the caller's. Every input
    appears in the result exactly once, with outcome ``RESOLVED`` / ``ABSENT`` / ``ERRORED`` — the caller
    must retry ``ERRORED`` (unknown), never store it as a genuine empty.
    """
    if not inputs:
        return {}

    loop = asyncio.get_running_loop()
    with requests.Session() as session:
        client = EnsemblRestClient(api_url=ENSEMBL_API_URL, session=session)
        resolutions = await loop.run_in_executor(
            None,
            functools.partial(
                resolve_consequences_kernel,
                inputs,
                vep=client,
                recoder=client,
                reference=reference,
                config=VepConfig(),
            ),
        )
    return {resolution.input.hgvs: resolution for resolution in resolutions}


def _to_db_source(source: Optional[ConsequenceSource]) -> Optional[VepConsequenceSource]:
    """Map the kernel's resolution source onto the api's persisted enum (identical values, distinct owners)."""
    return VepConsequenceSource(source.value) if source is not None else None


def link_vep_consequences_to_alleles(
    db: Session,
    resolutions_by_allele_id: Mapping[int, ConsequenceResolution],
    *,
    source_version: str,
    access_date: date,
) -> dict[int, VepLinkVerdict]:
    """Store VEP consequences against deduplicated alleles, superseding only on a headline-term change.

    ``resolutions_by_allele_id`` maps each queried allele to the kernel's :class:`ConsequenceResolution`.
    Only ``RESOLVED`` and ``ABSENT`` resolutions belong here — an ``ERRORED`` allele's failure is
    recorded in the annotation event stream and it is retried, never linked, so passing one is a caller
    bug and is ignored defensively. ``source_version`` is the Ensembl release the run resolved against.
    Each allele holds at most one live :class:`VepAlleleConsequence`, handled per allele:

    - **unchanged** (live row already carries this headline consequence): advance ``source_version``,
      ``resolver_version``, ``access_date``, and the provenance columns (``consequence_terms``/
      ``consequence_source``/``matched_transcript``) in place — no supersede. Supersede is keyed on the
      headline term, not the release, resolver version, or provenance: a re-run that resolves the same
      term must not fabricate a transaction-time boundary, and filling in a version/provenance column
      for an already-held term (e.g. a pre-#772 row with NULL provenance, or a row predating
      ``resolver_version``) is not a value change either.
    - **new or changed** (no live row, or a different headline term): supersede keyed on ``allele_id``
      (retire the old, insert the successor stamped with this run's version + provenance).
    - **absent this run** (``ABSENT`` outcome) with a live row: leave it in place — do not overwrite a
      held consequence with a null result — and return :attr:`~VepLinkVerdict.RETAINED_ON_ABSENCE`.
      This is loudly logged (per allele and in aggregate) because it can signal a genuine upstream
      disappearance, not just a transient miss; the caller surfaces the aggregate in job metadata.
    - **absent this run** with no live row: nothing to do; the allele is left out of the returned map.

    Does not commit. Returns a verdict per allele whose status is decided this run:
    :attr:`~VepLinkVerdict.CREATED` (created/superseded), :attr:`~VepLinkVerdict.UNCHANGED` (a live
    consequence re-confirmed), or :attr:`~VepLinkVerdict.RETAINED_ON_ABSENCE` (VEP found nothing, prior
    consequence held). An allele absent from the returned map had no live row and resolved nothing — the
    caller reads that as "no result". This is the single source of truth for per-allele status; callers
    must not re-derive it from consequence state.
    """
    save_to_logging_context({"num_alleles_to_link_vep": len(resolutions_by_allele_id)})
    logger.debug(msg="Linking VEP consequences to alleles", extra=logging_context())

    verdicts: dict[int, VepLinkVerdict] = {}
    for allele_id, resolution in resolutions_by_allele_id.items():
        if resolution.outcome is ConsequenceOutcome.ERRORED:
            # Failures are not this table's business; the caller records them in the event stream.
            continue

        consequence = resolution.most_severe_consequence if resolution.outcome is ConsequenceOutcome.RESOLVED else None

        live = db.scalar(
            select(VepAlleleConsequence).where(
                VepAlleleConsequence.allele_id == allele_id,
                VepAlleleConsequence.current,
            )
        )

        # VEP found nothing this run. Do not overwrite a held consequence with a null result; a retained
        # consequence is RETAINED_ON_ABSENCE (surfaced), while no live row at all leaves the allele out
        # of the map (the caller reads that as a no-result).
        if consequence is None:
            if live is not None:
                logger.warning(
                    f"VEP returned NO consequence for allele {allele_id} (release {source_version}) that "
                    f"previously resolved to '{live.functional_consequence}'; retaining the prior value "
                    f"(not superseding). Investigate if this recurs — it can mean transcript retirement or "
                    f"an upstream Ensembl change, not a transient miss.",
                    extra=logging_context(),
                )
                verdicts[allele_id] = VepLinkVerdict.RETAINED_ON_ABSENCE

            continue

        # Unchanged headline term: advance version/freshness and refresh provenance in place.
        if live is not None and live.functional_consequence == consequence:
            live.source_version = source_version
            live.resolver_version = RESOLVER_VERSION
            live.access_date = access_date
            live.consequence_terms = list(resolution.consequence_terms)
            live.consequence_source = _to_db_source(resolution.source)
            live.matched_transcript = resolution.matched_transcript
            verdicts[allele_id] = VepLinkVerdict.UNCHANGED
            continue

        # New or changed consequence: retire any live row, insert the successor with full provenance.
        VepAlleleConsequence.supersede_live_where(
            db,
            [
                VepAlleleConsequence(
                    allele_id=allele_id,
                    functional_consequence=consequence,
                    consequence_terms=list(resolution.consequence_terms),
                    consequence_source=_to_db_source(resolution.source),
                    matched_transcript=resolution.matched_transcript,
                    source_version=source_version,
                    resolver_version=RESOLVER_VERSION,
                    access_date=access_date,
                )
            ],
            VepAlleleConsequence.allele_id == allele_id,
        )
        verdicts[allele_id] = VepLinkVerdict.CREATED

    changed_allele_count = sum(1 for v in verdicts.values() if v is VepLinkVerdict.CREATED)
    retained_on_absence_count = sum(1 for v in verdicts.values() if v is VepLinkVerdict.RETAINED_ON_ABSENCE)
    save_to_logging_context(
        {"changed_allele_count": changed_allele_count, "retained_on_absence_count": retained_on_absence_count}
    )
    logger.info(
        msg=f"Created or superseded {changed_allele_count} VEP allele consequences this run.",
        extra=logging_context(),
    )
    if retained_on_absence_count:
        # Aggregate signal the job lifts into its outcome metadata: a large or growing count means VEP
        # is now returning nothing for alleles that used to resolve — worth a look, not just a log line.
        logger.warning(
            msg=f"VEP returned no consequence for {retained_on_absence_count} allele(s) that previously had one; "
            f"prior consequences were RETAINED, not superseded. A large or growing count may indicate "
            f"transcript retirement or an upstream Ensembl data change.",
            extra=logging_context(),
        )
    return verdicts
