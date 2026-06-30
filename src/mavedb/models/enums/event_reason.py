from enum import Enum


class EventReason(str, Enum):
    """Pipeline-wide vocabulary for a variant event's ``reason`` — the single field that says
    *what happened*, spanning present / absent / not_applicable / failed.

    Reasons are shared across jobs wherever they mean the same thing (the ``annotation_type``
    already says *which* job), and job-specific only where a case is genuinely unique. One job
    contributes its own pre-existing domain enum instead of duplicating here: ``mapping`` uses
    ``MappingOutcome`` (mapped / intronic / no_protein_consequence / failed), which mirrors the
    external dcd-mapping vocabulary. The full ``reason`` vocabulary is this enum plus that one.
    """

    # present — we hold the result / the step succeeded
    CREATED = "created"  # linked/registered this run (gnomAD, ClinVar, CAR)
    PREEXISTING = "preexisting"  # already held before this run (gnomAD, ClinVar, CAR)
    RECONFIRMED = "reconfirmed"  # re-verified unchanged (gnomAD, CAR force)
    SKIPPED = "skipped"  # version-skip: already current at this source version (gnomAD, VEP)
    SUPERSEDED = "superseded"  # re-resolved within a release, newest wins (ClinVar)
    SUBMITTED = "submitted"  # LDH
    TRANSLATED = "translated"  # reverse translation
    RECONFIRMATION_SKIPPED = "reconfirmation_skipped"  # HGVS no longer buildable, existing CAID kept (CAR)

    # absent — the source or biology has nothing (informative negative)
    NO_RECORD = "no_record"  # source queried, returned nothing (gnomAD, ClinVar, VEP)
    NO_CODING_TRANSCRIPT = "no_coding_transcript"  # non-coding target has no protein consequence (RT)

    # not_applicable — we could not ask (structural gap)
    NO_CAID = "no_caid"  # no ClinGen allele id to key on (gnomAD, ClinVar)
    NO_HGVS = "no_hgvs"  # no HGVS to submit/resolve (CAR, VEP)
    MULTI_VARIANT_CAID = "multi_variant_caid"  # cis-block CAID cannot be used (ClinVar)
    NO_ASSAY_LEVEL_HGVS = "no_assay_level_hgvs"  # no assay-level HGVS to translate (RT)

    # failed — errored
    API_ERROR = "api_error"  # network/timeout/upstream error (ClinVar, VEP, LDH, CAR no-response)
    SERVICE_REJECTED = "service_rejected"  # external service refused the input (CAR)
    MALFORMED_RESPONSE = "malformed_response"  # unparseable/contractless response (CAR)
    CAID_CONFLICT = "caid_conflict"  # returned identifier conflicts with the stored one (CAR)
    TRANSLATION_FAILED = "translation_failed"  # all candidate HGVS failed translation (RT)
    TRANSLATION_ERROR = "translation_error"  # the translation engine errored (RT)
    TRANSCRIPT_UNRESOLVED = "transcript_unresolved"  # protein-coding target with no resolvable transcript (RT)
