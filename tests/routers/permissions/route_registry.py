"""Every route the api serves, by method and path: swept by the canary, or exempt with a written reason (#810).

Each entry is one of three kinds:

- ``Read``: the route only reads. The sweep calls it as every persona, and the positive control reads it too.
- ``Write``: the route changes data, and the entry says what its response can carry. Writes are listed so none
  goes unnoticed, but aren't swept yet; the test harness's per-test templates will make that cheap.
- ``Exempt``: no planted private record can reach the response, and the reason says why.

``test_route_registry.py`` fails when a served route is missing, so a new route can't ship unswept. To add one,
give it a ``Read`` built from what ``plant_canary`` planted, reusing a helper below where the path fits one, a
``Write`` saying what its response carries, or an ``Exempt`` with a reason a reviewer can check.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

from mavedb.routers.shared import ROUTER_BASE_PREFIX
from tests.routers.permissions.canary import (
    PRIVATE_ALLELE_ID,
    PRIVATE_CAID,
    PRIVATE_DOI,
    PRIVATE_PUBLICATION_IDENTIFIER,
    PRIVATE_TARGET_NAME,
    SHARED_ALLELE_ID,
    SHARED_CAID,
    Canary,
)


@dataclass(frozen=True)
class Call:
    """One request the sweep makes: a URL, and the JSON body a search or lookup takes."""

    url: str
    json: Optional[Any] = None


Calls = Callable[[Canary], list[Union[str, Call]]]
"""Builds a route's requests from what was planted. A plain string is a call with no body."""


@dataclass(frozen=True)
class Read:
    """A route that only reads, with the requests the sweep and the positive control make."""

    requests: Calls


@dataclass(frozen=True)
class Write:
    """A route that changes data, and what its response can carry.

    Writes aren't swept yet. Sweeping one as every persona needs fresh planted data for each persona, since a
    successful write can change who may see what; the test harness's per-test templates make that cheap (see #782).
    """

    carries: str


@dataclass(frozen=True)
class Exempt:
    """Why the route can't carry a planted private record."""

    reason: str


Entry = Union[Read, Write, Exempt]
"""A route's registry entry: the requests the sweep makes, or why it makes none."""


def _static(path: str) -> Read:
    return Read(lambda canary: [path])


def _score_sets(suffix: str = "") -> Read:
    return Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-sets/{urn}{suffix}"
            for urn in _both(canary.private_score_set_urn, canary.public_score_set_urn)
        ]
    )


def _quote(urn: str) -> str:
    """A variant URN's ``#`` would start a URL fragment."""
    return urn.replace("#", "%23")


def _variants(suffix: str = "") -> Read:
    return Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/variants/{_quote(urn)}{suffix}"
            for urn in canary.private_variant_urns + canary.public_variant_urns
        ]
    )


def _mapped_variants(suffix: str = "") -> Read:
    # These answer with a permanent redirect to the variant routes; the client follows it.
    return Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/mapped-variants/{_quote(urn)}{suffix}"
            for urn in canary.private_variant_urns + canary.public_variant_urns
        ]
    )


def _calibrations(suffix: str = "") -> Read:
    return Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-calibrations/{urn}{suffix}"
            for urn in (canary.community_calibration_urn, canary.investigator_calibration_urn)
        ]
    )


def _record_statistics(field: str) -> Read:
    return Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/statistics/record/{record}/{field}" for record in ("experiment", "score-set")
        ]
    )


def _both(private: str, public: str) -> list[str]:
    return [private, public]


_ALLELE_IDS = (SHARED_ALLELE_ID, PRIVATE_ALLELE_ID)
"""Allele IDs the lookups are called with: one a published variant reaches, one only a private variant does."""

_CAIDS = (SHARED_CAID, PRIVATE_CAID)
"""The ClinGen allele IDs of ``_ALLELE_IDS``, in the same order."""

_REFERENCE_DATA = "Serves reference data that no record owns and that holds nothing private."
"""The exemption reason for the reference data routes."""

_EXTERNAL_LOOKUP = "Proxies an external service and reads no MaveDB records."
"""The exemption reason for routes that only call another service."""

_ADMIN_ONLY = "Admins only (RoleRequirer), and admins may read everything; every other persona is refused."
"""The exemption reason for routes only admins may call."""

_OWN_ACCOUNT = "Acts only on the caller's own account, and returns only that account."
"""The exemption reason for routes that manage the signed-in user."""

_COLLECTION_WRITE = "Changes a collection; the response can carry its score sets, experiments and roster."
"""What a collection write's response can carry."""

_EXPERIMENT_WRITE = "Changes an experiment; the response can carry its experiment set and score sets."
"""What an experiment write's response can carry."""

_SCORE_SET_WRITE = "Changes a score set; the response can carry its experiment, contributors and calibrations."
"""What a score set write's response can carry."""

_CALIBRATION_WRITE = "Changes a calibration; the response can carry its classes and its score set."
"""What a calibration write's response can carry."""


def _post(path: str, *bodies: dict) -> Read:
    """A search or lookup that reads through a POST, called once per body."""
    return Read(lambda canary: [Call(f"{ROUTER_BASE_PREFIX}{path}", json=body) for body in bodies])


ROUTES: dict[str, Entry] = {
    # Alleles reached by identity across score sets, with no parent boundary to guard them.
    "GET /api/v1/alleles/{identifier}": Read(
        lambda canary: [f"{ROUTER_BASE_PREFIX}/alleles/{identifier}" for identifier in (*_ALLELE_IDS, *_CAIDS)]
    ),
    "GET /api/v1/clingen-alleles/{clingen_allele_id}/measurements": Read(
        lambda canary: [f"{ROUTER_BASE_PREFIX}/clingen-alleles/{caid}/measurements" for caid in _CAIDS]
    ),
    "GET /api/v1/variants/vrs/{identifier}": Read(
        lambda canary: [f"{ROUTER_BASE_PREFIX}/variants/vrs/{identifier}" for identifier in _ALLELE_IDS]
    ),
    "GET /api/v1/mapped-variants/vrs/{identifier}": Read(
        lambda canary: [f"{ROUTER_BASE_PREFIX}/mapped-variants/vrs/{identifier}" for identifier in _ALLELE_IDS]
    ),
    # Variants.
    "GET /api/v1/variants/{urn}": _variants(),
    "GET /api/v1/variants/{urn}/csv": _variants("/csv"),
    "GET /api/v1/variants/{urn}/csv-namespaces": _variants("/csv-namespaces"),
    "GET /api/v1/variants/{urn}/va/functional-statement": _variants("/va/functional-statement"),
    "GET /api/v1/variants/{urn}/va/pathogenicity-statement": _variants("/va/pathogenicity-statement"),
    "GET /api/v1/variants/{urn}/va/study-result": _variants("/va/study-result"),
    "GET /api/v1/mapped-variants/{urn}": _mapped_variants(),
    "GET /api/v1/mapped-variants/{urn}/va/functional-statement": _mapped_variants("/va/functional-statement"),
    "GET /api/v1/mapped-variants/{urn}/va/pathogenicity-statement": _mapped_variants("/va/pathogenicity-statement"),
    "GET /api/v1/mapped-variants/{urn}/va/study-result": _mapped_variants("/va/study-result"),
    # Score sets.
    "GET /api/v1/score-sets/": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-sets/?urns={canary.private_score_set_urn}&urns={canary.public_score_set_urn}"
        ]
    ),
    "GET /api/v1/score-sets/mapped-genes": _static(f"{ROUTER_BASE_PREFIX}/score-sets/mapped-genes"),
    "GET /api/v1/score-sets/recently-published": _static(f"{ROUTER_BASE_PREFIX}/score-sets/recently-published"),
    "GET /api/v1/score-sets/{urn}": _score_sets(),
    "GET /api/v1/score-sets/{urn}/annotated-variants/functional-statement": _score_sets(
        "/annotated-variants/functional-statement"
    ),
    "GET /api/v1/score-sets/{urn}/annotated-variants/pathogenicity-statement": _score_sets(
        "/annotated-variants/pathogenicity-statement"
    ),
    "GET /api/v1/score-sets/{urn}/annotated-variants/study-result": _score_sets("/annotated-variants/study-result"),
    "GET /api/v1/score-sets/{urn}/clinical-controls": _score_sets("/clinical-controls"),
    "GET /api/v1/score-sets/{urn}/clinical-controls/options": _score_sets("/clinical-controls/options"),
    "GET /api/v1/score-sets/{urn}/counts": _score_sets("/counts"),
    "GET /api/v1/score-sets/{urn}/csv-namespaces": _score_sets("/csv-namespaces"),
    "GET /api/v1/score-sets/{urn}/gnomad-variants": _score_sets("/gnomad-variants"),
    "GET /api/v1/score-sets/{urn}/mapped-variants": _score_sets("/mapped-variants"),
    "GET /api/v1/score-sets/{urn}/scores": _score_sets("/scores"),
    "GET /api/v1/score-sets/{urn}/variant-details": _score_sets("/variant-details"),
    "GET /api/v1/score-sets/{urn}/variants": _score_sets("/variants"),
    "GET /api/v1/score-sets/{urn}/variants/data": _score_sets("/variants/data"),
    # Experiments and experiment sets.
    "GET /api/v1/experiments/": _static(f"{ROUTER_BASE_PREFIX}/experiments/"),
    "GET /api/v1/experiments/{urn}": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/experiments/{urn}"
            for urn in _both(canary.private_experiment_urn, canary.public_experiment_urn)
        ]
    ),
    "GET /api/v1/experiments/{urn}/score-sets": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/experiments/{urn}/score-sets"
            for urn in _both(canary.private_experiment_urn, canary.public_experiment_urn)
        ]
    ),
    "GET /api/v1/experiment-sets/{urn}": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/experiment-sets/{urn}"
            for urn in _both(canary.private_experiment_set_urn, canary.public_experiment_set_urn)
        ]
    ),
    # Calibrations.
    "GET /api/v1/score-calibrations/me": _static(f"{ROUTER_BASE_PREFIX}/score-calibrations/me"),
    "GET /api/v1/score-calibrations/score-set/{score_set_urn}": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-calibrations/score-set/{urn}"
            for urn in _both(canary.private_score_set_urn, canary.public_score_set_urn)
        ]
    ),
    "GET /api/v1/score-calibrations/score-set/{score_set_urn}/primary": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-calibrations/score-set/{urn}/primary"
            for urn in _both(canary.private_score_set_urn, canary.public_score_set_urn)
        ]
    ),
    "GET /api/v1/score-calibrations/{urn}": _calibrations(),
    "GET /api/v1/score-calibrations/{urn}/variants": _calibrations("/variants"),
    "GET /api/v1/score-calibrations/{urn}/functional-classifications/{classification_id}/variants": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/score-calibrations/{urn}/functional-classifications/{classification_id}/variants"
            for urn, ids in (
                (canary.community_calibration_urn, canary.community_classification_ids),
                (canary.investigator_calibration_urn, canary.investigator_classification_ids),
            )
            for classification_id in ids
        ]
    ),
    # Collections and users.
    "GET /api/v1/collections/{urn}": Read(lambda canary: [f"{ROUTER_BASE_PREFIX}/collections/{canary.collection_urn}"]),
    "GET /api/v1/users/me/collections": _static(f"{ROUTER_BASE_PREFIX}/users/me/collections"),
    "GET /api/v1/users/": _static(f"{ROUTER_BASE_PREFIX}/users/"),
    "GET /api/v1/users/me": _static(f"{ROUTER_BASE_PREFIX}/users/me"),
    "GET /api/v1/users/me/access-keys": _static(f"{ROUTER_BASE_PREFIX}/users/me/access-keys"),
    "GET /api/v1/users/{id}": Read(lambda canary: [f"{ROUTER_BASE_PREFIX}/users/{canary.owner_id}"]),
    "GET /api/v1/users/{orcid_id}": Read(lambda canary: [f"{ROUTER_BASE_PREFIX}/users/{canary.owner_orcid}"]),
    # Publications.
    "GET /api/v1/publication-identifiers/": _static(f"{ROUTER_BASE_PREFIX}/publication-identifiers/"),
    "GET /api/v1/publication-identifiers/databases": _static(f"{ROUTER_BASE_PREFIX}/publication-identifiers/databases"),
    "GET /api/v1/publication-identifiers/journals": _static(f"{ROUTER_BASE_PREFIX}/publication-identifiers/journals"),
    "GET /api/v1/publication-identifiers/{identifier}": _static(
        f"{ROUTER_BASE_PREFIX}/publication-identifiers/{PRIVATE_PUBLICATION_IDENTIFIER}"
    ),
    "GET /api/v1/publication-identifiers/{db_name}/{identifier}": _static(
        f"{ROUTER_BASE_PREFIX}/publication-identifiers/PubMed/{PRIVATE_PUBLICATION_IDENTIFIER}"
    ),
    "GET /api/v1/publication-identifiers/search/{identifier}": _static(
        f"{ROUTER_BASE_PREFIX}/publication-identifiers/search/{PRIVATE_PUBLICATION_IDENTIFIER}"
    ),
    "GET /api/v1/publication-identifiers/search/{db_name}/{identifier}": _static(
        f"{ROUTER_BASE_PREFIX}/publication-identifiers/search/PubMed/{PRIVATE_PUBLICATION_IDENTIFIER}"
    ),
    # Target genes.
    "GET /api/v1/target-genes": _static(f"{ROUTER_BASE_PREFIX}/target-genes"),
    "GET /api/v1/target-genes/categories": _static(f"{ROUTER_BASE_PREFIX}/target-genes/categories"),
    "GET /api/v1/target-genes/names": _static(f"{ROUTER_BASE_PREFIX}/target-genes/names"),
    "GET /api/v1/target-genes/{item_id}": Read(
        lambda canary: [f"{ROUTER_BASE_PREFIX}/target-genes/{canary.private_target_gene_id}"]
    ),
    # Statistics, which read the published-variants materialized view as well as the tables.
    "GET /api/v1/statistics/mapped-variant/count": _static(f"{ROUTER_BASE_PREFIX}/statistics/mapped-variant/count"),
    "GET /api/v1/statistics/variant/count": _static(f"{ROUTER_BASE_PREFIX}/statistics/variant/count"),
    "GET /api/v1/statistics/record/score-set/mapped-variant/count": _static(
        f"{ROUTER_BASE_PREFIX}/statistics/record/score-set/mapped-variant/count"
    ),
    "GET /api/v1/statistics/record/score-set/variant/count": _static(
        f"{ROUTER_BASE_PREFIX}/statistics/record/score-set/variant/count"
    ),
    "GET /api/v1/statistics/record/{model}/published/count": Read(
        lambda canary: [
            f"{ROUTER_BASE_PREFIX}/statistics/record/{model}/published/count" for model in ("experiment", "score-set")
        ]
    ),
    "GET /api/v1/statistics/record/{record}/created-by": _record_statistics("created-by"),
    "GET /api/v1/statistics/record/{record}/doi-identifiers": _record_statistics("doi-identifiers"),
    "GET /api/v1/statistics/record/{record}/keywords": _record_statistics("keywords"),
    "GET /api/v1/statistics/record/{record}/publication-identifiers": _record_statistics("publication-identifiers"),
    "GET /api/v1/statistics/record/{record}/raw-read-identifiers": _record_statistics("raw-read-identifiers"),
    **{
        f"GET /api/v1/statistics/target/{path}": _static(f"{ROUTER_BASE_PREFIX}/statistics/target/{path}")
        for path in (
            "accession/accession",
            "accession/assembly",
            "accession/gene",
            "gene/category",
            "gene/ensembl-identifier",
            "gene/organism",
            "gene/refseq-identifier",
            "gene/uniprot-identifier",
            "mapped/gene",
            "sequence/sequence",
            "sequence/sequence-type",
        )
    },
    # Reads through a POST: searches and lookups.
    "POST /api/v1/doi-identifiers/search": _post("/doi-identifiers/search", {"text": PRIVATE_DOI}),
    "POST /api/v1/experiments/search": _post("/experiments/search", {}, {"text": "Canary"}),
    "POST /api/v1/me/experiments/search": _post("/me/experiments/search", {}),
    "POST /api/v1/me/score-sets/search": _post("/me/score-sets/search", {}),
    "POST /api/v1/me/target-genes/search": _post("/me/target-genes/search", {"text": PRIVATE_TARGET_NAME}),
    "POST /api/v1/publication-identifiers/search": _post(
        "/publication-identifiers/search", {"text": PRIVATE_PUBLICATION_IDENTIFIER}
    ),
    "POST /api/v1/publication-identifiers/search/doi": _post("/publication-identifiers/search/doi", {"text": "10."}),
    "POST /api/v1/publication-identifiers/search/identifier": _post(
        "/publication-identifiers/search/identifier", {"text": PRIVATE_PUBLICATION_IDENTIFIER}
    ),
    "POST /api/v1/raw-read-identifiers/search": _post("/raw-read-identifiers/search", {"text": "SRR"}),
    "POST /api/v1/score-sets/search": _post(
        "/score-sets/search",
        {},
        {"text": "Canary"},
        {"published": False},
        {"targets": [PRIVATE_TARGET_NAME]},
    ),
    "POST /api/v1/score-sets/search/filter-options": _post("/score-sets/search/filter-options", {}),
    "POST /api/v1/target-gene-identifiers/search": _post("/target-gene-identifiers/search", {"text": ""}),
    "POST /api/v1/target-genes/search": _post("/target-genes/search", {"text": PRIVATE_TARGET_NAME}),
    "POST /api/v1/variants/clingen-allele-id-lookups": Read(
        lambda canary: [
            Call(f"{ROUTER_BASE_PREFIX}/variants/clingen-allele-id-lookups", json={"clingenAlleleIds": list(_CAIDS)})
        ]
    ),
    # Writes, listed but not swept yet; see Write.
    "POST /api/v1/collections/": Write(_COLLECTION_WRITE),
    "PATCH /api/v1/collections/{urn}": Write(_COLLECTION_WRITE),
    "DELETE /api/v1/collections/{urn}": Write(_COLLECTION_WRITE),
    "POST /api/v1/collections/{collection_urn}/experiments": Write(_COLLECTION_WRITE),
    "DELETE /api/v1/collections/{collection_urn}/experiments/{experiment_urn}": Write(_COLLECTION_WRITE),
    "POST /api/v1/collections/{collection_urn}/score-sets": Write(_COLLECTION_WRITE),
    "DELETE /api/v1/collections/{collection_urn}/score-sets/{score_set_urn}": Write(_COLLECTION_WRITE),
    "POST /api/v1/collections/{urn}/{role}s": Write(_COLLECTION_WRITE),
    "DELETE /api/v1/collections/{urn}/{role}s/{orcid_id}": Write(_COLLECTION_WRITE),
    "POST /api/v1/experiments/": Write(_EXPERIMENT_WRITE),
    "PUT /api/v1/experiments/{urn}": Write(_EXPERIMENT_WRITE),
    "DELETE /api/v1/experiments/{urn}": Write(_EXPERIMENT_WRITE),
    "POST /api/v1/score-sets/": Write(_SCORE_SET_WRITE),
    "PUT /api/v1/score-sets/{urn}": Write(_SCORE_SET_WRITE),
    "PATCH /api/v1/score-sets-with-variants/{urn}": Write(_SCORE_SET_WRITE),
    "DELETE /api/v1/score-sets/{urn}": Write(_SCORE_SET_WRITE),
    "POST /api/v1/score-sets/{urn}/publish": Write(_SCORE_SET_WRITE),
    "POST /api/v1/score-sets/{urn}/variants/data": Write(_SCORE_SET_WRITE),
    "POST /api/v1/score-calibrations/": Write(_CALIBRATION_WRITE),
    "PUT /api/v1/score-calibrations/{urn}": Write(_CALIBRATION_WRITE),
    "DELETE /api/v1/score-calibrations/{urn}": Write(_CALIBRATION_WRITE),
    "DELETE /api/v1/score-calibrations/{urn}/controls": Write(_CALIBRATION_WRITE),
    "POST /api/v1/score-calibrations/{urn}/promote-to-primary": Write(_CALIBRATION_WRITE),
    "POST /api/v1/score-calibrations/{urn}/demote-from-primary": Write(_CALIBRATION_WRITE),
    "POST /api/v1/score-calibrations/{urn}/publish": Write(_CALIBRATION_WRITE),
    # Exempt.
    "POST /api/v1/controlled-keywords/search/{key}/{value}": Exempt(_REFERENCE_DATA),
    "POST /api/v1/taxonomies/search": Exempt(_REFERENCE_DATA),
    "POST /api/v1/hgvs/validate": Exempt(_EXTERNAL_LOOKUP),
    "POST /api/v1/publication-identifiers/search-external": Exempt(_EXTERNAL_LOOKUP),
    "POST /api/v1/orcid/token": Exempt("Exchanges an ORCID sign-in code with ORCID; reads no MaveDB records."),
    "PUT /api/v1/users//{id}": Exempt(_ADMIN_ONLY),
    "PUT /api/v1/users/me": Exempt(_OWN_ACCOUNT),
    "PUT /api/v1/users/me/has-logged-in": Exempt(_OWN_ACCOUNT),
    "POST /api/v1/users/me/access-keys": Exempt(_OWN_ACCOUNT),
    "POST /api/v1/users/me/access-keys/{role}": Exempt(_OWN_ACCOUNT),
    "DELETE /api/v1/users/me/access-keys/{access_key_id}": Exempt(_OWN_ACCOUNT),
    "GET /api/v1/api/version": Exempt("Returns the api's version string."),
    "GET /api/v1/alphafold-files/version": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/diseases/search": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/orcid/users/{orcid_id}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/genes/{symbol}": Exempt(
        "Looks the symbol up in HGNC, a network call the PR suite can't make. Its score sets come from a query that "
        "selects only published, current score sets."
    ),
    "GET /api/v1/hgvs/assemblies": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/fetch/{accession}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/gene/{gene}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/genes": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/genes/{gene}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/protein/{transcript}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/{assembly}/accessions": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/hgvs/{transcript}": Exempt(_EXTERNAL_LOOKUP),
    "GET /api/v1/refget/sequence/service-info": Exempt("Describes the refget service; reads no sequences or records."),
    "GET /api/v1/refget/sequence/{alias}": Exempt(
        "Serves sequences from SeqRepo, not the database, so no row-level rule applies to it."
    ),
    "GET /api/v1/refget/sequence/{alias}/metadata": Exempt(
        "Serves sequence metadata from SeqRepo, not the database, so no row-level rule applies to it."
    ),
    "GET /api/v1/seqrepo/metadata/{alias}": Exempt(
        "Serves sequence metadata from SeqRepo, not the database, so no row-level rule applies to it."
    ),
    "GET /api/v1/seqrepo/sequence/{alias}": Exempt(
        "Serves sequences from SeqRepo, not the database, so no row-level rule applies to it."
    ),
    "GET /api/v1/seqrepo/version": Exempt("Reports the SeqRepo release; reads no sequences or records."),
    "GET /api/v1/controlled-keywords/{key}": Exempt(_REFERENCE_DATA),
    "GET /api/v1/licenses/": Exempt(_REFERENCE_DATA),
    "GET /api/v1/licenses/active": Exempt(_REFERENCE_DATA),
    "GET /api/v1/licenses/{item_id}": Exempt(_REFERENCE_DATA),
    "GET /api/v1/taxonomies/": Exempt(_REFERENCE_DATA),
    "GET /api/v1/taxonomies/code/{item_id}": Exempt(_REFERENCE_DATA),
    "GET /api/v1/taxonomies/commonNames": Exempt(_REFERENCE_DATA),
    "GET /api/v1/taxonomies/speciesNames": Exempt(_REFERENCE_DATA),
    "GET /api/v1/taxonomies/{item_id}": Exempt(_REFERENCE_DATA),
    "GET /api/v1/job-runs/": Exempt(_ADMIN_ONLY),
    "GET /api/v1/job-runs/{urn}": Exempt(_ADMIN_ONLY),
    "GET /api/v1/pipelines/": Exempt(_ADMIN_ONLY),
    "GET /api/v1/pipelines/{urn}": Exempt(_ADMIN_ONLY),
    "GET /api/v1/permissions/user-is-permitted/{model_name}/{urn}/{action}": Exempt(
        "Answers true or false and carries no record data."
    ),
    "GET /docs": Exempt("FastAPI's interactive documentation."),
    "GET /docs/oauth2-redirect": Exempt("FastAPI's interactive documentation."),
    "GET /redoc": Exempt("FastAPI's interactive documentation."),
    "GET /openapi.json": Exempt("The API schema, which describes routes and holds no records."),
}
"""Every route the app serves, keyed ``"METHOD path"``. The tripwire fails on a served route missing from it."""

READS = {route: entry for route, entry in ROUTES.items() if isinstance(entry, Read)}
"""The routes the sweep and the positive control call."""
