"""
The MaveDB MCP server, served over Streamable HTTP at ``/mcp`` on the API.

Its tools are a curated set of the API's read-only operations, generated from the OpenAPI schema by FastMCP, and
two tools over the user documentation. An API tool runs in-process against the application with the caller's
``Authorization`` or ``X-API-key`` header, so it returns what the same request to the REST API would.
"""

from typing import Annotated

import httpx2
from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.providers.openapi import MCPType, OpenAPIProvider
from fastmcp.utilities.lifespan import combine_lifespans
from pydantic import Field

from mavedb import __version__
from mavedb.lib import documentation

INSTRUCTIONS = """\
MaveDB is a public repository of data from Multiplexed Assays of Variant Effect (MAVEs), such as deep mutational \
scans and massively parallel reporter assays.

Records nest as experiment sets (urn:mavedb:00000001), experiments (urn:mavedb:00000001-a), score sets \
(urn:mavedb:00000001-a-1) and variants (urn:mavedb:00000001-a-1#1). Unpublished records have tmp: URNs.

To find datasets, use search_score_sets, or get_gene for a gene symbol. get_score_set describes a dataset, and \
get_score_set_calibrations gives the ranges that turn its scores into functional or clinical evidence. For how \
MaveDB works (data formats, calibrations, variant mapping, accession numbers), use search_docs and get_docs_page.

Tools return what the REST API returns to the same caller: published data, plus any private records the caller may \
see when the client sends an ORCID token or a MaveDB API key."""

# Each tool's name, and the method and path of the API operation it calls.
API_TOOLS: dict[str, tuple[str, str]] = {
    "search_score_sets": ("POST", "/api/v1/score-sets/search"),
    "get_score_set_search_filter_options": ("POST", "/api/v1/score-sets/search/filter-options"),
    "list_recently_published_score_sets": ("GET", "/api/v1/score-sets/recently-published"),
    "get_score_set": ("GET", "/api/v1/score-sets/{urn}"),
    "get_score_set_calibrations": ("GET", "/api/v1/score-calibrations/score-set/{score_set_urn}"),
    "get_primary_score_calibration": ("GET", "/api/v1/score-calibrations/score-set/{score_set_urn}/primary"),
    "get_score_calibration": ("GET", "/api/v1/score-calibrations/{urn}"),
    "search_experiments": ("POST", "/api/v1/experiments/search"),
    "get_experiment": ("GET", "/api/v1/experiments/{urn}"),
    "get_experiment_score_sets": ("GET", "/api/v1/experiments/{urn}/score-sets"),
    "get_experiment_set": ("GET", "/api/v1/experiment-sets/{urn}"),
    "get_collection": ("GET", "/api/v1/collections/{urn}"),
    "get_variant": ("GET", "/api/v1/variants/{urn}"),
    "lookup_variants_by_vrs_id": ("GET", "/api/v1/variants/vrs/{identifier}"),
    "get_variant_study_result": ("GET", "/api/v1/variants/{urn}/va/study-result"),
    "get_variant_functional_statement": ("GET", "/api/v1/variants/{urn}/va/functional-statement"),
    "get_variant_pathogenicity_statement": ("GET", "/api/v1/variants/{urn}/va/pathogenicity-statement"),
    "get_clingen_allele_measurements": ("GET", "/api/v1/clingen-alleles/{clingen_allele_id}/measurements"),
    "get_gene": ("GET", "/api/v1/genes/{symbol}"),
    "search_publications": ("POST", "/api/v1/publication-identifiers/search"),
    "get_publication": ("GET", "/api/v1/publication-identifiers/{identifier}"),
    "validate_hgvs": ("POST", "/api/v1/hgvs/validate"),
    "list_transcripts_for_gene": ("GET", "/api/v1/hgvs/gene/{gene}"),
    "get_transcript": ("GET", "/api/v1/hgvs/{transcript}"),
    "convert_transcript_to_protein": ("GET", "/api/v1/hgvs/protein/{transcript}"),
    "count_published_records": ("GET", "/api/v1/statistics/record/{model}/published/count"),
    "count_variants": ("GET", "/api/v1/statistics/variant/count"),
}


async def search_docs(
    query: Annotated[str, Field(description="Words to search for.")],
    limit: Annotated[int, Field(ge=1, le=20, description="The most sections to return.")] = 5,
) -> list[dict[str, str]]:
    """
    Search the MaveDB user documentation, which covers finding, downloading and submitting data, data formats, score
    calibrations, variant mapping, accession numbers and the API. Returns the best-matching sections with their text,
    the path of the page each belongs to and its URL.
    """
    return [
        {
            "page": section.page.path,
            "page_title": section.page.title,
            "section": section.title,
            "url": section.url,
            "text": section.text,
        }
        for section in await documentation.search_docs(query, limit)
    ]


async def get_docs_page(
    path: Annotated[
        str,
        Field(
            description=(
                "The page's path, as search_docs gives it, such as reference/score-calibrations.md, or a link from "
                "another page."
            )
        ),
    ],
) -> str:
    """Read a whole page of the MaveDB user documentation, as Markdown."""
    page = await documentation.get_docs_page(path)
    return f"{page.markdown.rstrip()}\n\nSource: {page.url}"


def mount_mcp_server(app: FastAPI) -> FastMCP:
    """
    Serve the MCP server over Streamable HTTP at ``/mcp`` on the application, adding its lifespan to the
    application's.

    Tools are generated from the application's OpenAPI schema when this is called, so call it after every router has
    been included.

    :raises LookupError: If a tool in API_TOOLS calls an operation the application does not serve.
    """
    openapi = app.openapi()
    operation_ids = {
        (method.upper(), path): operation["operationId"]
        for path, path_item in openapi["paths"].items()
        for method, operation in path_item.items()
    }
    for name, operation in API_TOOLS.items():
        if operation not in operation_ids:
            raise LookupError(f"MCP tool {name} calls {' '.join(operation)}, which is not an operation of this API.")

    tool_operations = set(API_TOOLS.values())
    provider = OpenAPIProvider(
        openapi_spec=openapi,
        client=httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app),
            base_url="http://apiserver",
            # A read naming a URN that publication retired is answered with a redirect to the record's current URN
            # (see mavedb.lib.urn_redirects).
            follow_redirects=True,
            event_hooks={"request": [_forward_authorization]},
        ),
        route_map_fn=lambda route, mcp_type: (
            MCPType.TOOL if (route.method, route.path) in tool_operations else MCPType.EXCLUDE
        ),
        mcp_names={operation_ids[operation]: name for name, operation in API_TOOLS.items()},
        # Validating responses would also put each operation's full response schema in its tool definition: more
        # than 100,000 characters for each VA-Spec operation.
        validate_output=False,
    )

    mcp_server = FastMCP(name="MaveDB", version=__version__, instructions=INSTRUCTIONS, providers=[provider])
    mcp_server.tool(search_docs)
    mcp_server.tool(get_docs_page)

    mcp_app = mcp_server.http_app(path="/mcp", stateless_http=True, json_response=True)
    app.router.routes.extend(mcp_app.routes)
    app.router.lifespan_context = combine_lifespans(app.router.lifespan_context, mcp_app.lifespan)
    return mcp_server


async def _forward_authorization(request: httpx2.Request) -> None:
    # FastMCP passes the MCP request's headers on to the API, X-API-key among them, but withholds credentials such
    # as Authorization.
    authorization = get_http_headers(include={"authorization"}).get("authorization")
    if authorization:
        request.headers["authorization"] = authorization
