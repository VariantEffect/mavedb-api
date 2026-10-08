# ruff: noqa: E402

import json
import re
from unittest.mock import AsyncMock, patch

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")
fastmcp = pytest.importorskip("fastmcp")

from fastapi import Depends, Request
from sqlalchemy.orm import Session

from mavedb import __version__
from mavedb.deps import get_db
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.documentation import MAVEDB_DOCS_URL, DocsPage, DocsSection
from mavedb.mcp_server import API_TOOLS
from mavedb.models.user import User
from mavedb.server_main import mcp_server
from tests.helpers.constants import TEST_USER
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    create_seq_score_set_with_mapped_variants,
    create_seq_score_set_with_variants,
    publish_score_set,
    seed_annotation_substrate,
)

MCP_HEADERS = {"Accept": "application/json, text/event-stream"}

PAGE = DocsPage(
    path="reference/score-calibrations.md",
    title="Score Calibrations",
    source="# Score Calibrations\n\nA calibration turns scores into evidence for a VUS.\n",
    abbreviations=("*[VUS]: Variant of Uncertain Significance",),
)
SECTION = DocsSection(
    page=PAGE,
    title="Evidence strength",
    text="Use `PS3` for abnormal variants.",
    url=f"{MAVEDB_DOCS_URL}reference/score-calibrations.html#evidence-strength",
)


async def call_tool(name, arguments):
    async with fastmcp.Client(mcp_server) as client:
        return await client.call_tool(name, arguments, raise_on_error=False)


def post_mcp(client, method, params=None, headers=None):
    message = {"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params else {})}
    return client.post("/mcp", headers={**MCP_HEADERS, **(headers or {})}, json=message)


def current_user_from_header(header, value):
    """A get_current_user that finds TEST_USER only on a request carrying the given header value."""

    def current_user(request: Request, db: Session = Depends(get_db)):
        if request.headers.get(header) != value:
            return None

        user = db.query(User).filter(User.username == TEST_USER["username"]).one()
        return UserData(user, user.roles)

    return current_user


@pytest.mark.asyncio
async def test_tools_are_the_curated_api_operations_and_the_docs_tools():
    async with fastmcp.Client(mcp_server) as client:
        tools = await client.list_tools()

    assert {tool.name for tool in tools} == set(API_TOOLS) | {"search_docs", "get_docs_page"}


@pytest.mark.asyncio
async def test_tool_names_fit_the_limit_clients_place_on_them():
    """Claude Code prefixes each tool with mcp__<server>__ and rejects names longer than 64 characters in total."""
    async with fastmcp.Client(mcp_server) as client:
        tools = await client.list_tools()

    for tool in tools:
        assert re.fullmatch(r"[a-z_]+", tool.name)
        assert len(f"mcp__mavedb__{tool.name}") <= 64


@pytest.mark.asyncio
async def test_server_identifies_as_mavedb():
    async with fastmcp.Client(mcp_server) as client:
        assert client.server_info.name == "MaveDB"
        assert client.server_info.version == __version__
        assert "MaveDB" in client.instructions


@pytest.mark.asyncio
async def test_api_tool_returns_what_the_api_returns(client, setup_router_db):
    experiment = create_experiment(client)

    result = await call_tool("get_experiment", {"urn": experiment["urn"]})

    assert not result.is_error
    assert json.loads(result.content[0].text) == client.get(f"/api/v1/experiments/{experiment['urn']}").json()


@pytest.mark.asyncio
async def test_api_tool_reports_an_api_error(client, setup_router_db):
    result = await call_tool("get_experiment", {"urn": "urn:mavedb:99999999-a"})

    assert result.is_error
    assert "404" in result.content[0].text


@pytest.mark.asyncio
async def test_api_tool_takes_a_variant_urn(session, data_provider, client, setup_router_db, data_files):
    """A variant URN contains a '#', which has to reach the API escaped."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    variant_urn = f"{score_set['urn']}#1"

    result = await call_tool("get_variant", {"urn": variant_urn})

    assert not result.is_error
    assert json.loads(result.content[0].text)["urn"] == variant_urn


@pytest.mark.asyncio
async def test_api_tool_follows_a_retired_urn_to_the_published_record(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = publish_score_set(client, score_set["urn"])

    result = await call_tool("get_score_set", {"urn": score_set["urn"]})

    assert not result.is_error
    assert json.loads(result.content[0].text)["urn"] == published_score_set["urn"]


@pytest.mark.asyncio
async def test_va_spec_tool_returns_a_study_result(session, data_provider, client, setup_router_db, data_files):
    """The VA-Spec operations have recursive response schemas."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    result = await call_tool("get_variant_study_result", {"urn": f"{score_set['urn']}#1"})

    assert not result.is_error
    study_result = json.loads(result.content[0].text)
    assert study_result["description"] == f"Variant effect study result for {score_set['urn']}#1."


@pytest.mark.asyncio
async def test_search_docs_returns_the_matching_sections():
    with patch("mavedb.lib.documentation.search_docs", new=AsyncMock(return_value=[SECTION])) as search:
        result = await call_tool("search_docs", {"query": "evidence", "limit": 3})

    search.assert_awaited_once_with("evidence", 3)
    assert result.data == [
        {
            "page": "reference/score-calibrations.md",
            "page_title": "Score Calibrations",
            "section": "Evidence strength",
            "url": f"{MAVEDB_DOCS_URL}reference/score-calibrations.html#evidence-strength",
            "text": "Use `PS3` for abnormal variants.",
        }
    ]


@pytest.mark.asyncio
async def test_search_docs_rejects_a_limit_out_of_range():
    result = await call_tool("search_docs", {"query": "evidence", "limit": 0})

    assert result.is_error


@pytest.mark.asyncio
async def test_get_docs_page_returns_the_page_markdown_and_where_it_is_deployed():
    with patch("mavedb.lib.documentation.get_docs_page", new=AsyncMock(return_value=PAGE)) as get_page:
        result = await call_tool("get_docs_page", {"path": "score-calibrations.md"})

    get_page.assert_awaited_once_with("score-calibrations.md")
    assert result.data == (
        "# Score Calibrations\n"
        "\n"
        "A calibration turns scores into evidence for a VUS.\n"
        "\n"
        "*[VUS]: Variant of Uncertain Significance\n"
        "\n"
        f"Source: {MAVEDB_DOCS_URL}reference/score-calibrations.html"
    )


@pytest.mark.asyncio
async def test_get_docs_page_reports_an_unknown_page():
    missing = ValueError("There is no documentation page 'nowhere'.")
    with patch("mavedb.lib.documentation.get_docs_page", new=AsyncMock(side_effect=missing)):
        result = await call_tool("get_docs_page", {"path": "nowhere"})

    assert result.is_error
    assert "There is no documentation page 'nowhere'." in result.content[0].text


def test_mcp_endpoint_serves_streamable_http(client, setup_router_db):
    experiment = create_experiment(client)

    initialized = post_mcp(
        client,
        "initialize",
        {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}},
    )
    assert initialized.status_code == 200
    assert initialized.json()["result"]["serverInfo"] == {"name": "MaveDB", "version": __version__}
    assert "mcp-session-id" not in initialized.headers

    called = post_mcp(client, "tools/call", {"name": "get_experiment", "arguments": {"urn": experiment["urn"]}})
    assert called.status_code == 200
    result = called.json()["result"]
    assert not result["isError"]
    assert json.loads(result["content"][0]["text"])["urn"] == experiment["urn"]


@pytest.mark.parametrize("header, value", [("Authorization", "Bearer orcid-token"), ("X-API-key", "api-key")])
def test_mcp_endpoint_calls_the_api_with_the_callers_credentials(client, setup_router_db, header, value):
    """An unpublished experiment is visible to its owner alone."""
    experiment = create_experiment(client)
    arguments = {"name": "get_experiment", "arguments": {"urn": experiment["urn"]}}

    with DependencyOverrider({get_current_user: current_user_from_header(header.lower(), value)}):
        with_credentials = post_mcp(client, "tools/call", arguments, headers={header: value})
        without_credentials = post_mcp(client, "tools/call", arguments)

    assert not with_credentials.json()["result"]["isError"]
    assert without_credentials.json()["result"]["isError"]
