# ruff: noqa: E402

from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("aiocache")

import httpx
import pytest_asyncio

from mavedb.lib import documentation
from mavedb.lib.documentation import MAVEDB_DOCS_URL, fetch_docs_pages, get_docs_page, search_docs

REMOTE_REPO = "https://raw.githubusercontent.com/VariantEffect/mavedb-ui/docs-branch"
CALIBRATIONS_URL = MAVEDB_DOCS_URL + "reference/score-calibrations.html"

# A mavedb-ui repository's docs directory, by path.
DOCS = {
    "mkdocs.yml": """\
site_name: MaveDB Documentation
docs_dir: content
use_directory_urls: false
markdown_extensions:
  - pymdownx.emoji:
      emoji_index: !!python/name:material.extensions.emoji.twemoji
  - pymdownx.snippets:
      auto_append:
        - docs/includes/glossary.md
nav:
  - Home: index.md
  - Reference:
      - reference/index.md
      - Score Calibrations: reference/score-calibrations.md
  - GitHub: https://github.com/VariantEffect/mavedb-ui
""",
    "content/index.md": """\
# Welcome to the MaveDB Documentation

MaveDB is the open-access database for MAVE data.

## Citing MaveDB

If you use MaveDB in your research, please cite us.
""",
    "content/reference/index.md": """\
# Reference

Technical reference material.
""",
    "content/reference/score-calibrations.md": """\
# Score Calibrations

A calibration turns scores into evidence for a VUS. See the [reference](index.md).

## Evidence strength

| Strength | Code  |
|----------|-------|
| Strong   | `PS3` |

```python
# Read a calibration
calibration = client.get("score-calibrations")
```

## Using `hgvs_nt` values

Each variant has one.

## Evidence strength

A heading repeated within a page.

## API keys {#api-access-tokens}

Generate a key from your profile.

## Heading with no text
""",
    "content/orphan.md": "# Not in the navigation\n\nThis page is never read.\n",
    "includes/glossary.md": "*[VUS]: Variant of Uncertain Significance\n*[DMS]: Deep Mutational Scanning\n",
}


@pytest.fixture
def local_checkout(tmp_path, monkeypatch):
    """Write DOCS into a local mavedb-ui checkout, and point MAVEDB_DOCS_REPO at it."""
    for path, text in DOCS.items():
        file = tmp_path / "docs" / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(text)

    monkeypatch.setattr(documentation, "MAVEDB_DOCS_REPO", str(tmp_path))
    return tmp_path


@pytest_asyncio.fixture
async def remote_repo(monkeypatch):
    """Serve DOCS from REMOTE_REPO, point MAVEDB_DOCS_REPO at it, and start with nothing cached."""
    monkeypatch.setattr(documentation, "MAVEDB_DOCS_REPO", REMOTE_REPO)
    await documentation._fetch_remote_docs_pages.cache.clear()

    async def get(url):
        path = url.removeprefix(f"{REMOTE_REPO}/docs/")
        status, text = (200, DOCS[path]) if path in DOCS else (404, "")
        return httpx.Response(status, text=text, request=httpx.Request("GET", url))

    with patch.object(httpx.AsyncClient, "get", new=AsyncMock(side_effect=get)) as fetch:
        yield fetch

    await documentation._fetch_remote_docs_pages.cache.clear()


@pytest.mark.asyncio
async def test_remote_repo_is_read_through_its_navigation(remote_repo):
    pages = await fetch_docs_pages()

    assert [page.path for page in pages] == ["index.md", "reference/index.md", "reference/score-calibrations.md"]
    assert [fetch.args[0] for fetch in remote_repo.await_args_list] == [
        f"{REMOTE_REPO}/docs/mkdocs.yml",
        f"{REMOTE_REPO}/docs/content/index.md",
        f"{REMOTE_REPO}/docs/content/reference/index.md",
        f"{REMOTE_REPO}/docs/content/reference/score-calibrations.md",
        f"{REMOTE_REPO}/docs/includes/glossary.md",
    ]


@pytest.mark.asyncio
async def test_remote_repo_is_fetched_once_while_cached(remote_repo):
    await search_docs("calibration")
    await get_docs_page("index.md")

    assert remote_repo.await_count == 5


@pytest.mark.asyncio
async def test_local_checkout_is_read_afresh_on_every_call(local_checkout):
    await fetch_docs_pages()
    (local_checkout / "docs" / "content" / "index.md").write_text("# Rewritten\n")

    pages = await fetch_docs_pages()

    assert pages[0].source == "# Rewritten\n"


@pytest.mark.asyncio
async def test_missing_local_checkout_names_the_file_it_needs(tmp_path, monkeypatch):
    monkeypatch.setattr(documentation, "MAVEDB_DOCS_REPO", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="mkdocs.yml"):
        await fetch_docs_pages()


@pytest.mark.asyncio
async def test_page_is_titled_by_its_first_heading_and_linked_where_it_is_deployed(local_checkout):
    pages = await fetch_docs_pages()

    assert [page.title for page in pages] == ["Welcome to the MaveDB Documentation", "Reference", "Score Calibrations"]
    assert pages[2].url == CALIBRATIONS_URL


@pytest.mark.asyncio
async def test_page_ends_with_the_glossary_definitions_of_the_abbreviations_it_uses(local_checkout):
    page = await get_docs_page("reference/score-calibrations.md")

    assert page.markdown == page.source.rstrip() + "\n\n*[VUS]: Variant of Uncertain Significance"


@pytest.mark.asyncio
async def test_page_using_no_abbreviations_is_its_source(local_checkout):
    page = await get_docs_page("index.md")

    assert page.markdown == page.source.rstrip()


@pytest.mark.asyncio
async def test_glossary_definitions_are_not_searched(local_checkout):
    assert await search_docs("uncertain significance") == []


@pytest.mark.asyncio
async def test_sections_are_split_at_the_headings_below_the_title(local_checkout):
    sections = await search_docs("calibration evidence strength variant key", limit=20)

    assert {(section.title, section.url) for section in sections} == {
        ("Score Calibrations", CALIBRATIONS_URL),
        ("Evidence strength", f"{CALIBRATIONS_URL}#evidence-strength"),
        ("Using `hgvs_nt` values", f"{CALIBRATIONS_URL}#using-hgvs_nt-values"),
        ("Evidence strength", f"{CALIBRATIONS_URL}#evidence-strength_1"),
        ("API keys", f"{CALIBRATIONS_URL}#api-access-tokens"),
    }


@pytest.mark.asyncio
async def test_section_keeps_its_markdown_including_code_and_tables(local_checkout):
    [section] = await search_docs("strong", limit=20)

    assert section.title == "Evidence strength"
    assert "| Strong   | `PS3` |" in section.text
    assert "# Read a calibration" in section.text


@pytest.mark.asyncio
async def test_heading_with_no_text_is_left_out(local_checkout):
    sections = await search_docs("heading", limit=20)

    assert [section.title for section in sections] == ["Evidence strength"]


@pytest.mark.asyncio
async def test_search_ranks_a_title_match_above_a_text_match(local_checkout):
    results = await search_docs("evidence")

    assert [section.url for section in results] == [
        f"{CALIBRATIONS_URL}#evidence-strength",
        f"{CALIBRATIONS_URL}#evidence-strength_1",
        CALIBRATIONS_URL,
    ]


@pytest.mark.asyncio
async def test_search_leaves_out_sections_matching_no_term(local_checkout):
    results = await search_docs("Cite")

    assert [section.url for section in results] == [f"{MAVEDB_DOCS_URL}index.html#citing-mavedb"]


@pytest.mark.asyncio
async def test_search_returns_at_most_limit_sections(local_checkout):
    results = await search_docs("mavedb", limit=1)

    assert len(results) == 1


@pytest.mark.asyncio
async def test_search_for_no_letters_or_digits_finds_nothing(local_checkout):
    assert await search_docs("?!") == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "reference/score-calibrations.md",
        "reference/score-calibrations",
        "reference/score-calibrations.html",
        "/reference/score-calibrations.md#evidence-strength",
        CALIBRATIONS_URL,
        "../reference/score-calibrations.md",
        "score-calibrations.md",
    ],
)
async def test_page_is_found_from_any_form_of_its_path(local_checkout, path):
    page = await get_docs_page(path)

    assert page.path == "reference/score-calibrations.md"


@pytest.mark.asyncio
@pytest.mark.parametrize("path, page_path", [("reference", "reference/index.md"), ("", "index.md")])
async def test_directory_names_its_index_page(local_checkout, path, page_path):
    page = await get_docs_page(path)

    assert page.path == page_path


@pytest.mark.asyncio
async def test_unknown_page_is_reported_with_the_pages_there_are(local_checkout):
    with pytest.raises(ValueError, match="reference/score-calibrations.md"):
        await get_docs_page("reference/calibrations")
