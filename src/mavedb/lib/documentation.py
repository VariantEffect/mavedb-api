"""
Search and retrieval over the MaveDB user documentation.

The documentation is the MkDocs site in the mavedb-ui repository, deployed with the frontend at
``{MAVEDB_FRONTEND_URL}/docs/mavedb/``. Its pages are read as Markdown from a copy of that repository, which
``MAVEDB_DOCS_REPO`` names: a local checkout, or a URL that serves the repository's files. The default is the public
repository's main branch on GitHub. A page is read only if the site's navigation lists it, and it carries the
definitions of the abbreviations it uses from the site's glossary.
"""

import asyncio
import math
import os
import re
import unicodedata
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml
from aiocache import cached

from mavedb.constants import MAVEDB_FRONTEND_URL

MAVEDB_DOCS_URL = f"{MAVEDB_FRONTEND_URL}/docs/mavedb/"
MAVEDB_DOCS_REPO = os.getenv("MAVEDB_DOCS_REPO") or "https://raw.githubusercontent.com/VariantEffect/mavedb-ui/main"
DOCS_TTL_SECONDS = 60 * 60

# A query term found in a section's title counts this many times one found in its text.
TITLE_WEIGHT = 5

HEADING_RE = re.compile(r"(#{1,6})\s+(.+?)\s*#*\s*")
# An attribute list closing a heading, such as {#api-access-tokens}, which sets the heading's anchor.
HEADING_ID_RE = re.compile(r"\s*\{[^}]*#([\w-]+)[^}]*\}$")
# An abbreviation's definition, such as *[VUS]: Variant of Uncertain Significance.
ABBREVIATION_RE = re.compile(r"^\*\[([^\]]+)\]:.*$", re.MULTILINE)


@dataclass(frozen=True)
class DocsPage:
    """
    One page of the documentation.

    :param path: The page's path in the site's content directory, such as ``reference/score-calibrations.md``.
    :param title: The page's first heading.
    :param source: The page's Markdown, as written.
    :param abbreviations: The glossary's definitions of the abbreviations the page uses, one per line.
    """

    path: str
    title: str
    source: str
    abbreviations: tuple[str, ...] = ()

    @property
    def markdown(self) -> str:
        """The page's Markdown, followed by its abbreviations' definitions as MkDocs appends them."""
        return "\n\n".join(filter(None, [self.source.rstrip(), "\n".join(self.abbreviations)]))

    @property
    def url(self) -> str:
        # The site sets use_directory_urls to false, so a page is served at its path with .html in place of .md.
        return MAVEDB_DOCS_URL + self.path.removesuffix(".md") + ".html"


@dataclass(frozen=True)
class DocsSection:
    """
    A page's introduction, or the text under one of its headings.

    :param page: The page the section belongs to.
    :param title: The section's heading. For a page's introduction, this is the page title.
    :param text: The section's Markdown, up to the next heading.
    :param url: Where the section is deployed, with the anchor MkDocs gives its heading.
    """

    page: DocsPage
    title: str
    text: str
    url: str


async def fetch_docs_pages() -> list[DocsPage]:
    """
    Read every page in the documentation site's navigation, in navigation order. A local MAVEDB_DOCS_REPO is read
    afresh on every call, and a remote one is cached in memory for DOCS_TTL_SECONDS.

    :raises OSError: If a local repository lacks the site's configuration, one of its pages, or its glossary.
    :raises httpx.HTTPError: If a remote repository's files cannot be fetched.
    """
    if MAVEDB_DOCS_REPO.startswith(("http://", "https://")):
        return await _fetch_remote_docs_pages(MAVEDB_DOCS_REPO.rstrip("/"))

    async def read(path: str) -> str:
        return (Path(MAVEDB_DOCS_REPO) / path).read_text()

    return await _read_docs_pages(read)


async def search_docs(query: str, limit: int = 5) -> list[DocsSection]:
    """
    Rank documentation sections against a free-text query.

    A section scores each occurrence of a query term in its text, and TITLE_WEIGHT for each in its title, scaled by
    the term's inverse document frequency. A term found in every section scores nothing.

    :param query: The text to search for. Only its letters and digits are matched, case-insensitively.
    :param limit: The most sections to return.
    :return: The matching sections, best first. A section matching no query term is not returned.
    """
    sections = [section for page in await fetch_docs_pages() for section in _sections(page)]
    terms = set(_terms(query))
    text_counts = [Counter(_terms(section.text)) for section in sections]
    title_counts = [Counter(_terms(section.title)) for section in sections]
    document_frequency = Counter(
        term for text, title in zip(text_counts, title_counts) for term in terms if text[term] or title[term]
    )

    scores = [
        sum(
            math.log(len(sections) / document_frequency[term]) * (TITLE_WEIGHT * title[term] + text[term])
            for term in terms
            if document_frequency[term]
        )
        for text, title in zip(text_counts, title_counts)
    ]
    ranked = sorted(range(len(sections)), key=lambda i: scores[i], reverse=True)
    return [sections[i] for i in ranked[:limit] if scores[i] > 0]


async def get_docs_page(path: str) -> DocsPage:
    """
    Read one documentation page.

    :param path: The page's path in the site's content directory, such as ``reference/score-calibrations.md``. The
        ``.md`` suffix may be given as ``.html`` or left off, and a directory names its index page. A deployed URL, an
        anchor, and the leading ``./`` and ``../`` of a relative link are ignored, and a bare file name finds the one
        page with that name.
    :raises ValueError: If no page has this path. The message lists the pages there are.
    """
    pages = await fetch_docs_pages()
    by_name = {page.path.removesuffix(".md"): page for page in pages}

    name = path.split("/docs/mavedb/", 1)[-1].split("#", 1)[0]
    name = re.sub(r"^(\.{0,2}/)+", "", name).removesuffix(".md").removesuffix(".html").rstrip("/")
    for candidate in (name, f"{name}/index".lstrip("/")):
        if candidate in by_name:
            return by_name[candidate]

    matches = [page for page_name, page in by_name.items() if page_name.endswith(f"/{name}")]
    if name and len(matches) == 1:
        return matches[0]

    raise ValueError(
        f"There is no documentation page {path!r}. The pages are: {', '.join(page.path for page in pages)}"
    )


@cached(ttl=DOCS_TTL_SECONDS)
async def _fetch_remote_docs_pages(repo: str) -> list[DocsPage]:
    async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:

        async def read(path: str) -> str:
            response = await client.get(f"{repo}/{path}")
            response.raise_for_status()
            return response.text

        return await _read_docs_pages(read)


async def _read_docs_pages(read: Callable[[str], Awaitable[str]]) -> list[DocsPage]:
    """
    Read the site's pages through ``read``, which takes a path from the root of the repository.
    """
    config = yaml.load(await read("docs/mkdocs.yml"), Loader=_MkDocsConfigLoader)
    content_dir = f"docs/{config.get('docs_dir', 'docs')}"
    paths = _nav_paths(config["nav"])
    snippet_paths = _auto_appended_snippet_paths(config)
    files = await asyncio.gather(*(read(f"{content_dir}/{path}") for path in paths), *map(read, snippet_paths))
    sources, snippets = files[: len(paths)], files[len(paths) :]
    definitions = {match.group(1): match.group(0) for match in ABBREVIATION_RE.finditer("\n".join(snippets))}

    pages = []
    for path, source in zip(paths, sources):
        title = re.search(r"^# (.+?)\s*$", source, re.MULTILINE)
        # MkDocs marks an abbreviation wherever it stands as a whole word.
        used = tuple(line for name, line in definitions.items() if re.search(rf"\b{re.escape(name)}\b", source))
        pages.append(DocsPage(path, _split_heading(title.group(1))[0] if title else path, source, used))

    return pages


class _MkDocsConfigLoader(yaml.SafeLoader):
    """A safe YAML loader that reads the !!python/name tags in mkdocs.yml, which name MkDocs extensions, as None."""


_MkDocsConfigLoader.add_multi_constructor("tag:yaml.org,2002:python/", lambda loader, suffix, node: None)


def _auto_appended_snippet_paths(config: dict[str, Any]) -> list[str]:
    """
    The files pymdownx.snippets appends to every page, such as mavedb-ui's glossary, as paths from the root of the
    repository, where mavedb-ui runs MkDocs.
    """
    for extension in config.get("markdown_extensions") or []:
        if isinstance(extension, dict) and "pymdownx.snippets" in extension:
            return (extension["pymdownx.snippets"] or {}).get("auto_append", [])

    return []


def _nav_paths(nav: list[Any]) -> list[str]:
    paths: list[str] = []
    for item in nav:
        # An entry is a page path, or a mapping of a title to a page path, a link, or a nested list of entries.
        target = next(iter(item.values())) if isinstance(item, dict) else item
        if isinstance(target, list):
            paths += _nav_paths(target)
        elif target.endswith(".md"):
            paths.append(target)

    return paths


def _sections(page: DocsPage) -> list[DocsSection]:
    """
    Split a page into its introduction and the text under each heading below its title. A section with no text is
    left out.
    """
    headings: list[tuple[str, str | None]] = [(page.title, None)]
    bodies: list[list[str]] = [[]]
    used_anchors: set[str] = set()
    in_code_block = False

    for line in page.source.splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            in_code_block = not in_code_block

        heading = None if in_code_block else HEADING_RE.fullmatch(line)
        if heading is None:
            bodies[-1].append(line)
            continue

        # The title gets an anchor too, which a later heading of the same text must not reuse.
        title, anchor = _split_heading(heading.group(2))
        anchor = _unique_anchor(anchor or _slugify(title), used_anchors)
        if len(heading.group(1)) > 1:
            headings.append((title, anchor))
            bodies.append([])

    return [
        DocsSection(page, title, text, page.url + (f"#{anchor}" if anchor else ""))
        for (title, anchor), body in zip(headings, bodies)
        if (text := "\n".join(body).strip())
    ]


def _split_heading(heading: str) -> tuple[str, str | None]:
    """Split a heading's text from the anchor its attribute list sets, if it sets one."""
    anchor = HEADING_ID_RE.search(heading)
    return (heading[: anchor.start()], anchor.group(1)) if anchor else (heading, None)


def _slugify(heading: str) -> str:
    """Make the anchor Python-Markdown's toc extension makes from a heading, reduced first to its rendered text."""
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading)
    text = re.sub(r"[`*]", "", text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def _unique_anchor(anchor: str, used: set[str]) -> str:
    """Number a repeated or empty anchor the way Python-Markdown does: the first repeat gets _1, the next _2."""
    candidate, count = anchor, 0
    while not candidate or candidate in used:
        count += 1
        candidate = f"{anchor}_{count}"

    used.add(candidate)
    return candidate


def _terms(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())
