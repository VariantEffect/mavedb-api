---
description: 'Primary project guide for MaveDB API — architecture, conventions, domain patterns'
applyTo: '**'
---

# MaveDB API — Project Guide

MaveDB is a bioinformatics database API for Multiplex Assays of Variant Effect (MAVE) datasets. It stores, validates, maps, and publishes functional genomics data using standardized variant representations (HGVS, GA4GH VRS).

## Architecture

### Service Architecture
- **FastAPI application** — `src/mavedb/server_main.py`, router-based endpoint organization
- **Background worker** — ARQ/Redis for async processing (variant creation, mapping, publication, annotation)
- **Multi-container setup** — API server, worker, PostgreSQL, Redis, plus external services (cdot-rest, dcd-mapping, seqrepo)
- **Docker config** — `docker-compose-dev.yml` (6 services), multi-stage `Dockerfile`

### Core Domain Model
- **Hierarchical URN system**: ExperimentSet (`urn:mavedb:00000001`) → Experiment (`00000001-a`) → ScoreSet (`00000001-a-1`) → Variant (ScoreSet URN + `#` + number)
- **Temporary URNs** during development: `tmp:<uuid>` format, converted to permanent URNs on publication
- **Resource lifecycle**: Draft → Published (with background worker processing for variant creation, mapping, and annotation)
- **URN regex patterns**: `src/mavedb/lib/validation/urn_re.py`
- **URN generation**: `src/mavedb/lib/urns.py` and `temp_urns.py`

### Key Dependencies (Dependency Injection)
```python
def get_db() -> Generator[Session, Any, None]        # Database session
async def get_worker() -> AsyncGenerator[ArqRedis, Any]  # Worker queue
def hgvs_data_provider() -> RESTDataProvider          # HGVS validation
def get_seqrepo() -> SeqRepo                           # Sequence retrieval
```

## Project Structure

```
src/mavedb/
├── server_main.py          # FastAPI app setup, middleware, dependency injection
├── models/                 # SQLAlchemy ORM models
├── view_models/            # Pydantic request/response models
├── routers/                # API endpoint handlers
├── worker/                 # ARQ background jobs
│   ├── jobs.py             # Job implementations
│   └── settings.py         # Worker config, function registry, cron jobs
├── lib/                    # Shared utilities
│   ├── authentication.py   # ORCID JWT + API key auth
│   ├── authorization.py    # Permission checks
│   ├── exceptions.py       # Domain exceptions (MixedTargetError, NonexistentOrcidError, etc.)
│   ├── logging/            # LoggedRoute, logging_context(), save_to_logging_context()
│   ├── urns.py             # URN generation
│   └── validation/         # Validators, URN regex, HGVS checks, transform module
├── data_providers/         # External service clients
│   └── services.py         # ClinGen, SeqRepo, CDOT integrations
└── scripts/                # Operational Click-based CLI scripts
alembic/
├── versions/               # Migration files
└── manual_migrations/      # Complex data migration scripts
tests/
├── conftest.py             # Core fixtures (DB, auth, users)
├── helpers/
│   └── constants.py        # Test data constants
└── <mirror of src structure>
```

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Variables & functions | `snake_case` | `score_set_id`, `create_variants_for_score_set` |
| Classes | `PascalCase` | `ScoreSet`, `UserData`, `ProcessingState` |
| Constants | `UPPER_SNAKE_CASE` | `MAPPING_QUEUE_NAME`, `ROUTER_BASE_PREFIX` |
| Enum values | `snake_case` | `ProcessingState.success`, `MappingState.incomplete` |
| Database tables | `snake_case` | `scoresets`, `scoreset_contributors` |
| API endpoints | kebab-case | `/score-sets`, `/experiment-sets` |
| View model aliases | camelCase (auto) | Python `score_set` → JSON `scoreSet` |

## Living Documentation

Documentation and instruction files (`.github/instructions/`, `.claude/`, `CLAUDE.md`, `README.md`, etc.) are living documents. When making code changes that render existing instructions or documentation obsolete, update those files as part of the same changeset. Never leave stale docs behind.

## Commenting Guidelines

**Core principle: explain WHY, not WHAT. Focus on bioinformatics reasoning.**

Comment for:
- Complex bioinformatics algorithms (variant mapping, score normalization)
- Business logic rationale (why validation rules exist)
- External API constraints (rate limits, data format requirements)
- Non-obvious thresholds or configuration values

Do not comment obvious operations, variable assignments, or code that is self-explanatory.

## Error Handling

- **Structured logging**: Use `logger` with `extra=logging_context()` for correlation IDs via starlette-context
- **HTTP exceptions**: FastAPI `HTTPException` with appropriate status codes
- **Domain exceptions**: `src/mavedb/lib/exceptions.py` — `MixedTargetError`, `NonexistentOrcidError`, etc.
- **Worker errors**: `send_slack_error()` + full logging context
- **Validation errors**: Two distinct classes exist:
  - `src/mavedb/lib/validation/exceptions.py` — validation package exceptions
  - `src/mavedb/lib/exceptions.py` — legacy `ValidationError` (Django-style, used in some older code)

## External Integrations

| Service | Purpose | Client Location |
|---------|---------|----------------|
| HGVS / SeqRepo | Genomic sequence operations, variant validation | `data_providers/services.py` |
| DCD Mapping | Variant mapping and VRS transformation | `data_providers/services.py` |
| CDOT | Transcript/genomic coordinate conversion | REST service in Docker |
| GA4GH VRS | Variant representation standardization | Via DCD Mapping |
| ClinGen Allele Registry | Allele registration and lookup | `data_providers/services.py` |
| ClinGen Linked Data Hub | Functional annotation submission | `data_providers/services.py` |

## Development Commands

```bash
# Docker development environment
docker-compose -f docker-compose-dev.yml up --build -d

# Direct execution (requires env vars)
export PYTHONPATH="${PYTHONPATH}:`pwd`/src"
uvicorn mavedb.server_main:app --reload

# Run tests
poetry run pytest tests/

# Database migrations
alembic upgrade head
alembic revision --autogenerate -m "Description"

# Operational scripts (Click CLI, dry-run by default)
poetry run python -m mavedb.scripts.<script_name>
```

## Key Reference Files

- [score_set.py](src/mavedb/models/score_set.py) — Primary data model patterns
- [score_sets.py](src/mavedb/routers/score_sets.py) — Complex router with worker integration
- [jobs.py](src/mavedb/worker/jobs.py) — Background processing patterns
- [score_set.py](src/mavedb/view_models/score_set.py) — Pydantic model hierarchy
- [server_main.py](src/mavedb/server_main.py) — App setup and dependency injection
- [authentication.py](src/mavedb/lib/authentication.py) — Auth patterns
- [conftest.py](tests/conftest.py) — Test fixtures and database setup
