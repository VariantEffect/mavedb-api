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

### Naming Conventions
- **Variables & functions**: `snake_case` (e.g., `score_set_id`, `create_variants_for_score_set`)
- **Classes**: `PascalCase` (e.g., `ScoreSet`, `UserData`, `ProcessingState`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAPPING_QUEUE_NAME`, `DEFAULT_LDH_SUBMISSION_BATCH_SIZE`)
- **Enum values**: `snake_case` (e.g., `ProcessingState.success`, `MappingState.incomplete`)
- **Database tables**: `snake_case` with descriptive association table names (e.g., `scoreset_contributors`, `experiment_set_doi_identifiers`)
- **API endpoints**: kebab-case paths (e.g., `/score-sets`, `/experiment-sets`)

### Documentation Conventions
*For general Python documentation standards, see `.github/instructions/python.instructions.md`. The following are MaveDB-specific additions:*

- **Algorithm explanations**: Include comments explaining complex logic, especially URN generation and bioinformatics operations
- **Design decisions**: Comment on why certain architectural choices were made
- **External dependencies**: Explain purpose of external bioinformatics libraries (HGVS, SeqRepo, etc.)
- **Bioinformatics context**: Document biological reasoning behind genomic data processing patterns

### Commenting Guidelines
**Core Principle: Write self-explanatory code. Comment only to explain WHY, not WHAT.**

**✅ WRITE Comments For:**
- **Complex bioinformatics algorithms**: Variant mapping algorithms, external service interactions
- **Business logic**: Why specific validation rules exist, regulatory requirements
- **External API constraints**: Rate limits, data format requirements
- **Non-obvious calculations**: Score normalization, statistical methods
- **Configuration values**: Why specific timeouts, batch sizes, or thresholds were chosen

**❌ AVOID Comments For:**
- **Obvious operations**: Variable assignments, simple loops, basic conditionals
- **Redundant descriptions**: Comments that repeat what the code clearly shows
- **Outdated information**: Comments that don't match current implementation

### Error Handling Conventions
- **Structured logging**: Always use `logger` with `extra=logging_context()` for correlation IDs
- **HTTP exceptions**: Use FastAPI `HTTPException` with appropriate status codes and descriptive messages
- **Custom exceptions**: Define domain-specific exceptions in `src/mavedb/lib/exceptions.py`
- **Worker job errors**: Send Slack notifications via `send_slack_error()` and log with full context
- **Validation errors**: Use Pydantic validators and raise `ValueError` with clear messages

### Code Style and Organization Conventions
*For general Python style conventions, see `.github/instructions/python.instructions.md`. The following are MaveDB-specific patterns:*

- **Async patterns**: Use `async def` for I/O operations, regular functions for CPU-bound work
- **Database operations**: Use SQLAlchemy 2.0 style with `session.scalars(select(...)).one()`
- **Pydantic models**: Separate request/response models with clear inheritance hierarchies
- **Bioinformatics data flow**: Structure code to clearly show genomic data transformations

### Testing Conventions
*For testing philosophy, mocking boundaries, and conventions see `.github/instructions/testing.instructions.md`. For general Python testing standards, see `.github/instructions/python.instructions.md`. The following are MaveDB-specific patterns:*

- **Test function naming**: Use descriptive names that reflect bioinformatics operations (e.g., `test_cannot_publish_score_set_without_variants`)
- **Fixtures**: Use `conftest.py` for shared fixtures, especially database and worker setup
- **Mocking**: Mock only at system boundaries (external services, Redis/ARQ, Slack). Do not mock internal helpers or `update_progress`
- **Constants**: Define test data including genomic sequences and variants in `tests/helpers/constants.py`
- **Integration testing**: Test full bioinformatics workflows including external service interactions

## Codebase Conventions

### URN Validation
- Use regex patterns from `src/mavedb/lib/validation/urn_re.py`
- Validate URNs in Pydantic models with `@field_validator`
- URN generation logic in `src/mavedb/lib/urns.py` and `temp_urns.py`

### Worker Jobs (ARQ/Redis)
- **Job definitions**: All background jobs in `src/mavedb/worker/jobs.py`
- **Settings**: Worker configuration in `src/mavedb/worker/settings.py` with function registry and cron jobs
- **Job patterns**: 
  - Use `setup_job_state()` for logging context with correlation IDs
  - Implement exponential backoff with `enqueue_job_with_backoff()`
  - Handle database sessions within job context
  - Send Slack notifications on failures via `send_slack_error()`
- **Key job types**: 
  - `create_variants_for_score_set` - Process uploaded CSV data
  - `map_variants_for_score_set` - External variant mapping via VRS
  - `submit_score_set_mappings_to_*` - Submit to external annotation services
- **Enqueueing**: Use `ArqRedis.enqueue_job()` from routers with correlation ID for request tracing

### View Models (Pydantic)
- **Base model** (`src/mavedb/view_models/base/base.py`) converts empty strings to None and uses camelCase aliases
- **Inheritance patterns**: `Base` → `Create` → `Modify` → `Saved` model hierarchy
- **Field validation**: Use `@field_validator` for single fields, `@model_validator(mode="after")` for cross-field validation
- **URN validation**: Validate URNs with regex patterns from `urn_re.py` in field validators
- **Transform functions**: Use functions in `validation/transform.py` for complex data transformations
- **Separate models**: Request (`Create`, `Modify`) vs response (`Saved`) models with different field requirements

### External Integrations
- **HGVS/SeqRepo** for genomic sequence operations
- **DCD Mapping** for variant mapping and VRS transformation
- **CDOT** for transcript/genomic coordinate conversion
- **GA4GH VRS** for variant representation standardization
- **ClinGen services** for allele registry and linked data hub submissions

## Key Files to Reference
- `src/mavedb/models/score_set.py` - Primary data model patterns
- `src/mavedb/routers/score_sets.py` - Complex router with worker integration
- `src/mavedb/worker/jobs.py` - Background processing patterns  
- `src/mavedb/view_models/score_set.py` - Pydantic model hierarchy examples
- `src/mavedb/server_main.py` - Application setup and dependency injection
- `src/mavedb/data_providers/services.py` - External service integration patterns
- `src/mavedb/lib/authentication.py` - Authentication and authorization patterns
- `tests/conftest.py` - Test fixtures and database setup
- `docker-compose-dev.yml` - Service architecture and dependencies
