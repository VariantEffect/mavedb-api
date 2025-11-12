# MaveDB API Copilot Instructions

## Core Directives & Control Principles

### Hierarchy of Operations
**These rules have the highest priority and must not be violated:**

1. **Primacy of User Directives**: A direct and explicit command from the user is the highest priority. If the user instructs to use a specific tool, edit a file, or perform a specific search, that command **must be executed without deviation**, even if other rules would suggest it is unnecessary.

2. **Factual Verification Over Internal Knowledge**: When a request involves information that could be version-dependent, time-sensitive, or requires specific external data (e.g., bioinformatics library documentation, latest genomics standards, API details), prioritize using tools to find the current, factual answer over relying on general knowledge.

3. **Adherence to MaveDB Philosophy**: In the absence of a direct user directive or the need for factual verification, all other rules regarding interaction, code generation, and modification must be followed within the context of bioinformatics and software development best practices.

### Interaction Philosophy for Bioinformatics
- **Code on Request Only**: Default response should be clear, natural language explanation. Do NOT provide code blocks unless explicitly asked, or if a small example is essential to illustrate a bioinformatics concept.
- **Direct and Concise**: Answers must be precise and free from unnecessary filler. Get straight to the solution for genomic data processing challenges.
- **Bioinformatics Best Practices**: All suggestions must align with established bioinformatics standards (HGVS, VRS, GA4GH) and proven genomics research practices.
- **Explain the Scientific "Why"**: Don't just provide code; explain the biological reasoning. Why is this approach standard in genomics? What scientific problem does this pattern solve?

## Related Instructions

**Domain-Specific Guidance**: This file provides MaveDB-specific development guidance. For specialized topics, reference these additional instruction files:

- **AI Safety & Ethics**: See `.github/instructions/ai-prompt-engineering-safety-best-practices.instructions.md` for comprehensive AI safety protocols, bias mitigation, responsible AI usage, and security frameworks
- **Python Standards**: Follow `.github/instructions/python.instructions.md` for Python-specific coding conventions, PEP 8 compliance, type hints, docstring requirements, and testing practices
- **Documentation Standards**: Reference `.github/instructions/markdown.instructions.md` for documentation formatting, content creation guidelines, and validation requirements
- **Prompt Engineering**: Use `.github/instructions/prompt.instructions.md` for creating effective prompts and AI interaction optimization
- **Instruction File Management**: See `.github/instructions/instructions.instructions.md` for guidelines on creating and maintaining instruction files

**Integration Principle**: These specialized files provide expert-level guidance in their respective domains. Apply their principles alongside the MaveDB-specific patterns documented here. When conflicts arise, prioritize the specialized file's guidance within its domain scope.

**Hierarchy for Conflicts**:
1. **User directives** (highest priority)
2. **MaveDB-specific bioinformatics patterns** (this file)  
3. **Domain-specific specialized files** (python.instructions.md, etc.)
4. **General best practices** (lowest priority)

## Architecture Overview

MaveDB API is a bioinformatics database API for Multiplex Assays of Variant Effect (MAVE) datasets. The architecture follows these key patterns:

### Core Domain Model
- **Hierarchical URN system**: ExperimentSet (`urn:mavedb:00000001`) → Experiment (`00000001-a`) → ScoreSet (`00000001-a-1`) → Variant (`00000001-a-1` + # + variant number)
- **Temporary URNs** during development: `tmp:uuid` format, converted to permanent URNs on publication
- **Resource lifecycle**: Draft → Published (with background worker processing)

### Service Architecture
- **FastAPI application** (`src/mavedb/server_main.py`) with router-based endpoint organization
- **Background worker** using ARQ/Redis for async processing (mapping, publication, annotation)
- **Multi-container setup**: API server, worker, PostgreSQL, Redis, external services (cdot-rest, dcd-mapping, seqrepo)
- **External bioinformatics services**: HGVS data providers, SeqRepo for sequence data, VRS mapping for variant representation

## Development Patterns

### Database & Models
- **SQLAlchemy 2.0** with declarative models in `src/mavedb/models/`
- **Alembic migrations** with manual migrations in `alembic/manual_migrations/`
- **Association tables** for many-to-many relationships (contributors, publications, keywords)
- **Enum classes** for controlled vocabularies (UserRole, ProcessingState, MappingState)

### Key Dependencies & Injections
```python
# Database session
def get_db() -> Generator[Session, Any, None]

# Worker queue
async def get_worker() -> AsyncGenerator[ArqRedis, Any]

# External data providers
def hgvs_data_provider() -> RESTDataProvider
def get_seqrepo() -> SeqRepo
```

### Authentication & Authorization
- **ORCID JWT tokens** and **API keys** for authentication
- **Role-based permissions** with `Action` enum and `assert_permission()` helper
- **User data context** available via `UserData` dataclass

### Router Patterns
- Endpoints organized by resource type in `src/mavedb/routers/`
- **Dependency injection** for auth, DB sessions, and external services
- **Structured exception handling** with custom exception types
- **Background job enqueueing** for publish/update operations

## Development Commands

### Environment Setup
```bash
# Local development with Docker
docker-compose -f docker-compose-dev.yml up --build -d

# Direct Python execution (requires env vars)
export PYTHONPATH="${PYTHONPATH}:`pwd`/src"
uvicorn mavedb.server_main:app --reload
```

### Testing
```bash
# Core dependencies only
poetry install --no-dev
poetry run pytest tests/

# Full test suite with optional dependencies
poetry install --with dev --extras server
poetry run pytest tests/ --cov=src
```

### Database Management
```bash
# Run migrations
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "Description"

# Manual migration (for complex data changes)
# Place in alembic/manual_migrations/ and reference in version file
```

## Project Conventions

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
*For general Python testing standards, see `.github/instructions/python.instructions.md`. The following are MaveDB-specific patterns:*

- **Test function naming**: Use descriptive names that reflect bioinformatics operations (e.g., `test_cannot_publish_score_set_without_variants`)
- **Fixtures**: Use `conftest.py` for shared fixtures, especially database and worker setup
- **Mocking**: Use `unittest.mock.patch` for external bioinformatics services and worker jobs
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