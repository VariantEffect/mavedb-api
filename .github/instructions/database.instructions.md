---
description: 'MaveDB database patterns — SQLAlchemy models, migrations, query conventions'
applyTo: 'src/mavedb/models/**/*.py,alembic/**/*.py'
---

# Database Patterns for MaveDB

## Model Structure

### Base Class
All models use `@as_declarative()` with a shared `Base` class providing:
- `id`: Integer primary key (auto-increment)
- `creation_date` / `modification_date`: Auto-managed timestamps
- `created_by_id` / `modified_by_id`: Foreign keys to users table

### Common Columns
Most resource models include:
- `urn`: Unique string identifier (temporary `tmp:<uuid>` or permanent `urn:mavedb:...`)
- `private`: Boolean flag for visibility control
- `approved`: Boolean for moderation status
- `published_date`: Nullable datetime, set on publication
- `extra_metadata`: JSONB column for extensible metadata

### Relationship Patterns

**One-to-many** (parent → children):
```python
class Experiment(Base):
    experiment_set_id = Column(Integer, ForeignKey("experiment_sets.id"), nullable=False)
    experiment_set = relationship("ExperimentSet", back_populates="experiments")
```

**Many-to-many with association tables** (simple join):
```python
# Table object for simple joins (no extra columns)
scoreset_contributors = Table(
    "scoreset_contributors",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id")),
    Column("contributor_id", ForeignKey("contributors.id")),
)
```

**Many-to-many with Association Object** (extra columns needed):
```python
# Class-based for additional attributes on the relationship
class ScoreSetPublicationIdentifierAssociation(Base):
    scoreset_id = Column(ForeignKey("scoresets.id"), primary_key=True)
    publication_identifier_id = Column(ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, default=False)
```

### Enum Columns
Use non-native enums stored as strings:
```python
processing_state = Column(
    Enum(ProcessingState, native_enum=False, create_constraint=True, length=32),
    nullable=True,
)
```

### JSONB Columns
Use `JSONB` for flexible structured data:
```python
mapped_scores = Column(JSONB, nullable=True)  # Post-mapping variant scores
extra_metadata = Column(JSONB, nullable=True)  # User-defined metadata
```

### Column Aliasing
When Python attribute names differ from DB column names:
```python
num_variants = Column("variant_count", Integer, nullable=False, default=0)
```

## Migrations (Alembic)

### Standard Migrations
```bash
alembic revision --autogenerate -m "add column to scoresets"
alembic upgrade head
```

### Manual Migrations
For complex data transformations that can't be auto-generated:
- Place scripts in `alembic/manual_migrations/`
- Import and call from the version file's `upgrade()` function
- Always include a `downgrade()` path

### Migration Conventions
- One logical change per migration
- Use descriptive messages: `"add variant_count to scoresets"` not `"update model"`
- Test migrations against a populated database when possible
- For enum changes: use `native_enum=False` to avoid PostgreSQL enum type ALTER issues

## Query Patterns

### Fetching
```python
# Single item (raises if not found)
item = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one()

# Single item (returns None if not found)
item = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()

# List with filters
items = db.scalars(
    select(ScoreSet)
    .where(ScoreSet.private.is_(False))
    .order_by(ScoreSet.urn)
).all()
```

### Visibility Filtering
Private resources return 404 (not 403) to avoid information leakage:
```python
item = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
if item is None:
    raise HTTPException(status_code=404)
if item.private and not user_has_access(user, item):
    raise HTTPException(status_code=404)  # 404, not 403
```

## Materialized Views
Used for pre-computed aggregations. Refresh via worker cron jobs defined in `src/mavedb/worker/settings.py`.
