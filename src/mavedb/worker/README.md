# ARQ Worker System

The worker is a separate process from the FastAPI API server, connected via Redis (ARQ). It processes background jobs for variant creation, genomic mapping, external service annotation, and system maintenance.

## Quick Start: "I want to..."

| Goal | Start Here |
|------|-----------|
| Understand the whole system | [Job System Overview](jobs_overview.md) |
| Add a new job to an existing pipeline | [Job Registry — Adding a Pipeline Job](job_registry.md#adding-a-pipeline-job) |
| Add a standalone or cron job | [Job Registry — Adding a Standalone/Cron Job](job_registry.md#adding-a-standalonecron-job) |
| Define a new pipeline | [Pipeline Management — Defining a New Pipeline](pipeline_management.md#defining-a-new-pipeline) |
| Understand how decorators work | [Job Decorators](job_decorators.md) |
| Understand how managers work | [Job Managers](job_managers.md) |
| Learn coding patterns and conventions | [Best Practices & Patterns](best_practices.md) |

## Architecture Overview

```
┌───────────┐    enqueue    ┌───────┐    dequeue     ┌────────────────────────┐
│  Router   │ ──────────►   │ Redis │  ──────────►   │      ARQ Worker        │
│ (FastAPI) │               │ (ARQ) │                │                        │
└───────────┘               └───────┘                │  ┌──────────────────┐  │
     │                                               │  │   Decorators     │  │
     │  PipelineFactory                              │  │   (lifecycle)    │  │
     │  creates Pipeline,                            │  └────────┬─────────┘  │
     │  JobRun, and                                  │           │            │
     │  JobDependency                                │  ┌────────▼─────────┐  │
     │  records in DB                                │  │  Job Function    │  │
     │                                               │  │  (business)      │  │
     └──► PostgreSQL ◄───────────────────────────────│  └────────┬─────────┘  │
                                                     │           │            │
                                                     │    ┌──────▼──────┐     │
                                                     │    │ PostgreSQL  │     │
                                                     │    │ (state)     │     │
                                                     │    └─────────────┘     │
                                                     └────────────────────────┘
```

The system has **two layers**:

1. **Infrastructure layer** (`lib/decorators/`, `lib/managers/`): Handles job lifecycle, state persistence, error recovery, pipeline coordination. Developers rarely modify this.
2. **Business layer** (`jobs/`): Implements domain logic. This is where most new code goes.

Two types of work:
- **Pipeline jobs**: Multi-step workflows with dependency management (e.g., create → map → annotate variants). Orchestrated by `PipelineManager`.
- **Standalone jobs**: Independent tasks or cron-scheduled maintenance (e.g., cleanup stalled jobs, refresh materialized views).

## Directory Structure

```
worker/
├── README.md                          # This file
├── jobs_overview.md                   # System architecture and end-to-end flows
├── job_decorators.md                  # Decorator usage and internals
├── job_managers.md                    # Manager classes and their APIs
├── pipeline_management.md             # Pipeline lifecycle and coordination
├── job_registry.md                    # Registration and step-by-step how-to guides
├── best_practices.md                  # Coding patterns and conventions
│
├── jobs/                              # ── Business Layer ──
│   ├── registry.py                    # Central registry of all job functions
│   ├── variant_processing/            # Variant creation and mapping jobs
│   │   ├── creation.py                #   create_variants_for_score_set
│   │   └── mapping.py                 #   map_variants_for_score_set
│   ├── external_services/             # Integration with external APIs
│   │   ├── clingen.py                 #   CAR and LDH submission
│   │   ├── clinvar.py                 #   ClinVar control refresh
│   │   ├── gnomad.py                  #   gnomAD variant linking
│   │   ├── hgvs.py                    #   HGVS annotation
│   │   ├── uniprot.py                 #   UniProt mapping submission/polling
│   │   └── variant_translation.py     #   Variant translation population
│   ├── data_management/               # Database maintenance jobs
│   │   └── views.py                   #   Materialized view refresh
│   ├── pipeline_management/           # Pipeline orchestration jobs
│   │   └── start_pipeline.py          #   Pipeline entrypoint job
│   ├── system/                        # System maintenance jobs
│   │   └── cleanup.py                 #   Stalled job cleanup (cron)
│   └── utils/                         # Shared job utilities
│       ├── setup.py                   #   validate_job_params()
│       └── constants.py               #   Job-level constants
│
├── lib/                               # ── Infrastructure Layer ──
│   ├── decorators/                    # Job/pipeline lifecycle decorators
│   │   ├── job_management.py          #   @with_job_management
│   │   ├── pipeline_management.py     #   @with_pipeline_management
│   │   ├── job_guarantee.py           #   @with_guaranteed_job_run_record
│   │   └── utils.py                   #   Session management, test mode detection
│   └── managers/                      # State management classes
│       ├── base_manager.py            #   BaseManager (DB + Redis init)
│       ├── job_manager.py             #   JobManager (individual job lifecycle)
│       ├── pipeline_manager.py        #   PipelineManager (pipeline coordination)
│       ├── constants.py               #   Status grouping constants
│       ├── exceptions.py              #   Exception hierarchy
│       ├── types.py                   #   TypedDicts (RetryHistoryEntry, PipelineProgress)
│       └── utils.py                   #   Dependency checking helpers, classify_exception()
│
└── settings/                          # ARQ worker configuration
    ├── worker.py                      #   ArqWorkerSettings class
    ├── lifecycle.py                   #   Startup/shutdown/job hooks, standalone_ctx()
    ├── redis.py                       #   Redis connection settings
    └── constants.py                   #   Environment variable handling
```

## Related Files Outside This Directory

| File | Purpose |
|------|---------|
| `src/mavedb/lib/workflow/definitions.py` | `PIPELINE_DEFINITIONS` — declarative pipeline and job definitions |
| `src/mavedb/lib/workflow/pipeline_factory.py` | `PipelineFactory` — creates Pipeline + JobRun + JobDependency records |
| `src/mavedb/lib/workflow/job_factory.py` | `JobFactory` — creates individual JobRun records |
| `src/mavedb/lib/types/workflow.py` | `JobExecutionOutcome`, `JobDefinition`, `PipelineDefinition` types |
| `src/mavedb/models/pipeline.py` | `Pipeline` ORM model |
| `src/mavedb/models/job_run.py` | `JobRun` ORM model |
| `src/mavedb/models/job_dependency.py` | `JobDependency` ORM model |
| `src/mavedb/models/enums/job_pipeline.py` | `JobStatus`, `PipelineStatus`, `DependencyType`, `FailureCategory`, `JobType` enums |
| `src/mavedb/routers/score_sets.py` | Primary router that triggers the `validate_map_annotate_score_set` pipeline |
| `src/mavedb/scripts/run_pipeline.py` | CLI script for running pipelines outside the API |
| `src/mavedb/scripts/run_job.py` | CLI script for running standalone jobs outside the API |
| `tests/worker/` | Test suite mirroring this directory structure |
