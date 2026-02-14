# Test Helpers

This directory contains shared test helper code used across the MaveDB test suite.

## Purpose

Use this area for reusable testing support that should not live inside a single test module, including:

- Shared constants and test data
- Reusable setup/override utilities
- Domain-specific helper functions
- Mock infrastructure and factories

## Directory Overview

- `constants.py`: shared constants used by tests
- `data/`: fixture and sample data files
- `dependency_overrider.py`: dependency override helpers
- `util/`: helper modules organized by domain/resource
- `mocks/`: reusable mock utilities and mock factories

## Where to find detailed guidance

For subdirectory-specific usage and conventions, check nested README files.

Current nested docs:

- [mocks/README.md](mocks/README.md)

As more helper areas evolve, add/consult README files in those subdirectories for focused guidance.

## General principles

1. Keep helpers reusable and test-focused.
2. Prefer composition over one-off test-specific helper logic.
3. Keep domain-specific behavior in subdirectories (for example `util/` and `mocks/`).
4. Document non-obvious helper behavior close to where it is implemented.
