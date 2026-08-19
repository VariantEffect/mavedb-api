# MaveDB Public Data Dump — Changelog

This file records what changed between versions of the MaveDB bulk data archive
published on [Zenodo](https://doi.org/10.5281/zenodo.11201736). It ships inside
every archive so the version you downloaded always carries its own history.

Versions are the Zenodo archive versions (v1, v2, …), not MaveDB API or website
release numbers. The `asOf` timestamp in `main.json` records the exact moment a
given archive was generated.

---

## [Unreleased]

### Added

- **VRS objects** — `vrs/{urn}.vrs.ndjson` for every score set that has completed
  the mapping pipeline. One record per variant placed on a reference, carrying the `pre_mapped` /
  `post_mapped` GA4GH VRS pair and the Cat-VRS `categorical_variant`. Nested objects
  cannot live in a CSV cell, so they get their own artifact; join to
  `csv/{urn}.annotations.csv` on `mavedb.post_mapped_vrs_id`. No part of this file is
  withheld on score-calibration visibility grounds, because none of it is derived
  from a calibration.

### Deprecated

- **`mapped/{urn}.mapped-variants.json`** — superseded by `vrs/{urn}.vrs.ndjson`. It
  is sourced from the pre-allele-graph mapping store, which no longer receives
  writes, so it now appears only for score sets mapped before that migration. It will
  be removed in a future version.

### Changed

- gnomAD frequencies are now reported from whichever release each allele's current
  record came from, rather than only from the single release the deployment serves.
  An allele not covered by a newer release previously reported `NA` for every gnomAD
  column; it now reports the frequency MaveDB actually holds, with
  `gnomad.gnomad_version` naming its release. Expect a mixture of releases within one
  file and read that column per row.

### Fixed

- The three mapping-derived artifacts are no longer omitted for score sets mapped
  after the allele-graph migration.
- `main.json` no longer names an unpublished superseding score set's URN and title.

---

## [5] — 2026-06-25

Version 5 is the first version tracked in this changelog. Versions 1–4 contained
metadata (`main.json`) plus per-score-set score and count CSVs; they predate this
file and are not itemized here. Version 5 adds variant-mapping and annotation
outputs derived from the MaveDB variant mapping pipeline.

### Added

- **Annotation CSVs** — `csv/{urn}.annotations.csv` for every score set that has
  completed the mapping pipeline. Joins external-database annotations (Ensembl VEP
  functional consequence, gnomAD v4.1 allele frequency, ClinGen Allele Registry ID)
  with post-mapped HGVS (`g.`/`c.`/`p.` and assay-level) and the GA4GH VRS digest.
- **Mapped-variant JSON** — `mapped/{urn}.mapped-variants.json` for every mapped
  score set. Each record carries the pre-mapped and post-mapped VRS alleles, the
  VRS schema version, the mapping API version, and the ClinGen allele ID, mirroring
  `GET /api/v1/score-sets/{urn}/mapped-variants`.
- **VA-Spec annotation NDJSON** — `va/{urn}.va.ndjson` for every mapped score set.
  One line per variant currently placed on a reference, carrying its highest materialized GA4GH
  VA-Spec layer (study result → functional-impact statement → pathogenicity
  statement), mirroring the `annotated-variants` streaming endpoints.
- **ClinVar release columns** in the annotation CSVs, covering the `2015_02`
  through `2026_01` annual snapshots.

### Changed

- **CSV columns are now namespaced.** Every column carries a source prefix
  separated by a dot: score columns are `scores.*` (e.g. `scores.score`), counts
  are `counts.*`, mapping-pipeline columns are `mavedb.*`, and external annotations
  are `vep.*` / `gnomad.*` / `clingen.*`. Core identifier columns (`accession`,
  `hgvs_nt`, `hgvs_pro`, `hgvs_splice`) remain unprefixed. **Consumers that parsed
  earlier archives by bare column name (e.g. `score`) must update to the namespaced
  names.**
- **Cis-phased (haplotype) variants** are now combined into a single HGVS
  expression in the CSV outputs where a combined expression can be derived.
- Mapped-variant and VA-Spec outputs include only the **current** mapping for each
  variant; superseded mappings are excluded.

### Fixed

- VA-Spec annotation output is now valid and round-trippable (it deserializes back
  into GA4GH VA-Spec objects without loss).

### Notes

- Annotation, mapped-variant, and VA-Spec files are present **only** for score sets
  that have completed the mapping pipeline. Unmapped score sets ship scores/counts
  CSVs only, as before.
- See `README.md` in this archive for the full file layout, column definitions, and
  caveats.
