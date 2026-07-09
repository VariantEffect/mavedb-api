# MaveDB Public Data Dump — Changelog

This file records what changed between versions of the MaveDB bulk data archive
published on [Zenodo](https://doi.org/10.5281/zenodo.11201736). It ships inside
every archive so the version you downloaded always carries its own history.

Versions are the Zenodo archive versions (v1, v2, …), not MaveDB API or website
release numbers. The `asOf` timestamp in `main.json` records the exact moment a
given archive was generated.

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
  One line per current mapped variant carrying its highest materialized GA4GH
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
