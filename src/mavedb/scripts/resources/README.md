# MaveDB Public Data Dump

This archive contains a snapshot of publicly accessible variant effect data from MaveDB.
The `asOf` field in `main.json` records the exact date and time this dump was generated.

### Useful links
- **MaveDB website:** https://www.mavedb.org
- **API documentation:** https://api.mavedb.org/docs
- **MaveDB documentation:** https://mavedb.org/docs/mavedb/index.html
- **Source code:**
    - https://github.com/VariantEffect/mavedb-api
    - https://github.com/VariantEffect/mavedb-ui
    - https://github.com/VariantEffect/dcd_mapping2

---

## What's Included

This dump includes only data that is:

- **Published** — publicly released on MaveDB
- **CC0-licensed** — released under the Creative Commons CC0 1.0 Public Domain Dedication

Unpublished data, private datasets, and datasets published under other licenses are excluded.

---

## Archive Structure

```
mavedb-dump.YYYYMMDDHHMMSS.zip
├── README.md                                  # This file
├── LICENSE.txt                                # Creative Commons CC0 1.0 license text
├── main.json                                  # Metadata for all included datasets
├── csv/
│   ├── {urn}.scores.csv                       # Variant effect scores (all score sets)
│   ├── {urn}.counts.csv                       # Variant counts (score sets with count data only)
│   └── {urn}.annotations.csv                  # Variant annotations from VEP, gnomAD, ClinGen and ClinVar, plus score calibration interpretations
│                                              #   (score sets that have completed mapping only)
├── mapped/
│   └── {urn}.mapped-variants.json             # Mapped variant data including VRS alleles and HGVS
│                                              #   (score sets that have completed mapping only)
└── va/
    └── {urn}.va.ndjson                        # GA4GH VA-Spec annotations, one record per mapped variant
                                               #   (score sets that have completed mapping only)
```

`{urn}` is the score set URN with colons replaced by hyphens, e.g., `urn-mavedb-00000001-a-1`.

---

## File Descriptions

### `main.json`

A JSON object containing MaveDB metadata with three top-level fields:

- `title` — `"MaveDB public data"`
- `asOf` — ISO 8601 UTC timestamp indicating when this dump was generated
- `experimentSets` — Array of experiment set objects, each containing nested experiments and score
  sets with full metadata (targets, publications, licenses, contributors, etc.)

The hierarchy mirrors the MaveDB data model: each **ExperimentSet** contains one or more
**Experiments**, each of which contains one or more **ScoreSets**.

Score set metadata includes the `datasetColumns` field, which lists the names of the per-score-set
score and count columns that appear in the corresponding CSV files.

### CSV column namespacing

All CSV files exported from MaveDB use a namespaced column naming scheme. The namespace prefix
identifies which data source a column belongs to and is separated from the column name by a dot:

| Prefix | Source |
|--------|--------|
| *(no prefix)* | Core identifiers — `accession`, `hgvs_nt`, `hgvs_pro`, `hgvs_splice` |
| `scores.` | Score columns defined by the score set author (e.g. `scores.score`) |
| `counts.` | Count columns defined by the score set author |
| `mavedb.` | Columns computed by the MaveDB mapping pipeline (post-mapped HGVS, VRS identifier) |
| `vep.` | Ensembl Variant Effect Predictor annotations |
| `gnomad.` | gnomAD population frequency data |
| `clingen.` | ClinGen Allele Registry linkage |

Missing or inapplicable values in all CSV files are represented as the string `NA`.

### `csv/{urn}.scores.csv`

Comma-separated file with variant effect scores. Contains the following fixed columns, followed by
score columns defined by each individual score set:

| Column | Description |
|--------|-------------|
| `accession` | Full variant URN (e.g., `urn:mavedb:00000001-a-1#1`) |
| `hgvs_nt` | Assay-level nucleotide HGVS string in MAVE-HGVS format, if applicable |
| `hgvs_pro` | Assay-level protein HGVS string in MAVE-HGVS format, if applicable |
| `hgvs_splice` | Assay-level splice HGVS string in MAVE-HGVS format, if applicable |
| `scores.score` | The primary score column — always present |
| `scores.*` | Additional score columns defined by the score set author |

The `hgvs_nt`, `hgvs_pro`, and `hgvs_splice` columns use **MAVE-HGVS format** — a constrained
subset of HGVS notation used by MaveDB. These strings are often expressed relative to the
assay's reference sequence (a transcript or protein), not the genome, and may not validate against
a standard HGVS parser. Score values are not normalized across score sets; each score set defines
its own scale and units. Refer to the score set's entry in `main.json` for the meaning of each
score column.

### `csv/{urn}.counts.csv`

Same structure as `scores.csv`, but with `counts.*` columns in place of score columns. Only
present for score sets that have count data. The count column names are listed in
`datasetColumns.countColumns` in `main.json`.

### `csv/{urn}.annotations.csv`

Variant annotation data from external databases, joined with post-mapped HGVS and VRS identifiers
produced by the MaveDB variant mapping pipeline. **Only present for score sets that have completed
the MaveDB mapping pipeline.**

Columns are grouped by a namespace prefix. The groups below always appear:

| Column | Description |
|--------|-------------|
| `accession` | Full variant URN — use this to join with `scores.csv` |
| `hgvs_nt` | Assay-level nucleotide HGVS (MAVE-HGVS format) |
| `hgvs_pro` | Assay-level protein HGVS (MAVE-HGVS format) |
| `hgvs_splice` | Assay-level splice HGVS (MAVE-HGVS format) |
| `mavedb.post_mapped_hgvs_g` | Post-mapped genomic HGVS on GRCh38 (g. notation) |
| `mavedb.post_mapped_hgvs_c` | Post-mapped coding HGVS (c. notation) |
| `mavedb.post_mapped_hgvs_p` | Post-mapped protein HGVS (p. notation) |
| `mavedb.post_mapped_hgvs_at_assay_level` | Post-mapped HGVS at the assay reference level (transcript or protein) |
| `mavedb.post_mapped_vrs_id` | GA4GH VRS identifier for the post-mapped allele (e.g. `ga4gh:VA.n9ax-9x6gOC0OEt73VMYqCBfqfxG1XUH`) |
| `vep.vep_functional_consequence` | VEP functional consequence term (e.g. `missense_variant`) |
| `gnomad.gnomad_af` | gnomAD allele frequency (allele count ÷ allele number) |
| `gnomad.gnomad_ac` | gnomAD allele count — chromosomes observed carrying the allele |
| `gnomad.gnomad_an` | gnomAD allele number — total chromosomes sampled |
| `gnomad.gnomad_faf95_max` | Maximum filtering allele frequency at 95% confidence across genetic ancestry groups |
| `gnomad.gnomad_faf95_max_ancestry` | Genetic ancestry group attaining `gnomad_faf95_max` |
| `gnomad.gnomad_id` | gnomAD variant identifier (`chrom-pos-ref-alt`), e.g. `17-43092919-G-A` |
| `gnomad.gnomad_version` | gnomAD release the frequencies were drawn from (e.g. `v4.1`) |
| `clingen.clingen_allele_id` | ClinGen Allele Registry CA identifier (e.g. `CA12345`) |

Two further groups vary by score set, because they exist only where MaveDB holds the underlying data.
Read the header rather than assuming a fixed column set.

**ClinVar** — one pair of columns per ingested release, prefixed `clinvar.YEAR_MONTH`. A score set with
records from the January 2024 release carries `clinvar.2024_01.clinical_significance` and
`clinvar.2024_01.clinical_review_status`.

This file carries **every** release MaveDB holds for the score set, not just the most recent one, so a
change in ClinVar's assessment over time can be read off a single file.

**Score calibrations** — one group per calibration, prefixed `calibration.<calibration urn>`, giving
that calibration's interpretation of each variant:

| Column suffix | Description |
|---------------|-------------|
| `title` | Human-readable name of the calibration |
| `research_use_only` | `True` if the calibration is not intended for clinical use — see below |
| `functional_classification` | `normal`, `abnormal`, or `indeterminate` |
| `acmg_criterion` | ACMG 2015 criterion evaluated, e.g. `PS3` or `BS3` |
| `acmg_evidence_strength` | Strength the criterion was met at, e.g. `MODERATE`. `NA` when not met |
| `acmg_evidence_outcome_code` | ACMG evidence outcome code, e.g. `PS3_moderate`, `PS3` (strong), `BS3_not_met` |
| `pathogenicity_classification` | `PATHOGENIC`, `BENIGN`, or `UNCERTAIN_SIGNIFICANCE` |

Every calibration MaveDB holds for the score set gets a group, including one that defines no score
ranges. Such a group carries `title` and
`research_use_only` with `NA` in every interpretation column: the calibration exists and was consulted,
and it has no classification to give. That is different from a calibration whose ranges simply do not
contain a particular variant, which reports `UNCERTAIN_SIGNIFICANCE` and `PS3_not_met`.

**Research-use-only calibrations are included in this file**, and are marked by
`research_use_only` = `True`. These calibrations have not been assessed as suitable for clinical variant
interpretation. Filter them out on that column if you are assembling clinical evidence. They are currently *not*
present in `va/{urn}.va.ndjson`.

`acmg_evidence_strength` uses MaveDB's own scale, which includes `MODERATE_PLUS` — an intermediate
strength that the GA4GH VA-Spec has no equivalent for. The same variant's record in `va/{urn}.va.ndjson`
therefore reports `moderate` where this file reports `MODERATE_PLUS`.

Variants that could not be mapped, or for which a specific annotation is unavailable, will have
`NA` in the corresponding column. For multi-allelic variants (haplotypes), `mavedb.*` HGVS columns
will be `NA` because a single combined HGVS string cannot currently be derived. This may be updated in
a future release.

### `mapped/{urn}.mapped-variants.json`

A JSON array of the score set's **current** mapped variant records. The same shape as
`GET /api/v1/score-sets/{urn}/mapped-variants`, narrowed to `current: true`. Each record
corresponds to a single variant:

| Field | Description |
|-------|-------------|
| `variantUrn` | URN of the source variant — use this to join with `accession` in the CSV files |
| `preMapped` | VRS allele or haplotype using coordinates on the assay's reference sequence (transcript or protein accession) |
| `postMapped` | VRS allele or haplotype lifted over to GRCh38 genomic coordinates |
| `vrsVersion` | VRS schema version used to encode these objects (e.g., `"1.3"`, `"2.0"`) |
| `mappingApiVersion` | Version of the dcd_mapping service that produced this result |
| `mappedDate` | Date the mapping was produced |
| `modificationDate` | Date this mapping record was last modified |
| `current` | Always `true` in this dump — superseded mappings are not included (see Caveats) |
| `errorMessage` | Diagnostic message if mapping failed; `null` on success |
| `clingenAlleleId` | ClinGen Allele Registry identifier, if the variant has been registered |

`preMapped` and `postMapped` are raw GA4GH VRS objects (JSON). The `type` field within them may be
`"Allele"`, `"Haplotype"`, or `"CisPhasedBlock"` depending on the variant. Records where mapping
failed will have `preMapped: null`, `postMapped: null`, and a non-null `errorMessage`. **Only
present for score sets that have completed the MaveDB mapping pipeline.**

---

### `va/{urn}.va.ndjson`

[Newline-delimited JSON](https://ndjson.org/): one line per current mapped variant. Each line is an
envelope mirroring the `GET /api/v1/score-sets/{urn}/annotated-variants/*` streaming endpoints:

```json
{"variant_urn": "urn:mavedb:00000001-a-1#1", "annotation": { ... }}
```

| Field | Description |
|-------|-------------|
| `variant_urn` | URN of the source variant — use this to join with `accession` in the CSV files |
| `annotation` | A single GA4GH VA-Spec object, or `null` |

Rather than re-emitting every nested layer, each variant carries only its **highest materialized**
VA-Spec layer. The lower layers are not dropped — they are nested inside the higher one (the study
result sits inside the functional statement, which sits inside the pathogenicity statement). Both
statement layers serialize with `type: "Statement"`, so `annotation.type` alone does not distinguish
them — use `annotation.proposition.type`:

| Layer | `annotation.type` | `annotation.proposition.type` | GA4GH class | Emitted when |
|-------|-------------------|-------------------------------|-------------|--------------|
| Pathogenicity statement | `Statement` | `VariantPathogenicityProposition` | `VariantPathogenicityStatement` | A non-research-use calibration with ACMG classifications exists |
| Functional impact statement | `Statement` | `ExperimentalVariantFunctionalImpactProposition` | `Statement` | A non-research-use calibration with functional ranges exists |
| Study result | `ExperimentalVariantFunctionalImpactStudyResult` | — | `ExperimentalVariantFunctionalImpactStudyResult` | Any variant that can be mapped (lowest layer) |

**Note on the pathogenicity layer:** its `classification` (e.g. `Pathogenic` / `Uncertain
Significance` / `Benign`) integrates **only MaveDB functional evidence** — every eligible calibration
for the variant, with the strongest determining the statement-level classification — and not the
non-functional ACMG criteria (population frequency, segregation, computational predictions) that a
full clinical determination requires. Treat it as the functional contribution to a classification, to
be combined with other evidence downstream, not as a standalone clinical verdict.

Research-use-only calibrations are excluded from this file, unlike `csv/{urn}.annotations.csv`, which
includes them under a `research_use_only` flag.

`annotation` is `null` for current mapped variants that have no post-mapped allele (and therefore
cannot be annotated); the `variant_urn` is still present on those lines. Every current mapped variant
produces exactly one line, so the line count equals the current mapped-variant count. **Only present
for score sets that have completed the MaveDB mapping pipeline.**

---

## Working with this data

### Joining files for a single score set

All files for a given score set share the same variant identifier:

- In CSV files: the `accession` column (e.g. `urn:mavedb:00000001-a-1#42`)
- In `mapped-variants.json`: the `variantUrn` field

To combine scores with annotations or with VRS data, join on `accession` = `variantUrn`.

### Linking files back to metadata

A filename like `urn-mavedb-00000001-a-1.scores.csv` corresponds to the score set with
`"urn": "urn:mavedb:00000001-a-1"` in `main.json`. The filename prefix is the score set URN with
every colon (`:`) replaced by a hyphen (`-`).

### Reconstructing score set metadata from `main.json`

`main.json` contains the full metadata hierarchy. Score sets are nested inside experiments, which
are nested inside experiment sets. To find the metadata for a specific score set:

```python
import json

with open("main.json") as f:
    data = json.load(f)

target_urn = "urn:mavedb:00000001-a-1"
score_set = next(
    ss
    for es in data["experimentSets"]
    for exp in es["experiments"]
    for ss in exp["scoreSets"]
    if ss["urn"] == target_urn
)
```

---

## Caveats

- Only **published**, **CC0-licensed** data is included. Datasets with other licenses are not
  present in this dump even if they are publicly visible on MaveDB.
- Annotation files (`.annotations.csv`), mapped variant files (`.mapped-variants.json`), and
  VA-Spec files (`.va.ndjson`) are **only present for score sets that have been processed by the
  MaveDB variant mapping pipeline**. Score sets that have not yet been mapped, or for which mapping
  failed entirely, will not have these files.
- `annotations.csv` includes **research-use-only** calibrations, flagged by `research_use_only`; the
  `va/` files exclude them. Filter on that column before using calibration output as clinical evidence,
  or before comparing the two files.
- The `va/` files carry only each variant's highest materialized VA-Spec layer (see
  [`va/{urn}.va.ndjson`](#vaurnvandjson)). The pathogenicity layer's classification reflects MaveDB
  functional evidence only, not a full clinical ACMG determination.
- Mapping is applied per variant within a score set. A score set that has completed the mapping
  pipeline may still contain individual variants with failed mappings. Those variants have `NA` in
  all `mavedb.*`, `vep.*`, `gnomad.*`, and `clingen.*` columns in the annotations CSV, and
  `preMapped: null` / `postMapped: null` in the JSON.
- This dump is a snapshot of MaveDB's **current** state, not a historical archive. The `mapped/`
  JSON files include only each variant's current mapping — superseded records from earlier mapping
  runs are not retained here. (Unlike this per-run dump, `GET /api/v1/score-sets/{urn}/mapped-variants`
  does return superseded mappings, for callers that need that history directly.) The same applies to
  `annotations.csv` and the `va/` files: annotations are always reported with respect to the current
  mapping object. If you need MaveDB's state as of a specific point in time, use a dump from that
  time (e.g. an earlier Zenodo-archived release) rather than looking for historical rows within one.
- gnomAD allele frequencies in `annotations.csv` are sourced from **gnomAD v4.1** specifically.
- `preMapped` VRS objects reference the assay's input sequence (a transcript or protein accession).
  `postMapped` VRS objects are remapped to the **GRCh38** reference genome. Do not compare
  coordinates between `preMapped` and `postMapped` directly.
- Assay-level HGVS strings (`hgvs_nt`, `hgvs_pro`, `hgvs_splice`) are in **MAVE-HGVS format**, a
  constrained community convention that may not parse with a standard HGVS library.
- Score values are **not normalized** across score sets. Each score set defines its own scale,
  range, and interpretation. A score of `1.0` in one score set has no defined relationship to a
  score of `1.0` in another.
- The data in this dump reflects the state of MaveDB at the time of export, as recorded in the
  `asOf` UTC timestamp in `main.json`. It may not reflect changes made after that time.

---

## License

All data in this archive is released under the
[Creative Commons CC0 1.0 Universal (CC0 1.0) Public Domain Dedication](https://creativecommons.org/publicdomain/zero/1.0/).

See `LICENSE.txt` for the full license text.
