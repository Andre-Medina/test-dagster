# Dagster asset check metadata experiments

Minimal, standalone Dagster project used to work out how asset check metadata is rendered in the
Dagster UI and persisted for downstream querying (the Vision UI). It is deliberately separate
from the Vision repo but pins the same stack: Dagster / dagster-webserver **1.13.30**, polars,
plotly, matplotlib.

Ten partitioned assets carry eighteen checks. Seven pairs each return one metadata shape (PNG
markdown, SVG markdown, scalars, table, JSON, Plotly JSON, URL artifact). The scalar asset also
has a `@multi_asset_check` that yields four individually named checks from one function. An
11,520-row grouped timeseries has four checks: the same rows as a table, row-oriented JSON, and
column-oriented JSON, plus a KPI-only check with eight floats for automatic plotting. Two
cross-asset examples compare date coverage between `orders_asset` and `prices_asset`, one using
`additional_ins` and one using `@multi_asset_check` with a spec per asset. Findings and
screenshots are in [README.md](README.md); read that before changing the experiments.

## Commands

| Task | Command |
| :--- | :--- |
| Install env | `pixi install` |
| Materialize all assets + checks for every partition | `pixi run materialize` |
| Start Dagster UI + daemon on port 3000 | `pixi run dev` |
| Query stored check metadata over GraphQL | `pixi run query` |
| Run one script | `pixi run python scripts/<name>.py` |
| Run a check pytest-free (breakpoints work) | launch `scripts/run_partitions.py` under the debugger |

`pixi run dev` uses the VSCode launch configuration "Dagster UI" as an equivalent alternative.
The "Materialize all partitions" launch configuration runs checks in-process, so breakpoints in
asset/check code are hit there — `dagster dev` runs code locations in a subprocess.

## Environment

- `DAGSTER_HOME` is `.dagster_home/` (SQLite run storage, filesystem event log, telemetry off).
  Every task sets it; ad-hoc commands must set it too or they will write to the default instance.
- Check artifacts (`url_artifact_check`) are written to
  `.dagster_home/storage/check_artifacts/<asset>_<partition>.png|.csv`.
- Decoded base64 payloads from `pixi run query` land in `artifacts/decoded/`.
- To reset local state: stop the server, then
  `rm -rf .dagster_home/history .dagster_home/storage artifacts` and re-run `pixi run materialize`.
- `pixi.lock` is committed; change versions in `pixi.toml` and let pixi re-lock.

## Layout

```text
src/test_dagster/
  data.py          deterministic half-hourly native/PV/net series per categorical partition,
                   plus the 4-segment x 2-network-asset grouped series (11,520 rows/partition)
  partitions.py    the shared categorical StaticPartitionsDefinition (`PARTITIONS`)
  cross_asset.py   two cross-asset date-consistency examples over orders/prices assets
  plots.py         matplotlib PNG/SVG bytes, base64 data URIs, Plotly figure dict
  definitions.py   the metadata-shape assets/checks, the job, and defs; imports cross-asset
scripts/
  run_partitions.py       materializes the job for every partition against DAGSTER_HOME
  query_check_metadata.py GraphQL client used by the Vision UI path; decodes images
screenshots/       UI captures referenced by README.md
```

## Conventions and gotchas

- Partitions are categorical and unordered, `north`, `south`, `east`, `west`, `central`
  (5 partitions), defined once as `PARTITIONS = StaticPartitionsDefinition(list(CATEGORIES))`.
  `data.py` maps each category to a synthetic month only to vary the generated shape; that
  mapping is not part of the partition key semantics.
- **Checks must be registered twice**: decorator-based checks need
  `Definitions(asset_checks=[...])`, otherwise their keys cannot be resolved by a job selection.
- **Checks on a partitioned asset need `partitions_def=PARTITIONS`.** Without it the check is
  unpartitioned and Dagster applies `AllPartitionMapping` — the check receives a
  `dict[partition_key, DataFrame]` instead of the current partition's value.
- The implicit global asset job does **not** run checks; `metadata_job` selects them explicitly
  with `AssetSelection.all() | AssetSelection.checks(*CHECK_KEYS)`.
- Metadata entry names in GraphQL: `MarkdownMetadataEntry` exposes `mdStr`, ints/floats expose
  `intRepr` / `floatRepr`, tables expose `table.records` (JSON strings) and `table.schema`.
- Large markdown values (>1,000 chars) render behind `[Show Markdown]` in the UI; embedded
  base64 images always exceed that, so a click is required. SVG is ~25% smaller than PNG here.
- Raw `plotly.graph_objects.Figure` metadata raises `DagsterInvalidMetadata`; serialise to JSON.
  Dagster 1.13.30 has no native plotly/vega renderer — numeric metadata *is* auto-charted in the
  check's "Plots" section.
- `file://` URLs render as links but browsers block opening them from the `http://` UI; use
  `https://` (e.g. signed GCS URLs) for real artifacts.
- `MetadataValue.table` wraps every record as `{"__class__": "TableRecord", "data": {...}}`, so
  it is ~20% larger than row-oriented JSON in the event log and ~2x columnar JSON. The UI also
  renders every row (the 11,520-row check page is 659k px tall). Use tables for tens of rows.
- Check Overview plots are hard-coded to `xAxis="time"` (`AssetCheckOverview.tsx`); asset
  Overview plots use `xAxis="partition"` for partitioned assets. To get partition-axis auto
  plots, add numeric metadata to the **asset** with `context.add_output_metadata(...)`, not to
  the check.
- Check metadata is visible only in the check panel's Overview, Partitions (after selecting a
  partition), and Execution history (`{}` per row). The asset-level Overview and Partitions tabs
  show check status / materialization metadata, not check metadata.
- `@dg.multi_asset_check(specs=[...])` yields several `AssetCheckResult`s from one function;
  each result's `check_name` must match a spec. It creates one check evaluation per spec (and
  partition), not one evaluation holding several results. `scalar_individual_checks` is the
  example. Its simulated outcomes are seeded by `partition_key:check_name`, so re-runs are
  deterministic.
- `AssetCheckSeverity.WARN` vs `ERROR` does not change the pass/fail text, but the UI does
  render it: WARN failures get a yellow warning icon/tag, ERROR failures a red icon/tag.
  Severity only affects blocking behavior; these checks are non-blocking.
- Cross-asset checks: `@dg.asset_check(..., additional_ins={"other": dg.AssetIn("other")})`
  gives the owning check another asset's value; the result is recorded only on its own asset.
  To record checks on both assets, use `@multi_asset_check` with a spec per asset (see
  `cross_asset.py`).
- Cross-asset partition mapping defaults: two time-window definitions ->
  `TimeWindowPartitionMapping`; both partitioned otherwise -> `IdentityPartitionMapping`; one
  side unpartitioned -> `AllPartitionMapping` (which may hand you a partition-key mapping).
  Set `partition_mapping` on the `AssetIn` when the keys do not line up.
- Long metadata labels (the 45-char `net_energy_kwh__<segment>__<asset>` keys) overlap their
  values in the asset Partitions metadata table; keep labels short if they appear there.
- Screenshots use `NN__section__subject__detail.png` under `screenshots/`; keep the `__`
  separators so the folder groups and sorts by section.
- Keep the one-line docstring style from the Vision repo; do not add parameter/return sections.
