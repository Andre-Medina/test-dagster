# Dagster asset check metadata experiments

Minimal partitioned Dagster pipeline (Dagster 1.13.30, the version pinned in the Vision
repo) that answers two questions:

1. Which metadata shapes render natively in the Dagster UI when returned from an asset check?
2. Which metadata shapes survive in the Dagster database/GraphQL API in a form the Vision UI
   can query?

There are five categorical, unordered partitions (`north`, `south`, `east`, `west`, `central`),
defined once as `PARTITIONS = StaticPartitionsDefinition(...)`. Each partition materializes
eight assets and fifteen checks — one pair per small metadata approach, four independently named
checks yielded from a single `@multi_asset_check`, and four checks on the large-time-series
asset.

## Run it

```bash
cd /Users/andre.medina/git/temp/test-dagster
pixi install
pixi run materialize   # runs every asset + check for every partition
pixi run dev           # http://127.0.0.1:3000
pixi run query         # queries GraphQL, decodes embedded images to artifacts/decoded/
```

`DAGSTER_HOME` is `.dagster_home/` (SQLite run storage, filesystem event log). The check
artifacts for the URL experiment are written under `.dagster_home/storage/check_artifacts/`.

In VSCode, `.vscode/launch.json` provides equivalent configurations: **Dagster UI** (starts
`dagster dev` on port 3000) and **Materialize all partitions (debug checks)** (runs checks
in-process so breakpoints are hit). [AGENTS.md](AGENTS.md) is the short orientation for
working in this project.

UI path: **Catalog → an asset → Checks tab → a check**. The check panel's **Overview** shows
the latest metadata plus automatic plots; **Partitions** → select a partition shows that
partition's metadata; **Execution history** → `{}` opens any evaluation's metadata.

## Experiments

| Asset / check | Metadata returned | What it tests |
| :--- | :--- | :--- |
| `png_markdown_asset` / `png_markdown_check` | `MetadataValue.md` with a `data:image/png;base64,...` image + `int` byte count | Raster plot inline in metadata |
| `svg_markdown_asset` / `svg_markdown_check` | `MetadataValue.md` with a `data:image/svg+xml;base64,...` image + `int` byte count | Vector plot, smaller payload |
| `scalar_metadata_asset` / `scalar_check` | `int`, `float`, `bool`, `text`, `timestamp` | Scalar KPIs |
| `scalar_metadata_asset` / `scalar_peak_limit`, `scalar_pv_energy`, `scalar_above_1000h`, `scalar_row_count` | Four `AssetCheckResult`s yielded by one `@multi_asset_check`, two metadata entries each, with a deterministic simulated pass / WARN-failure / ERROR-failure | Several individually named checks from one function |
| `table_metadata_asset` / `table_check` | `MetadataValue.table` with explicit `TableSchema` | Structured key metrics |
| `json_metadata_asset` / `json_check` | `MetadataValue.json` (summary + 1440-point series) | Plot-ready structured payload |
| `plotly_metadata_asset` / `plotly_check` | Plotly figure serialised to `MetadataValue.json` | Native interactive chart? |
| `url_artifact_asset` / `url_artifact_check` | `MetadataValue.url` + `MetadataValue.path` + a markdown link, PNG/CSV written to disk | Keep bytes out of metadata |
| `timeseries_metadata_asset` / `timeseries_table_check` | 11,520-row long-format table (4 customer segments × 2 network assets × 1,440 half-hours) | Large timeseries as table metadata |
| `timeseries_metadata_asset` / `timeseries_kpi_check` | Eight `net_energy_kwh__<segment>__<asset>` floats | Automatic plots from a few bytes of metadata |
| `timeseries_metadata_asset` / `timeseries_json_check` | The same 11,520 rows as one row-oriented JSON payload | JSON size comparison |
| `timeseries_metadata_asset` / `timeseries_columnar_json_check` | The same rows as column-oriented JSON (`{"column": [values...]}`) | Compact encoding for large series |

The `scalar_check` deliberately fails when peak net demand exceeds 1200 kW (`east`, whose
synthetic shape uses March), so the partition grid shows both green and red states.
`timeseries_metadata_asset` also attaches
the eight group KPIs to its **asset materialization** (`context.add_output_metadata`) so the
asset-level Overview plots them against partition keys.

## Findings

### Rendered natively in the Dagster UI

| Approach | UI behaviour |
| :--- | :--- |
| PNG in markdown | Shows `plot → [Show Markdown]` in the check panel. One click opens a dialog that renders the PNG at full size. Markdown values longer than 1,000 chars are always collapsed behind this button; the image itself renders correctly. |
| SVG in markdown | Same behaviour as PNG. For this chart the SVG payload was ~25% smaller (93–95 KB raw vs 123–125 KB PNG; 124–126 KB vs 164–166 KB when base64-encoded). |
| int / float / bool / text / timestamp | Key-value table in the check panel. **Numeric entries also get automatic line charts** in the "Plots" section (one small line chart per numeric metadata key, x-axis = execution timestamp). |
| `MetadataValue.table` | Renders as a real table inline, with a "Show schema" toggle. Best native structured display. |
| `MetadataValue.json` | Collapsed `[Show JSON]` → dialog with raw, pretty-printed JSON. Not charted. |
| `MetadataValue.url` / `path` | Clickable link / copyable path. No preview or chart. A `file://` URL is displayed but the browser blocks navigating to it from the `http://` Dagster page (verified: clicking it opens nothing). Use an `https://` URL in production (e.g. a signed GCS URL). |
| Plotly JSON | Renders as raw JSON, exactly like any other `JsonMetadataEntry`. **Dagster 1.13.30 has no native Plotly/Vega chart renderer.** Passing a raw `plotly.graph_objects.Figure` raises `DagsterInvalidMetadata`; it must be converted to JSON (or an image) first. |

### Large timeseries: table vs JSON (11,520 rows per partition)

`timeseries_metadata_asset` produces 4 customer segments × 2 network assets × 1,440 half-hours
= **11,520 rows per partition**. The three checks return the identical rows in three encodings.
Measured on the `central` partition (the others are within ±0.2%):

| Encoding | Event-log JSON | GraphQL payload | Relative to columnar |
| :--- | ---: | ---: | ---: |
| `MetadataValue.table` (11,520 `TableRecord`s) | 2,588 KiB | 2,438 KiB (`table.records`) | 2.0× |
| Row-oriented JSON (`{"rows": [{...}, ...]}`) | 2,158 KiB | 2,208 KiB (`jsonString`) | 1.75× |
| Column-oriented JSON (`{"columns": {"col": [...]}}`) | 1,214 KiB | 1,240 KiB (`jsonString`) | 1.0× |
| Parquet artifact (snappy, not stored in metadata) | 221 KiB on disk | — | 0.18× |

All three checks together add ~6 MiB to a single run's event log (`history/runs/<run_id>.db`)
for one partition, i.e. ~30 MiB across the five materialized partitions.

Conclusions:

- **Table metadata is not efficient for thousands of rows.** Each record is persisted with a
  `{"__class__": "TableRecord", "data": {...}}` wrapper, so the table is ~20% larger than the
  equivalent row-oriented JSON in the event log and ~10% larger over GraphQL.
- **Column-oriented JSON is the cheapest in-metadata encoding**, roughly half the table and
  ~44% smaller than row-oriented JSON, because dimension columns compress to repeated strings
  and numeric columns avoid repeating keys on every row.
- **The UI cost is worse than the storage cost.** Dagster renders every table row into the DOM:
  the 11,520-row check page has a 659,165 px scroll height and ~1.09 MB of body text. Scrolling
  from the metadata table to the "Plots" section is a real usability problem.
- **For a full timeseries, metadata is the wrong store.** A snappy Parquet file of the same
  frame is 221 KiB — 12× smaller than the table and 5.5× smaller than columnar JSON. Write it
  to object storage and return `MetadataValue.url` / `MetadataValue.path`; put only a small
  typed table of KPIs (tens of rows) and a handful of numeric group KPIs in check metadata.
- **If the series must live in metadata, use columnar JSON** (or a compact encoding such as
  dictionary-encoded dimensions) rather than `MetadataValue.table` or row-oriented JSON.

### Automatic plots: checks use time, assets use partitions

Numeric metadata produces Dagster's automatic "Plots" section without storing any image. The
two contexts are not equivalent in 1.13.30:

- **Check Overview** (`AssetCheckOverview.tsx`) hard-codes `xAxis="time"` and uses each
  evaluation's timestamp. Every numeric label becomes one chart across all evaluations; there
  is no per-partition x-axis and no way to scope the charts to one partition.
- **Asset Overview** (`AssetMaterializationGraphs.tsx`) uses `xAxis="partition"` for partitioned
  assets. Numeric metadata added with `context.add_output_metadata(...)` becomes one chart per
  label, plotted against the partition key.

`timeseries_metadata_asset` demonstrates both:

- `timeseries_kpi_check` returns eight `net_energy_kwh__<segment>__<asset>` floats and nothing
  else. The check Overview shows eight time-axis plots over the five evaluations (all points
  share the run timestamp); the whole metadata payload is 930 bytes per partition.
- Its materialization returns the same eight floats. The asset Overview's **Metadata → Plots**
  view shows eight partition-axis charts, again a few hundred bytes per partition instead of
  the 2.6 MB table.

For per-partition *within-series* shapes (e.g. the 1,440-point daily curve), Dagster has no
auto-chart option: return a PNG/SVG (Dagster-rendered) or JSON/series artifact (Vision-rendered).

### Several individual checks from one function

`scalar_individual_checks` is a single `@dg.multi_asset_check` with four `AssetCheckSpec`s. It
yields four consecutive `AssetCheckResult`s, one per spec, each with its own `check_name`, a
two-entry metadata payload, and an independently simulated outcome:

| Outcome | Severity | Meaning |
| :--- | :--- | :--- |
| pass | `ERROR` (irrelevant for a pass) | simulated pass |
| fail | `WARN` | warning-level failure |
| fail | `ERROR` | error-level failure |

The outcome is pseudo-random but deterministic per `partition_key × check_name` (seeded
`random.Random`), so re-running a partition does not flap. Across the five categories the mix
contains passes, WARN failures, and ERROR failures. The checks are non-blocking, but the UI
still distinguishes severity visually: WARN failures use a yellow warning icon/tag, ERROR
failures use a red icon/tag, and both read "Failed". The check list badge aggregates across
partitions (so a check can look failed while the latest partition passed); `Partitions` and
`Execution history` show one evaluation per check per partition, which is the point: one
function can emit several individually named checks, each with a small metadata payload.

### Where check metadata is visible

| Location | Check metadata shown? |
| :--- | :--- |
| Asset → Checks → check panel **Overview** | Yes: latest execution metadata, plus aggregated numeric "Plots" |
| Asset → Checks → check panel **Partitions** → select a partition | Yes: that partition's evaluation metadata |
| Asset → Checks → check panel **Execution history** → `{}` per row | Yes: that evaluation's metadata dialog |
| Asset → **Overview** | No: only latest check pass/fail count |
| Asset → **Partitions** → select a partition | No: asset materialization metadata only (e.g. IO manager `path`) |

Long metadata labels (the 45-character group KPI names) overlap their values in the asset
Partitions metadata table; the same labels render correctly in the check panel. Keep metadata
labels short if they will be viewed in the asset partition pane.

### Queryable over GraphQL (Vision UI path)

All of the above are persisted as part of the `ASSET_CHECK_EVALUATION` event and exposed per
evaluation. Query used by `scripts/query_check_metadata.py`:

```graphql
query CheckMetadata($assetKey: AssetKeyInput!, $checkName: String!, $partition: String) {
  assetCheckExecutions(
    assetKey: $assetKey
    checkName: $checkName
    partition: $partition   # omitted = all partitions; "" = unpartitioned only
    limit: 20
  ) {
    partition
    status
    evaluation {
      partition
      success
      severity
      description
      metadataEntries {
        __typename
        label
        ... on MarkdownMetadataEntry { mdStr }
        ... on IntMetadataEntry { intRepr }
        ... on FloatMetadataEntry { floatRepr }
        ... on JsonMetadataEntry { jsonString }
        ... on TableMetadataEntry { table { records schema { columns { name type } } } }
        ... on UrlMetadataEntry { url }
        ... on PathMetadataEntry { path }
      }
    }
  }
}
```

Observed payloads and sizes (one partition):

- `MarkdownMetadataEntry.mdStr`: 164–166 KB for a 124 KB PNG; 124–126 KB for a 94 KB SVG.
- `JsonMetadataEntry.jsonString`: 46 KB for summary + 1,440 half-hourly points; 68 KB for the
  Plotly figure (Plotly `layout` is verbose).
- `TableMetadataEntry`: `table.schema.columns` gives typed columns and `table.records` is a
  list of JSON-encoded record strings, e.g. `{"metric": "peak_net_kw", "unit": "kW", "value": 1106.55}`.
- Scalar entries expose `intRepr` / `floatRepr` (string forms avoid 32-bit and NaN/Inf issues),
  plus `boolValue`, `text`, `timestamp`.
- Both `execution.partition` and `evaluation.partition` are set, so per-partition results are
  directly addressable with the `partition` argument.

There is no dedicated `PlotlyMetadataEntry` in the GraphQL schema — the metadata entry union is
the standard `MetadataEntry` set listed in `dagster_graphql/schema/metadata.py`.

### Where it is stored

Each evaluation is an `ASSET_CHECK_EVALUATION` event-log entry. In the serialized event the
metadata lives under `dagster_event.event_specific_data.metadata`, a mapping of label →
`*MetadataEntryData`, e.g. for the PNG check:

```json
{
  "success": true,
  "partition": "central",
  "metadata": {
    "encoding":  {"__class__": "TextMetadataEntryData", "text": "data:image/png;base64 in markdown"},
    "plot":      {"__class__": "MarkdownMetadataEntryData", "md_str": "![...](data:image/png;base64,...)"},
    "png_bytes": {"__class__": "IntMetadataEntryData", "value": 124582}
  }
}
```

Local runs store that event file under `$DAGSTER_HOME/history/runs/<run_id>.db`. The Vision
pipeline uses `dagster-postgres`, where the same JSON lands in the `event_logs` table. The
GraphQL resolver above is the stable query surface; the JSON column is an option if the Vision
UI wants to read Postgres directly, but it couples you to Dagster's internal serialization.

## Practical recommendations

1. **For a graph that is useful inside Dagster only:** return a PNG or SVG in markdown. SVGs are
   smaller for line charts. If you want the image visible without a click, keep the markdown
   under 1,000 chars (not possible with embedded base64) or accept the `[Show Markdown]` step.
2. **For graphs the Vision UI should query and redraw:** store a Parquet/CSV artifact and return
   `MetadataValue.url` / `MetadataValue.path`. If a small series must stay in metadata, use
   column-oriented `MetadataValue.json`, never a multi-thousand-row `MetadataValue.table`.
   Do not rely on the metadata blob as the only copy of the data.
3. **For key metrics:** return `MetadataValue.table` — it renders natively in Dagster and is
   directly queryable as typed columns/records. This is the right choice for tens of rows, not
   thousands: 11,520 rows cost 2.6 MB per partition and render a 659k-pixel-tall DOM. Pair a
   small table with int/float entries if you want Dagster's automatic numeric plots.
4. **For automatic plots:** return one float per KPI series. On an asset, use
   `context.add_output_metadata({...})` to get partition-axis charts in the asset Overview; on a
   check, numeric metadata gives time-axis charts across evaluations. A handful of floats costs
   bytes, versus megabytes for the equivalent raw series.
5. **Watch the event log size.** Embedded base64 images and full series inflate
   `ASSET_CHECK_EVALUATION` rows by 33% (base64) plus the original size. A 124 KB PNG becomes a
   166 KB markdown string per asset/partition/execution. Multiply by assets × partitions and
   this becomes the dominant cost of storing checks. Prefer external artifacts (GCS + URL) for
   the Vision pipeline; keep base64 images for small, ad-hoc diagnostics.
6. **Partitioned check gotchas (found the hard way):**
   - Decorator-based checks must be passed to `Definitions(asset_checks=[...])`, otherwise the
     selection cannot resolve their keys.
   - A check attached to a partitioned asset needs `partitions_def=...` on `@dg.asset_check`.
     Without it the check is unpartitioned and Dagster maps the asset input with
     `AllPartitionMapping`, so the check receives a `dict[partition_key, value]` for every
     partition instead of the current partition's value.
   - The implicit global asset job does not run checks. Select them explicitly:
     `AssetSelection.all() | AssetSelection.checks(*check_keys)` (or run them from the UI).

## Screenshots

Captured from the running UI with Playwright + the installed Microsoft Edge. Every file lives in
`screenshots/` and is named `NN__section__subject__detail.png` so the folder sorts by section.
[screenshots/README.md](screenshots/README.md) is a rendered gallery of all of them.

- `01__check-overview__*.png` — check panel Overview for each metadata approach (PNG/SVG
  markdown dialogs, scalar + auto plots, table + schema, JSON/Plotly dialogs, URL links).
- `02__check-partitions__*__east.png` — the check panel's Partitions tab after clicking
  the `east` partition; each shows that partition's evaluation metadata.
- `03__asset-overview__scalar-metadata.png` — asset Overview: latest check status only, no check
  metadata or check plots.
- `04__asset-partitions__scalar-metadata__east.png` — asset Partitions tab: materialization
  metadata only.
- `05__check-execution-history__scalar-metadata*.png` — execution-history table and the per-row
  `{}` metadata dialog for `east`.
- `06__timeseries-table__*.png` — 11,520-row table check: page top and partition detail.
- `06__timeseries-kpi__*.png` — eight-float KPI check: automatic time-axis plots in the check
  Overview and the per-partition metadata.
- `06__timeseries-asset__overview-plots-by-partition.png` — asset-level numeric metadata plotted
  against partition keys (the compact alternative to storing the series).
- `06__timeseries-asset__partitions__east.png` — per-partition asset metadata; long labels
  overlap in this pane.
- `07__timeseries-json__*.png` and `07__timeseries-columnar-json__*.png` — the two large JSON
  encodings and their per-partition metadata.
- `08__scalar-multi-check__*.png` — the five checks on `scalar_metadata_asset`, including pass,
  yellow warning-level failure, red error-level failure, and per-partition detail.
