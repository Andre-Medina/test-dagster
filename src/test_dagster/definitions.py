"""One partitioned asset and one check per metadata-rendering approach.

Each check returns ``AssetCheckResult`` metadata in a different shape so the
Dagster UI rendering and the persisted GraphQL payloads can be compared:

* ``png_markdown``   - matplotlib PNG embedded as a base64 data URI in markdown
* ``svg_markdown``   - matplotlib SVG embedded as a base64 data URI in markdown
* ``scalar``         - int / float / bool / text / timestamp metadata
* ``table``          - ``MetadataValue.table`` with an explicit schema
* ``json``           - structured ``MetadataValue.json`` payload (plot-ready)
* ``plotly_json``    - Plotly figure serialised to JSON (no native chart support)
* ``url_artifact``   - PNG + CSV written to disk and referenced by URL/path
"""

import json
import random
import time
from pathlib import Path

import dagster as dg
import polars as pl

from test_dagster.cross_asset import CROSS_ASSETS, CROSS_CHECKS, CROSS_CHECK_KEYS
from test_dagster.data import (
    load_daily_profile,
    load_partition_series,
    load_segment_series,
    segment_records,
    summarise,
)
from test_dagster.partitions import PARTITIONS
from test_dagster.plots import (
    minimal_svg_data_uri,
    plotly_figure_dict,
    raster_bytes,
    raster_data_uri,
    vector_data_uri,
)

CHECK_TABLE_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn(name="metric", type="string"),
        dg.TableColumn(name="value", type="float"),
        dg.TableColumn(name="unit", type="string"),
    ]
)

TIMESERIES_TABLE_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn(name="timestamp", type="string"),
        dg.TableColumn(name="customer_segment", type="string"),
        dg.TableColumn(name="network_asset", type="string"),
        dg.TableColumn(name="native_kw", type="float"),
        dg.TableColumn(name="pv_kw", type="float"),
        dg.TableColumn(name="net_kw", type="float"),
    ]
)


def _series(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return load_partition_series(context.partition_key)


# --------------------------------------------------------------------------- PNG


@dg.asset(partitions_def=PARTITIONS, group_name="png", compute_kind="polars")
def png_markdown_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the PNG-in-markdown metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=png_markdown_asset,
    partitions_def=PARTITIONS,
    description="PNG plotted as a base64 markdown image.",
)
def png_markdown_check(
    context: dg.AssetCheckExecutionContext, png_markdown_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return the partition series as a PNG embedded in markdown metadata."""
    data_uri, size = raster_data_uri(png_markdown_asset)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "plot": dg.MetadataValue.md(
                f"![Net demand {context.partition_key}]({data_uri})"
            ),
            "png_bytes": dg.MetadataValue.int(size),
            "encoding": dg.MetadataValue.text("data:image/png;base64 in markdown"),
        },
        description=f"Rendered PNG for {context.partition_key}",
    )


# --------------------------------------------------------------------------- SVG


@dg.asset(partitions_def=PARTITIONS, group_name="svg", compute_kind="polars")
def svg_markdown_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the SVG-in-markdown metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=svg_markdown_asset,
    partitions_def=PARTITIONS,
    description="SVG plotted as a base64 markdown image.",
)
def svg_markdown_check(
    context: dg.AssetCheckExecutionContext, svg_markdown_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return the partition series as an SVG embedded in markdown metadata."""
    data_uri, size = vector_data_uri(svg_markdown_asset)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "plot": dg.MetadataValue.md(
                f"![Net demand {context.partition_key}]({data_uri})"
            ),
            "svg_bytes": dg.MetadataValue.int(size),
            "encoding": dg.MetadataValue.text("data:image/svg+xml;base64 in markdown"),
        },
        description=f"Rendered SVG for {context.partition_key}",
    )


# ------------------------------------------------------------------------ SCALARS


@dg.asset(partitions_def=PARTITIONS, group_name="scalar", compute_kind="polars")
def scalar_metadata_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the scalar metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=scalar_metadata_asset,
    partitions_def=PARTITIONS,
    description="Native int/float/bool/text metadata.",
)
def scalar_check(
    context: dg.AssetCheckExecutionContext, scalar_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return headline partition metrics as native scalar metadata."""
    metrics = summarise(scalar_metadata_asset)
    peak = metrics["peak_net_kw"]
    return dg.AssetCheckResult(
        passed=peak < 1200.0,
        metadata={
            "peak_net_kw": dg.MetadataValue.float(peak),
            "min_net_kw": dg.MetadataValue.float(metrics["min_net_kw"]),
            "pv_energy_kwh": dg.MetadataValue.float(metrics["pv_energy_kwh"]),
            "hours_net_above_1000_kw": dg.MetadataValue.float(
                metrics["hours_net_above_1000_kw"]
            ),
            "half_hour_rows": dg.MetadataValue.int(scalar_metadata_asset.height),
            "peak_is_under_limit": dg.MetadataValue.bool(peak < 1200.0),
            "partition": dg.MetadataValue.text(context.partition_key),
            "evaluated_at": dg.MetadataValue.timestamp(time.time()),
        },
        severity=dg.AssetCheckSeverity.WARN,
        description="Peak net demand under the 1200 kW test limit"
        if peak < 1200.0
        else f"Peak net demand {peak:.0f} kW exceeds the 1200 kW test limit",
    )


SCALAR_INDIVIDUAL_CHECKS = (
    "scalar_peak_limit",
    "scalar_pv_energy",
    "scalar_above_1000h",
    "scalar_row_count",
)


def _simulated_outcome(
    partition_key: str, check_name: str
) -> tuple[bool, dg.AssetCheckSeverity, str]:
    """Return a deterministic pseudo-random pass/fail result and its severity."""
    badness = random.Random(f"{partition_key}:{check_name}").random()
    if badness < 0.55:
        return True, dg.AssetCheckSeverity.ERROR, "simulated pass"
    if badness < 0.80:
        return False, dg.AssetCheckSeverity.WARN, "simulated warning-level failure"
    return False, dg.AssetCheckSeverity.ERROR, "simulated error-level failure"


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(
            name="scalar_peak_limit",
            asset=scalar_metadata_asset,
            partitions_def=PARTITIONS,
            description="Peak net demand against the 1200 kW limit.",
        ),
        dg.AssetCheckSpec(
            name="scalar_pv_energy",
            asset=scalar_metadata_asset,
            partitions_def=PARTITIONS,
            description="PV energy against the 90 MWh target.",
        ),
        dg.AssetCheckSpec(
            name="scalar_above_1000h",
            asset=scalar_metadata_asset,
            partitions_def=PARTITIONS,
            description="Hours above 1000 kW against the 60 h budget.",
        ),
        dg.AssetCheckSpec(
            name="scalar_row_count",
            asset=scalar_metadata_asset,
            partitions_def=PARTITIONS,
            description="Half-hour row count against the expected 1,440.",
        ),
    ],
    name="scalar_individual_checks",
)
def scalar_individual_checks(
    context: dg.AssetCheckExecutionContext, scalar_metadata_asset: pl.DataFrame
):
    """Yield four independent scalar checks with simulated outcomes and small metadata."""
    metrics = summarise(scalar_metadata_asset)
    results = {
        "scalar_peak_limit": {
            "metadata": {
                "peak_net_kw": dg.MetadataValue.float(metrics["peak_net_kw"]),
                "limit_kw": dg.MetadataValue.float(1200.0),
            }
        },
        "scalar_pv_energy": {
            "metadata": {
                "pv_energy_kwh": dg.MetadataValue.float(metrics["pv_energy_kwh"]),
                "target_kwh": dg.MetadataValue.float(90000.0),
            }
        },
        "scalar_above_1000h": {
            "metadata": {
                "hours_net_above_1000_kw": dg.MetadataValue.float(
                    metrics["hours_net_above_1000_kw"]
                ),
                "budget_h": dg.MetadataValue.float(60.0),
            }
        },
        "scalar_row_count": {
            "metadata": {
                "half_hour_rows": dg.MetadataValue.int(scalar_metadata_asset.height),
                "expected_rows": dg.MetadataValue.int(1440),
            }
        },
    }
    for check_name in SCALAR_INDIVIDUAL_CHECKS:
        passed, severity, outcome = _simulated_outcome(
            context.partition_key, check_name
        )
        yield dg.AssetCheckResult(
            asset_key="scalar_metadata_asset",
            check_name=check_name,
            passed=passed,
            severity=severity,
            metadata=results[check_name]["metadata"],
            description=f"{outcome} for {context.partition_key}",
        )


# -------------------------------------------------------------------------- TABLE


@dg.asset(partitions_def=PARTITIONS, group_name="table", compute_kind="polars")
def table_metadata_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the table metadata experiment."""
    return _series(context)


def _metric_rows(metrics: dict[str, float]) -> list[dg.TableRecord]:
    units = {
        "peak_net_kw": "kW",
        "min_net_kw": "kW",
        "mean_net_kw": "kW",
        "native_energy_kwh": "kWh",
        "pv_energy_kwh": "kWh",
        "net_energy_kwh": "kWh",
        "hours_net_above_1000_kw": "h",
    }
    return [
        dg.TableRecord({"metric": name, "value": value, "unit": units[name]})
        for name, value in metrics.items()
    ]


@dg.asset_check(
    asset=table_metadata_asset,
    partitions_def=PARTITIONS,
    description="Structured table metadata.",
)
def table_check(
    context: dg.AssetCheckExecutionContext, table_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return headline partition metrics as a schema-typed table."""
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "summary_table": dg.MetadataValue.table(
                records=_metric_rows(summarise(table_metadata_asset)),
                schema=CHECK_TABLE_SCHEMA,
            )
        },
        description=f"Key metrics for {context.partition_key}",
    )


# --------------------------------------------------------------------------- JSON


@dg.asset(partitions_def=PARTITIONS, group_name="json", compute_kind="polars")
def json_metadata_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the JSON metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=json_metadata_asset,
    partitions_def=PARTITIONS,
    description="Structured JSON payload.",
)
def json_check(
    context: dg.AssetCheckExecutionContext, json_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return a plot-ready JSON payload (summary plus half-hourly series)."""
    payload = {
        "partition": context.partition_key,
        "summary": summarise(json_metadata_asset),
        "series": {
            "timestamp": [
                value.isoformat() for value in json_metadata_asset["timestamp"].to_list()
            ],
            "net_kw": [round(value, 3) for value in json_metadata_asset["net_kw"].to_list()],
        },
    }
    encoded = json.dumps(payload)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "payload": dg.MetadataValue.json(payload),
            "json_bytes": dg.MetadataValue.int(len(encoded)),
        },
        description=f"JSON payload for {context.partition_key}",
    )


# ------------------------------------------------------------------------ PLOTLY


@dg.asset(partitions_def=PARTITIONS, group_name="plotly", compute_kind="polars")
def plotly_metadata_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the Plotly JSON metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=plotly_metadata_asset,
    partitions_def=PARTITIONS,
    description="Plotly figure serialised to JSON.",
)
def plotly_check(
    context: dg.AssetCheckExecutionContext, plotly_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return a Plotly figure as JSON metadata (Dagster 1.13 has no native chart renderer)."""
    figure = plotly_figure_dict(plotly_metadata_asset)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "figure": dg.MetadataValue.json(figure),
            "figure_json_bytes": dg.MetadataValue.int(len(json.dumps(figure))),
            "note": dg.MetadataValue.text(
                "Plotly JSON is stored and queryable, but the Dagster UI renders it as JSON, "
                "not as an interactive chart."
            ),
        },
        description=f"Plotly JSON for {context.partition_key}",
    )


# ----------------------------------------------------------------- URL ARTIFACT


@dg.asset(partitions_def=PARTITIONS, group_name="url", compute_kind="polars")
def url_artifact_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series for the external-artifact metadata experiment."""
    return _series(context)


@dg.asset_check(
    asset=url_artifact_asset,
    partitions_def=PARTITIONS,
    description="PNG/CSV written to disk and linked.",
)
def url_artifact_check(
    context: dg.AssetCheckExecutionContext, url_artifact_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Write the plot and metrics to disk and return URL/path metadata."""
    artifact_dir = Path(context.instance.storage_directory()) / "check_artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    asset_name = "url_artifact_asset"

    png_path = artifact_dir / f"{asset_name}_{context.partition_key}.png"
    csv_path = artifact_dir / f"{asset_name}_{context.partition_key}.csv"

    png_path.write_bytes(raster_bytes(url_artifact_asset))
    metrics = pl.DataFrame(
        [
            {"metric": name, "value": value}
            for name, value in summarise(url_artifact_asset).items()
        ]
    )
    metrics.write_csv(csv_path)

    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "plot_png": dg.MetadataValue.url(png_path.as_uri()),
            "metrics_csv": dg.MetadataValue.url(csv_path.as_uri()),
            "plot_path": dg.MetadataValue.path(png_path),
            "link": dg.MetadataValue.md(f"[Open PNG]({png_path.as_uri()})"),
        },
        description=f"Artifacts written for {context.partition_key}",
    )


# ---------------------------------------------------------- MINIMAL SVG


@dg.asset(partitions_def=PARTITIONS, group_name="minimal_svg", compute_kind="polars")
def minimal_svg_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """24 hourly points of the partition's mean daily profile."""
    return load_daily_profile(context.partition_key)


@dg.asset_check(
    asset=minimal_svg_asset,
    partitions_def=PARTITIONS,
    description="24-point daily profile as a hand-built minimal SVG.",
)
def minimal_svg_check(
    context: dg.AssetCheckExecutionContext, minimal_svg_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return the daily profile as the smallest useful SVG in markdown metadata."""
    data_uri, svg_bytes = minimal_svg_data_uri(minimal_svg_asset)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "plot": dg.MetadataValue.md(
                f"![Daily profile {context.partition_key}]({data_uri})"
            ),
            "svg_bytes": dg.MetadataValue.int(svg_bytes),
            "points": dg.MetadataValue.int(minimal_svg_asset.height),
        },
        description=f"Minimal SVG for {context.partition_key}",
    )


# ------------------------------------------------------- GROUPED TIMESERIES


def _group_energy_values(df: pl.DataFrame) -> dict[str, float]:
    grouped = df.group_by(["customer_segment", "network_asset"]).agg(
        (pl.col("net_kw").sum() * 0.5).alias("net_energy_kwh")
    )
    return {
        f"net_energy_kwh__{row['customer_segment']}__{row['network_asset']}": float(
            row["net_energy_kwh"]
        )
        for row in grouped.iter_rows(named=True)
    }


@dg.asset(partitions_def=PARTITIONS, group_name="timeseries", compute_kind="polars")
def timeseries_metadata_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Partition series split by customer segment and network asset."""
    df = load_segment_series(context.partition_key)
    context.add_output_metadata(_group_energy_values(df))
    return df


@dg.asset_check(
    asset=timeseries_metadata_asset,
    partitions_def=PARTITIONS,
    description="Full long-format timeseries as table metadata.",
)
def timeseries_table_check(
    context: dg.AssetCheckExecutionContext, timeseries_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return every half-hourly row as a schema-typed table."""
    records = [
        dg.TableRecord(row) for row in segment_records(timeseries_metadata_asset)
    ]
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "timeseries_table": dg.MetadataValue.table(
                records=records, schema=TIMESERIES_TABLE_SCHEMA
            ),
            "row_count": dg.MetadataValue.int(len(records)),
        },
        description=f"{len(records)} grouped rows for {context.partition_key}",
    )


@dg.asset_check(
    asset=timeseries_metadata_asset,
    partitions_def=PARTITIONS,
    description="Eight per-group net-energy KPIs as numeric metadata (no raw series).",
)
def timeseries_kpi_check(
    context: dg.AssetCheckExecutionContext, timeseries_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return one float per customer segment and network asset for automatic plotting."""
    values = _group_energy_values(timeseries_metadata_asset)
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            label: dg.MetadataValue.float(value) for label, value in values.items()
        },
        description=f"{len(values)} group KPIs for {context.partition_key}",
    )


@dg.asset_check(
    asset=timeseries_metadata_asset,
    partitions_def=PARTITIONS,
    description="Full long-format timeseries as row-oriented JSON metadata.",
)
def timeseries_json_check(
    context: dg.AssetCheckExecutionContext, timeseries_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return the same rows as one row-oriented JSON payload."""
    records = segment_records(timeseries_metadata_asset)
    payload = {"partition": context.partition_key, "rows": records}
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "payload": dg.MetadataValue.json(payload),
            "json_bytes": dg.MetadataValue.int(len(json.dumps(payload))),
        },
        description=f"{len(records)} row-oriented JSON records",
    )


@dg.asset_check(
    asset=timeseries_metadata_asset,
    partitions_def=PARTITIONS,
    description="Full long-format timeseries as column-oriented JSON metadata.",
)
def timeseries_columnar_json_check(
    context: dg.AssetCheckExecutionContext, timeseries_metadata_asset: pl.DataFrame
) -> dg.AssetCheckResult:
    """Return the same rows as column-oriented JSON."""
    frame = timeseries_metadata_asset.with_columns(
        pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S")
    )
    columns = frame.to_dict(as_series=False)
    payload = {"partition": context.partition_key, "columns": columns}
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "payload": dg.MetadataValue.json(payload),
            "json_bytes": dg.MetadataValue.int(len(json.dumps(payload))),
        },
        description=f"{frame.height} column-oriented JSON rows",
    )


PNG_ASSET = png_markdown_asset
SVG_ASSET = svg_markdown_asset
SCALAR_ASSET = scalar_metadata_asset
TABLE_ASSET = table_metadata_asset
JSON_ASSET = json_metadata_asset
PLOTLY_ASSET = plotly_metadata_asset
URL_ASSET = url_artifact_asset
MINIMAL_ASSET = minimal_svg_asset
TIMESERIES_ASSET = timeseries_metadata_asset

CHECK_KEYS = [
    dg.AssetCheckKey(png_markdown_asset.key, "png_markdown_check"),
    dg.AssetCheckKey(svg_markdown_asset.key, "svg_markdown_check"),
    dg.AssetCheckKey(scalar_metadata_asset.key, "scalar_check"),
    *[
        dg.AssetCheckKey(scalar_metadata_asset.key, name)
        for name in SCALAR_INDIVIDUAL_CHECKS
    ],
    dg.AssetCheckKey(table_metadata_asset.key, "table_check"),
    dg.AssetCheckKey(json_metadata_asset.key, "json_check"),
    dg.AssetCheckKey(plotly_metadata_asset.key, "plotly_check"),
    dg.AssetCheckKey(url_artifact_asset.key, "url_artifact_check"),
    dg.AssetCheckKey(minimal_svg_asset.key, "minimal_svg_check"),
    dg.AssetCheckKey(timeseries_metadata_asset.key, "timeseries_table_check"),
    dg.AssetCheckKey(timeseries_metadata_asset.key, "timeseries_kpi_check"),
    dg.AssetCheckKey(timeseries_metadata_asset.key, "timeseries_json_check"),
    dg.AssetCheckKey(timeseries_metadata_asset.key, "timeseries_columnar_json_check"),
    *CROSS_CHECK_KEYS,
]

metadata_job = dg.define_asset_job(
    "metadata_checks_job",
    selection=dg.AssetSelection.all() | dg.AssetSelection.checks(*CHECK_KEYS),
)

defs = dg.Definitions(
    assets=[
        png_markdown_asset,
        svg_markdown_asset,
        scalar_metadata_asset,
        table_metadata_asset,
        json_metadata_asset,
        plotly_metadata_asset,
        url_artifact_asset,
        minimal_svg_asset,
        timeseries_metadata_asset,
        *CROSS_ASSETS,
    ],
    asset_checks=[
        png_markdown_check,
        svg_markdown_check,
        scalar_check,
        scalar_individual_checks,
        table_check,
        json_check,
        plotly_check,
        url_artifact_check,
        minimal_svg_check,
        timeseries_table_check,
        timeseries_kpi_check,
        timeseries_json_check,
        timeseries_columnar_json_check,
        *CROSS_CHECKS,
    ],
    jobs=[metadata_job],
)
