"""Two cross-asset check patterns, both comparing assets' date coverage.

* ``orders_same_dates_as_prices_check`` - a check owned by ``orders_asset`` that reads
  ``prices_asset`` through ``additional_ins``; the result is recorded only against orders.
* ``orders_prices_date_checks`` - one ``@multi_asset_check`` with a spec per asset; it yields
  one result for orders and one for prices.
"""

from datetime import date

import dagster as dg
import polars as pl

from test_dagster.data import load_partition_series
from test_dagster.partitions import PARTITIONS


def _daily_frame(partition_key: str, value_name: str, factor: float) -> pl.DataFrame:
    """Return one row per date with the mean net demand as the named value column."""
    base = load_partition_series(partition_key)
    return (
        base.select(
            pl.col("timestamp").dt.date().alias("date"),
            (pl.col("net_kw") * factor).alias(value_name),
        )
        .group_by("date")
        .agg(pl.col(value_name).mean())
        .sort("date")
    )


@dg.asset(partitions_def=PARTITIONS, group_name="cross_asset", compute_kind="polars")
def orders_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Daily orders signal for the cross-asset date-consistency examples."""
    return _daily_frame(context.partition_key, "orders", 1.0)


@dg.asset(partitions_def=PARTITIONS, group_name="cross_asset", compute_kind="polars")
def prices_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Daily prices signal for the cross-asset date-consistency examples."""
    return _daily_frame(context.partition_key, "prices", 1.1)


def _compare_dates(
    left: pl.DataFrame, right: pl.DataFrame
) -> tuple[list[date], list[date], list[date], list[date]]:
    left_dates = set(left["date"].to_list())
    right_dates = set(right["date"].to_list())
    return (
        sorted(left_dates),
        sorted(right_dates),
        sorted(left_dates - right_dates),
        sorted(right_dates - left_dates),
    )


def _range(values: list[date]) -> str:
    return f"{values[0]} .. {values[-1]}" if values else "empty"


@dg.asset_check(
    asset=orders_asset,
    name="orders_same_dates_as_prices",
    partitions_def=PARTITIONS,
    additional_ins={"prices_asset": dg.AssetIn("prices_asset")},
    description="Orders and prices cover the same dates (additional_ins example).",
)
def orders_same_dates_as_prices_check(
    context: dg.AssetCheckExecutionContext,
    orders_asset: pl.DataFrame,
    prices_asset: pl.DataFrame,
) -> dg.AssetCheckResult:
    """Check two assets' dates through an additional input on one asset check."""
    orders_dates, prices_dates, missing, extra = _compare_dates(
        orders_asset, prices_asset
    )
    passed = not missing and not extra
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "orders_rows": dg.MetadataValue.int(orders_asset.height),
            "prices_rows": dg.MetadataValue.int(prices_asset.height),
            "missing_in_prices": dg.MetadataValue.int(len(missing)),
            "extra_in_prices": dg.MetadataValue.int(len(extra)),
            "orders_range": dg.MetadataValue.text(_range(orders_dates)),
            "prices_range": dg.MetadataValue.text(_range(prices_dates)),
        },
        description=(
            f"Identical dates for {context.partition_key}"
            if passed
            else f"{len(missing)} dates only in orders, {len(extra)} only in prices"
        ),
    )


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(
            name="orders_dates_match_prices",
            asset=orders_asset,
            partitions_def=PARTITIONS,
            description="Orders and prices cover the same dates (multi-check example).",
        ),
        dg.AssetCheckSpec(
            name="prices_dates_match_orders",
            asset=prices_asset,
            partitions_def=PARTITIONS,
            description="Prices and orders cover the same dates (multi-check example).",
        ),
    ],
    ins={
        "orders_asset": dg.AssetIn("orders_asset"),
        "prices_asset": dg.AssetIn("prices_asset"),
    },
)
def orders_prices_date_checks(
    context: dg.AssetCheckExecutionContext,
    orders_asset: pl.DataFrame,
    prices_asset: pl.DataFrame,
):
    """Yield one cross-asset date-consistency result for each asset."""
    _, _, missing, extra = _compare_dates(orders_asset, prices_asset)
    passed = not missing and not extra
    shared = {
        "orders_rows": dg.MetadataValue.int(orders_asset.height),
        "prices_rows": dg.MetadataValue.int(prices_asset.height),
        "partition": dg.MetadataValue.text(context.partition_key),
    }
    yield dg.AssetCheckResult(
        asset_key="orders_asset",
        check_name="orders_dates_match_prices",
        passed=passed,
        metadata={
            **shared,
            "missing_in_prices": dg.MetadataValue.int(len(missing)),
            "extra_in_prices": dg.MetadataValue.int(len(extra)),
        },
        description="Identical dates" if passed else "Date coverage differs",
    )
    yield dg.AssetCheckResult(
        asset_key="prices_asset",
        check_name="prices_dates_match_orders",
        passed=passed,
        metadata={
            **shared,
            "missing_in_orders": dg.MetadataValue.int(len(extra)),
            "extra_in_orders": dg.MetadataValue.int(len(missing)),
        },
        description="Identical dates" if passed else "Date coverage differs",
    )


CROSS_ASSETS = [orders_asset, prices_asset]
CROSS_CHECKS = [orders_same_dates_as_prices_check, orders_prices_date_checks]
CROSS_CHECK_KEYS = [
    dg.AssetCheckKey(orders_asset.key, "orders_same_dates_as_prices"),
    dg.AssetCheckKey(orders_asset.key, "orders_dates_match_prices"),
    dg.AssetCheckKey(prices_asset.key, "prices_dates_match_orders"),
]
