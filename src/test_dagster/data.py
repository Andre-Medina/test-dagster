"""Deterministic synthetic half-hourly load data for the metadata experiments."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import numpy as np
import polars as pl

HALF_HOURS_PER_DAY = 48

# Categorical, unordered partition keys. The synthetic month mapping only varies the generated
# shape (season, peak height); it is not part of the partition key semantics.
CATEGORIES = ("north", "south", "east", "west", "central")
CATEGORY_MONTH = {"north": 1, "south": 2, "east": 3, "west": 4, "central": 5}

CUSTOMER_SEGMENTS = ("residential", "commercial", "industrial", "public")
NETWORK_ASSETS = ("substation_north", "substation_east")
SEGMENT_SHARE = {
    "residential": 0.38,
    "commercial": 0.30,
    "industrial": 0.22,
    "public": 0.10,
}
ASSET_SHARE = {"substation_north": 0.60, "substation_east": 0.40}


def load_partition_series(partition_key: str) -> pl.DataFrame:
    """Return a deterministic half-hourly native/PV/net demand series for a categorical partition."""
    month = CATEGORY_MONTH.get(partition_key)
    start = (
        datetime(2025, month, 1)
        if month is not None
        else datetime.strptime(partition_key, "%Y-%m-%d")
    )
    steps = 30 * HALF_HOURS_PER_DAY
    rng = np.random.default_rng(
        int.from_bytes(hashlib.sha256(partition_key.encode()).digest()[:8], "big")
    )

    hour = (np.arange(steps) % HALF_HOURS_PER_DAY) / 2
    season = 1.0 + 0.18 * np.sin(2 * np.pi * start.month / 12)
    native_shape = (
        0.55
        + 0.25 * np.sin(2 * np.pi * (hour - 8) / 24)
        + 0.45 * np.exp(-((hour - 18.5) ** 2) / 6)
    )
    solar_shape = np.clip(np.sin(np.pi * (hour - 6) / 12), 0, None)

    native_kw = 900 * season * native_shape + rng.normal(0, 12, steps)
    pv_kw = -520 * solar_shape * rng.normal(1.0, 0.06, steps)

    return pl.DataFrame(
        {
            "timestamp": [start + timedelta(minutes=30 * i) for i in range(steps)],
            "native_kw": native_kw,
            "pv_kw": pv_kw,
            "net_kw": native_kw + pv_kw,
        }
    )


def load_segment_series(partition_key: str) -> pl.DataFrame:
    """Return the partition series split across every customer segment and network asset."""
    base = load_partition_series(partition_key)
    timestamps = base["timestamp"].to_list()
    native = base["native_kw"].to_numpy()
    pv = base["pv_kw"].to_numpy()
    frames = []
    for segment in CUSTOMER_SEGMENTS:
        for asset in NETWORK_ASSETS:
            seed = int.from_bytes(
                hashlib.sha256(f"{partition_key}|{segment}|{asset}".encode()).digest()[:8],
                "big",
            )
            rng = np.random.default_rng(seed)
            factor = SEGMENT_SHARE[segment] * ASSET_SHARE[asset]
            jitter = rng.normal(1.0, 0.03, base.height)
            rows = len(timestamps)
            seg_native = native * factor * jitter
            seg_pv = pv * factor * jitter
            frames.append(
                pl.DataFrame(
                    {
                        "timestamp": timestamps,
                        "customer_segment": [segment] * rows,
                        "network_asset": [asset] * rows,
                        "native_kw": seg_native,
                        "pv_kw": seg_pv,
                        "net_kw": seg_native + seg_pv,
                    }
                )
            )
    return pl.concat(frames)


def segment_records(df: pl.DataFrame) -> list[dict]:
    """Return the grouped series as JSON-safe records with ISO timestamps."""
    return (
        df.with_columns(pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S"))
        .to_dicts()
    )


def summarise(df: pl.DataFrame) -> dict[str, float]:
    """Return the headline metrics used by the scalar/table/JSON metadata experiments."""
    net = df["net_kw"]
    return {
        "peak_net_kw": float(net.max()),
        "min_net_kw": float(net.min()),
        "mean_net_kw": float(net.mean()),
        "native_energy_kwh": float(df["native_kw"].sum() * 0.5),
        "pv_energy_kwh": float(-df["pv_kw"].sum() * 0.5),
        "net_energy_kwh": float(net.sum() * 0.5),
        "hours_net_above_1000_kw": float((net > 1000).sum() * 0.5),
    }
