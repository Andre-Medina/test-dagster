"""Plot builders for the PNG, SVG, and Plotly metadata experiments."""

from __future__ import annotations

import base64
import io
import json

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import plotly.graph_objects as go
import polars as pl


def _matplotlib_figure(df: pl.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 3.6), dpi=110)
    ts = df["timestamp"].to_numpy()
    ax.plot(ts, df["native_kw"].to_numpy(), label="Native", color="#4C78A8", linewidth=1.0)
    ax.plot(ts, df["pv_kw"].to_numpy(), label="PV", color="#F58518", linewidth=1.0)
    ax.plot(ts, df["net_kw"].to_numpy(), label="Net", color="#54A24B", linewidth=1.2)
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_ylabel("kW")
    ax.legend(loc="upper right", ncol=3, frameon=False, fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def raster_bytes(df: pl.DataFrame) -> bytes:
    """Render the series to PNG bytes."""
    fig = _matplotlib_figure(df)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


def vector_bytes(df: pl.DataFrame) -> bytes:
    """Render the series to SVG bytes."""
    fig = _matplotlib_figure(df)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg")
    plt.close(fig)
    return buf.getvalue()


def raster_data_uri(df: pl.DataFrame) -> tuple[str, int]:
    """Render the series to a PNG data URI and return it with the raw byte count."""
    raw = raster_bytes(df)
    return f"data:image/png;base64,{base64.b64encode(raw).decode('ascii')}", len(raw)


def vector_data_uri(df: pl.DataFrame) -> tuple[str, int]:
    """Render the series to an SVG data URI and return it with the raw byte count."""
    raw = vector_bytes(df)
    return f"data:image/svg+xml;base64,{base64.b64encode(raw).decode('ascii')}", len(raw)


def plotly_figure_dict(df: pl.DataFrame) -> dict:
    """Return the series as a plain-JSON Plotly figure definition."""
    fig = go.Figure()
    fig.add_scatter(
        x=df["timestamp"].to_list(),
        y=df["net_kw"].to_list(),
        mode="lines",
        name="Net",
        line={"color": "#54A24B"},
    )
    fig.update_layout(
        title="Net demand",
        xaxis_title="Timestamp",
        yaxis_title="kW",
        template="plotly_white",
    )
    return json.loads(fig.to_json())
