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


def minimal_svg(df: pl.DataFrame, width: int = 380, height: int = 170) -> str:
    """Render a 24-point daily profile as a hand-built, legend-free SVG."""
    hours = df["hour"].to_list()
    values = [float(value) for value in df["net_kw"].to_list()]
    left, right, top, bottom = 36, 10, 16, 28
    plot_w = width - left - right
    plot_h = height - top - bottom
    vmin, vmax = min(values), max(values)
    span = (vmax - vmin) or 1.0
    hour_span = (hours[-1] - hours[0]) or 1

    xs = [round(left + plot_w * (hour - hours[0]) / hour_span) for hour in hours]
    ys = [round(top + plot_h * (1 - (value - vmin) / span)) for value in values]
    points = " ".join(f"{x},{y}" for x, y in zip(xs, ys))

    min_i = values.index(vmin)
    max_i = values.index(vmax)
    min_x = min(max(xs[min_i], left + 16), left + plot_w - 16)
    max_x = min(max(xs[max_i], left + 16), left + plot_w - 16)
    min_y = min(ys[min_i] + 12, height - 16)
    max_y = max(ys[max_i] - 6, 10)
    axis_bottom = top + plot_h

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-size="8">'
        f'<path d="M{left} {top}V{axis_bottom}H{left + plot_w}" '
        f'fill="none" stroke="#000" stroke-width="1"/>'
        f'<polyline points="{points}" fill="none" stroke="#000" stroke-width="1.5"/>'
        f'<text x="{left}" y="{height - 5}" text-anchor="start">{hours[0]}h</text>'
        f'<text x="{left + plot_w}" y="{height - 5}" text-anchor="end">{hours[-1]}h</text>'
        f'<text x="{left - 4}" y="{top + 3}" text-anchor="end">{vmax:.0f}</text>'
        f'<text x="{left - 4}" y="{axis_bottom + 3}" text-anchor="end">{vmin:.0f}</text>'
        f'<text x="{max_x}" y="{max_y}" text-anchor="middle">'
        f"max {vmax:.0f}@{hours[max_i]}h</text>"
        f'<text x="{min_x}" y="{min_y}" text-anchor="middle">'
        f"min {vmin:.0f}@{hours[min_i]}h</text>"
        "</svg>"
    )


def minimal_svg_data_uri(df: pl.DataFrame) -> tuple[str, int]:
    """Return the minimal SVG as a data URI plus its raw byte count."""
    svg = minimal_svg(df)
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}", len(svg.encode("utf-8"))


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
