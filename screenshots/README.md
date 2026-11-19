# UI screenshot gallery

Captured from the local Dagster UI (1.13.30) with Playwright + Microsoft Edge, viewport
1600×1000. Filenames follow `NN__section__subject__detail.png`; the double underscores keep
the folder sorted by section. Full findings are in the [top-level README](../README.md).

## 01 — Check panel, Overview tab

| PNG markdown | SVG markdown |
| :---: | :---: |
| ![PNG markdown](01__check-overview__png-markdown.png) | ![SVG markdown](01__check-overview__svg-markdown.png) |

| Scalar metadata + auto plots | Table metadata |
| :---: | :---: |
| ![Scalar](01__check-overview__scalar-metadata.png) | ![Table](01__check-overview__table-metadata.png) |

| Table schema | JSON payload |
| :---: | :---: |
| ![Table schema](01__check-overview__table-metadata__schema.png) | ![JSON](01__check-overview__json-metadata.png) |

| Plotly JSON | URL artifact |
| :---: | :---: |
| ![Plotly](01__check-overview__plotly-metadata.png) | ![URL](01__check-overview__url-artifact.png) |

## 02 — Check panel, Partitions tab (east selected)

| PNG | SVG | Scalar |
| :---: | :---: | :---: |
| ![PNG](02__check-partitions__png-markdown__east.png) | ![SVG](02__check-partitions__svg-markdown__east.png) | ![Scalar](02__check-partitions__scalar-metadata__east.png) |

| Table | JSON | Plotly |
| :---: | :---: | :---: |
| ![Table](02__check-partitions__table-metadata__east.png) | ![JSON](02__check-partitions__json-metadata__east.png) | ![Plotly](02__check-partitions__plotly-metadata__east.png) |

| URL artifact |
| :---: |
| ![URL](02__check-partitions__url-artifact__east.png) |

## 03–05 — Asset-level tabs and execution history

| Asset Overview | Asset Partitions | Execution history |
| :---: | :---: | :---: |
| ![Asset overview](03__asset-overview__scalar-metadata.png) | ![Asset partitions](04__asset-partitions__scalar-metadata__east.png) | ![Execution history](05__check-execution-history__scalar-metadata.png) |

| Execution-history metadata dialog |
| :---: |
| ![Execution metadata](05__check-execution-history__scalar-metadata__east.png) |

## 06 — Grouped timeseries (11,520 rows/partition)

| Table check, top | Table check, partition detail |
| :---: | :---: |
| ![Table top](06__timeseries-table__check-overview.png) | ![Partition detail](06__timeseries-table__check-partitions__east.png) |

| KPI check, auto plots (time axis) | KPI check, partition detail |
| :---: | :---: |
| ![KPI plots](06__timeseries-kpi__check-overview.png) | ![KPI partition](06__timeseries-kpi__check-partitions__east.png) |

| Asset Overview, plots by partition | Asset Partitions, numeric metadata |
| :---: | :---: |
| ![Asset plots by partition](06__timeseries-asset__overview-plots-by-partition.png) | ![Asset partition metadata](06__timeseries-asset__partitions__east.png) |

## 07 — Row-oriented vs column-oriented JSON

| Row-oriented JSON | Column-oriented JSON | Row JSON, partition detail |
| :---: | :---: | :---: |
| ![Row JSON](07__timeseries-json__check-overview.png) | ![Columnar JSON](07__timeseries-columnar-json__check-overview.png) | ![Row JSON partition](07__timeseries-json__check-partitions__east.png) |

## 08 — Four checks yielded by one `multi_asset_check`

| Five checks on the asset | Pass | WARN failure |
| :---: | :---: | :---: |
| ![Check list](08__scalar-multi-check__checks-list.png) | ![Pass](08__scalar-multi-check__overview__pass.png) | ![WARN failure](08__scalar-multi-check__overview__warn-fail.png) |

| ERROR failure | Partition detail |
| :---: | :---: |
| ![ERROR failure](08__scalar-multi-check__overview__error-fail.png) | ![Partition detail](08__scalar-multi-check__partitions__east.png) |
