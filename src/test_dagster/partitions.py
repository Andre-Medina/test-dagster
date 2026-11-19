"""Shared categorical partition definition used by every experiment."""

import dagster as dg

from test_dagster.data import CATEGORIES

PARTITIONS = dg.StaticPartitionsDefinition(list(CATEGORIES))
