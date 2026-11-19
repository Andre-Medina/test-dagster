"""Materialize every asset and its check for all categorical partitions."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DAGSTER_HOME", str(PROJECT_ROOT / ".dagster_home"))

import dagster as dg  # noqa: E402

from test_dagster.definitions import PARTITIONS, defs  # noqa: E402


def main() -> None:
    """Run the implicit asset job once per partition against the local instance."""
    instance = dg.DagsterInstance.get()
    job = defs.get_job_def("metadata_checks_job")

    for partition_key in PARTITIONS.get_partition_keys():
        result = job.execute_in_process(
            instance=instance,
            partition_key=partition_key,
            raise_on_error=False,
        )
        print(f"{partition_key}: run_success={result.success}")


if __name__ == "__main__":
    main()
