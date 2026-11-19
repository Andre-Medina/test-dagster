"""Query asset check metadata over GraphQL, exactly as the Vision UI would."""

from __future__ import annotations

import base64
import json
import os
import re
import urllib.request
from pathlib import Path

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://127.0.0.1:3000/graphql")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DECODED_DIR = PROJECT_ROOT / "artifacts" / "decoded"

CHECKS = [
    ("png_markdown_asset", "png_markdown_check"),
    ("svg_markdown_asset", "svg_markdown_check"),
    ("scalar_metadata_asset", "scalar_check"),
    ("scalar_metadata_asset", "scalar_peak_limit"),
    ("scalar_metadata_asset", "scalar_pv_energy"),
    ("scalar_metadata_asset", "scalar_above_1000h"),
    ("scalar_metadata_asset", "scalar_row_count"),
    ("table_metadata_asset", "table_check"),
    ("json_metadata_asset", "json_check"),
    ("plotly_metadata_asset", "plotly_check"),
    ("url_artifact_asset", "url_artifact_check"),
    ("timeseries_metadata_asset", "timeseries_table_check"),
    ("timeseries_metadata_asset", "timeseries_kpi_check"),
    ("timeseries_metadata_asset", "timeseries_json_check"),
    ("timeseries_metadata_asset", "timeseries_columnar_json_check"),
]

QUERY = """
query CheckMetadata($assetKey: AssetKeyInput!, $checkName: String!) {
  assetCheckExecutions(assetKey: $assetKey, checkName: $checkName, limit: 20) {
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
        ... on IntMetadataEntry { intValue intRepr }
        ... on FloatMetadataEntry { floatValue floatRepr }
        ... on BoolMetadataEntry { boolValue }
        ... on TextMetadataEntry { text }
        ... on JsonMetadataEntry { jsonString }
        ... on UrlMetadataEntry { url }
        ... on PathMetadataEntry { path }
        ... on TimestampMetadataEntry { timestamp }
        ... on TableMetadataEntry {
          table {
            records
            schema { columns { name type } }
          }
        }
      }
    }
  }
}
"""


def query(asset: str, check: str) -> dict:
    """Execute the metadata query for one asset check."""
    payload = {
        "query": QUERY,
        "variables": {"assetKey": {"path": [asset]}, "checkName": check},
    }
    request = urllib.request.Request(
        DAGSTER_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:
        body = json.loads(response.read())
    if "errors" in body:
        raise RuntimeError(body["errors"])
    return body["data"]["assetCheckExecutions"]


def decode_markdown_images(entry: dict, stem: str) -> None:
    """Persist base64 data URIs embedded in markdown metadata for visual inspection."""
    match = re.search(
        r"!\[[^\]]*\]\(data:image/(?P<kind>[a-zA-Z+]+);base64,(?P<data>[^)]+)\)",
        entry["mdStr"],
    )
    if not match:
        print("    markdown has no embedded image")
        return

    kind = match.group("kind")
    suffix = "svg" if "svg" in kind else "png"
    DECODED_DIR.mkdir(parents=True, exist_ok=True)
    path = DECODED_DIR / f"{stem}.{suffix}"
    path.write_bytes(base64.b64decode(match.group("data")))
    print(f"    decoded image -> {path.relative_to(PROJECT_ROOT)} ({path.stat().st_size} bytes)")


def describe_entry(entry: dict, stem: str) -> None:
    """Print one metadata entry in a readable form."""
    typename = entry["__typename"]
    label = entry["label"]
    if typename == "MarkdownMetadataEntry":
        print(f"    {label} [{typename}] mdStr={len(entry['mdStr'])} chars")
        decode_markdown_images(entry, f"{stem}_{label}")
    elif typename == "IntMetadataEntry":
        print(f"    {label} [{typename}] {entry['intRepr']}")
    elif typename == "FloatMetadataEntry":
        print(f"    {label} [{typename}] {entry['floatRepr']}")
    elif typename == "BoolMetadataEntry":
        print(f"    {label} [{typename}] {entry['boolValue']}")
    elif typename == "TextMetadataEntry":
        print(f"    {label} [{typename}] {entry['text']}")
    elif typename == "JsonMetadataEntry":
        parsed = json.loads(entry["jsonString"])
        keys = list(parsed) if isinstance(parsed, dict) else f"list[{len(parsed)}]"
        print(
            f"    {label} [{typename}] json bytes={len(entry['jsonString'])} keys={keys}"
        )
    elif typename == "TableMetadataEntry":
        columns = [column["name"] for column in entry["table"]["schema"]["columns"]]
        records = entry["table"]["records"]
        payload_bytes = len(json.dumps(records))
        print(
            f"    {label} [{typename}] columns={columns} records={len(records)} "
            f"json_bytes={payload_bytes}"
        )
        for record in records[:3]:
            print(f"        {record[:240]}")
        if len(records) > 3:
            print(f"        ... {len(records) - 3} more records")
    elif typename in {"UrlMetadataEntry", "PathMetadataEntry"}:
        print(f"    {label} [{typename}] {entry.get('url') or entry.get('path')}")
    else:
        print(f"    {label} [{typename}] {entry}")


def main() -> None:
    """Print metadata for the latest execution of every check, grouped by partition."""
    for asset, check in CHECKS:
        executions = query(asset, check)
        print(f"\n=== {asset} / {check} ({len(executions)} executions) ===")
        for execution in executions:
            evaluation = execution.get("evaluation")
            if not evaluation:
                print(f"  {execution['partition']}: no evaluation yet ({execution['status']})")
                continue
            print(
                f"  partition={evaluation['partition']} success={evaluation['success']} "
                f"severity={evaluation.get('severity')} "
                f"description={evaluation.get('description')!r}"
            )
            for entry in evaluation["metadataEntries"]:
                describe_entry(entry, f"{asset}_{evaluation['partition']}")


if __name__ == "__main__":
    main()
