"""Medallion layout on local disk, with atomic writes.

    data/bronze/ds=YYYY-MM-DD/repo=<name>/{pull_requests,reviews,...}.json + _manifest.json
    data/quarantine/ds=YYYY-MM-DD/<entity>.jsonl
    data/silver/ds=YYYY-MM-DD/<entity>.parquet + _stats.json
    data/gold/ds=YYYY-MM-DD/{pr_compliance.parquet, summary.parquet, summary.json}

Every file is written to ``<name>.tmp`` and then ``os.replace``-d, so a crash never
leaves a half-written file. A repo's bronze partition is built in a temp directory and
swapped in as a whole, so a rerun replaces it atomically.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ds_str(ds: date) -> str:
    return ds.isoformat()


def _rename(src: Path, dst: Path, attempts: int = 5) -> None:
    """Directory rename with a short retry. On Windows hosts a virus scanner can hold a
    just-written file for a moment (PermissionError); on Linux this never loops."""
    for attempt in range(attempts):
        try:
            src.rename(dst)
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.2 * (attempt + 1))
        else:
            return


class Storage:
    """Builds partition paths and performs atomic file writes."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    # ---- paths ---------------------------------------------------------------------------
    def bronze_ds(self, ds: date) -> Path:
        return self.data_dir / "bronze" / f"ds={ds_str(ds)}"

    def bronze_repo(self, ds: date, repo: str) -> Path:
        return self.bronze_ds(ds) / f"repo={repo}"

    def silver_ds(self, ds: date) -> Path:
        return self.data_dir / "silver" / f"ds={ds_str(ds)}"

    def gold_ds(self, ds: date) -> Path:
        return self.data_dir / "gold" / f"ds={ds_str(ds)}"

    def quarantine_ds(self, ds: date) -> Path:
        return self.data_dir / "quarantine" / f"ds={ds_str(ds)}"

    # ---- writes --------------------------------------------------------------------------
    @staticmethod
    def write_json(path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(path)
        return path

    @staticmethod
    def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        tmp.replace(path)
        return path

    @staticmethod
    def write_parquet(path: Path, df: pd.DataFrame, schema: pa.Schema | None = None) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(table, tmp, compression="snappy")  # type: ignore[no-untyped-call]
        tmp.replace(path)
        return path

    @staticmethod
    def read_json(path: Path) -> Any:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def replace_dir(staging: Path, final: Path) -> Path:
        """Swap a fully written staging directory into place (idempotent rerun)."""
        final.parent.mkdir(parents=True, exist_ok=True)
        backup = final.with_name(final.name + ".old")
        if backup.exists():
            shutil.rmtree(backup)
        if final.exists():
            _rename(final, backup)
        _rename(staging, final)
        if backup.exists():
            shutil.rmtree(backup)
        return final

    @staticmethod
    def reset_dir(path: Path) -> Path:
        """Delete and recreate a partition directory (silver/gold/quarantine reruns)."""
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)
        return path
