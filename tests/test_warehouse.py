"""Testes do warehouse DuckDB."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from f1_pipeline.config import Settings
from f1_pipeline.warehouse import Warehouse


def test_manifest_idempotent(tmp_settings: Settings) -> None:
    tmp_settings.ensure_dirs()
    assert tmp_settings.warehouse_path is not None
    with Warehouse(tmp_settings.warehouse_path) as wh:
        assert not wh.is_ingested("jolpica", "results", 2024, 1)
        wh.record_ingestion(
            source="jolpica",
            dataset="results",
            season=2024,
            round_n=1,
            relative_path="bronze/jolpica/results/season=2024/round=01/part-000.parquet",
            row_count=20,
        )
        assert wh.is_ingested("jolpica", "results", 2024, 1)
        wh.record_ingestion(
            source="jolpica",
            dataset="results",
            season=2024,
            round_n=1,
            relative_path="bronze/jolpica/results/season=2024/round=01/part-000.parquet",
            row_count=22,
        )
        manifest = wh.load_manifest()
        assert len(manifest) == 1
        assert int(manifest.iloc[0]["row_count"]) == 22


def test_register_parquet_view(tmp_settings: Settings, tmp_path: Path) -> None:
    tmp_settings.ensure_dirs()
    assert tmp_settings.bronze_dir is not None
    assert tmp_settings.warehouse_path is not None

    part = (
        tmp_settings.bronze_dir
        / "jolpica"
        / "results"
        / "season=2024"
        / "round=01"
        / "part-000.parquet"
    )
    part.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "season": [2024],
            "round": [1],
            "driverId": ["max_verstappen"],
            "points": [25.0],
        }
    ).to_parquet(part, index=False)

    with Warehouse(tmp_settings.warehouse_path) as wh:
        wh.refresh_bronze_views(tmp_settings.bronze_dir)
        rows = wh.connection.execute("SELECT COUNT(*) FROM bronze.jolpica_results").fetchone()
        assert rows is not None
        assert rows[0] == 1
