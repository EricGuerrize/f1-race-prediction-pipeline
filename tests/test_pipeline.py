"""Testes do IngestPipeline com mocks (sem rede)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from f1_pipeline.cli import app
from f1_pipeline.config import Settings
from f1_pipeline.ingest.http_client import RateLimitedClient
from f1_pipeline.ingest.pipeline import IngestPipeline, copy_sample_to_bronze
from f1_pipeline.warehouse import Warehouse


def test_ingest_round_writes_bronze_and_manifest(
    tmp_settings: Settings,
    jolpica_client: RateLimitedClient,
    fastf1_fetcher: Callable[..., dict[str, pd.DataFrame]],
) -> None:
    with IngestPipeline(
        tmp_settings,
        jolpica_client=jolpica_client,
        fastf1_fetcher=fastf1_fetcher,
    ) as pipeline:
        result = pipeline.run(season=2024, round_n=1)

    assert result.rounds_processed == [1]
    assert result.datasets_written == 7  # 2 jolpica + 5 fastf1
    assert result.rows_written > 0

    assert tmp_settings.bronze_dir is not None
    assert (
        tmp_settings.bronze_dir
        / "jolpica"
        / "results"
        / "season=2024"
        / "round=01"
        / "part-000.parquet"
    ).exists()
    assert (
        tmp_settings.bronze_dir
        / "fastf1"
        / "laps"
        / "season=2024"
        / "round=01"
        / "part-000.parquet"
    ).exists()

    assert tmp_settings.warehouse_path is not None
    with Warehouse(tmp_settings.warehouse_path) as wh:
        assert wh.is_ingested("jolpica", "results", 2024, 1)
        assert wh.is_ingested("fastf1", "weather", 2024, 1)


def test_ingest_is_idempotent(
    tmp_settings: Settings,
    jolpica_client: RateLimitedClient,
    fastf1_fetcher: Callable[..., dict[str, pd.DataFrame]],
) -> None:
    with IngestPipeline(
        tmp_settings,
        jolpica_client=jolpica_client,
        fastf1_fetcher=fastf1_fetcher,
    ) as pipeline:
        first = pipeline.run(season=2024, round_n=1)
        second = pipeline.run(season=2024, round_n=1)

    assert first.rounds_processed == [1]
    assert second.rounds_skipped == [1]
    assert second.rounds_processed == []
    assert second.datasets_written == 0


def test_copy_sample_to_bronze(tmp_settings: Settings) -> None:
    tmp_settings.ensure_dirs()
    assert tmp_settings.sample_dir is not None
    assert tmp_settings.bronze_dir is not None

    sample_file = (
        tmp_settings.sample_dir
        / "jolpica"
        / "results"
        / "season=2024"
        / "round=01"
        / "part-000.parquet"
    )
    sample_file.parent.mkdir(parents=True)
    pd.DataFrame({"season": [2024], "round": [1], "driverId": ["x"]}).to_parquet(
        sample_file, index=False
    )

    copy_sample_to_bronze(tmp_settings.sample_dir, tmp_settings.bronze_dir)
    dest = tmp_settings.bronze_dir / sample_file.relative_to(tmp_settings.sample_dir)
    assert dest.exists()


def test_demo_cli_offline(tmp_path: Path) -> None:
    """Garante que o comando demo funciona só com sample local."""
    data = tmp_path / "data"
    sample = (
        data / "sample" / "jolpica" / "results" / "season=2024" / "round=01" / "part-000.parquet"
    )
    sample.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "season": [2024],
            "round": [1],
            "raceName": ["Test"],
            "circuitId": ["test"],
            "driverId": ["a"],
            "constructorId": ["b"],
            "position": [1],
            "points": [25.0],
            "status": ["Finished"],
        }
    ).to_parquet(sample, index=False)

    runner = CliRunner()
    result = runner.invoke(app, ["demo", "--data-dir", str(data)])
    assert result.exit_code == 0, result.output
    assert (data / "warehouse.duckdb").exists()
