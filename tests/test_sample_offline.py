"""Garante que o sample versionado existe e alimenta o demo sem rede."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from f1_pipeline.cli import app
from f1_pipeline.config import get_settings


def test_sample_has_expected_rounds() -> None:
    settings = get_settings()
    sample = settings.sample_dir
    assert sample is not None
    assert sample.exists(), "data/sample deve estar versionado"

    rounds = sorted({int(p.name.split("=")[1]) for p in sample.rglob("round=*") if p.is_dir()})
    assert rounds == [1, 2, 3]

    jolpica_results = list((sample / "jolpica" / "results").rglob("*.parquet"))
    assert len(jolpica_results) == 3
    df = pd.read_parquet(jolpica_results[0])
    assert "driverId" in df.columns
    assert len(df) >= 19  # Australia 2024 teve 19 classificados


def test_make_demo_equivalent_with_repo_sample(tmp_path: Path) -> None:
    """Copia sample do repo para tmp e roda demo — espelha CI offline."""
    import shutil

    settings = get_settings()
    assert settings.sample_dir is not None
    data = tmp_path / "data"
    shutil.copytree(settings.sample_dir, data / "sample")

    runner = CliRunner()
    result = runner.invoke(app, ["demo", "--data-dir", str(data)])
    assert result.exit_code == 0, result.output
    assert (data / "warehouse.duckdb").exists()
    assert (data / "bronze" / "jolpica" / "results").exists()
