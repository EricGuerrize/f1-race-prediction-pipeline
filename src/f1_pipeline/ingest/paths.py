"""Helpers de path hive-style para bronze."""

from __future__ import annotations

from pathlib import Path


def bronze_partition_dir(
    bronze_root: Path,
    source: str,
    dataset: str,
    season: int,
    round_n: int,
) -> Path:
    """data/bronze/<source>/<dataset>/season=YYYY/round=NN/"""
    return Path(bronze_root) / source / dataset / f"season={season}" / f"round={round_n:02d}"


def bronze_parquet_path(
    bronze_root: Path,
    source: str,
    dataset: str,
    season: int,
    round_n: int,
    filename: str = "part-000.parquet",
) -> Path:
    return bronze_partition_dir(bronze_root, source, dataset, season, round_n) / filename


def relative_to_data(path: Path, data_dir: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(Path(data_dir).resolve()))
    except ValueError:
        return str(path)
