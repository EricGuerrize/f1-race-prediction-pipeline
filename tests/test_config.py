"""Testes de configuração."""

from __future__ import annotations

from pathlib import Path

from f1_pipeline import __version__
from f1_pipeline.config import Settings


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_settings_defaults(tmp_path: Path) -> None:
    s = Settings(project_root=tmp_path)
    assert s.data_dir == tmp_path / "data"
    assert s.bronze_dir == tmp_path / "data" / "bronze"
    assert s.jolpica_base_url.startswith("https://api.jolpi.ca")
    s.ensure_dirs()
    assert s.bronze_dir is not None
    assert s.bronze_dir.exists()
