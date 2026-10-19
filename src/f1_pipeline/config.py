"""Configuração central do pipeline (paths, rate limits, cache)."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_project_root() -> Path:
    """Raiz do repositório (dois níveis acima de src/f1_pipeline)."""
    return Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Settings carregáveis via env / .env."""

    model_config = SettingsConfigDict(
        env_prefix="F1P_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    project_root: Path = Field(default_factory=_default_project_root)
    data_dir: Path | None = None
    bronze_dir: Path | None = None
    sample_dir: Path | None = None
    warehouse_path: Path | None = None
    fastf1_cache_dir: Path | None = None

    jolpica_base_url: str = "https://api.jolpi.ca/ergast/f1"
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 4
    http_min_interval_seconds: float = 0.35
    user_agent: str = "f1-race-prediction-pipeline/0.1 (+https://github.com/EricGuerrize/f1-race-prediction-pipeline)"

    def model_post_init(self, __context: object) -> None:
        root = self.project_root
        if self.data_dir is None:
            self.data_dir = root / "data"
        if self.bronze_dir is None:
            self.bronze_dir = self.data_dir / "bronze"
        if self.sample_dir is None:
            self.sample_dir = self.data_dir / "sample"
        if self.warehouse_path is None:
            self.warehouse_path = self.data_dir / "warehouse.duckdb"
        if self.fastf1_cache_dir is None:
            self.fastf1_cache_dir = self.data_dir / "cache" / "fastf1"

    def ensure_dirs(self) -> None:
        """Cria diretórios de dados necessários."""
        assert self.bronze_dir is not None
        assert self.sample_dir is not None
        assert self.fastf1_cache_dir is not None
        assert self.warehouse_path is not None
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.sample_dir.mkdir(parents=True, exist_ok=True)
        self.fastf1_cache_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)


def get_settings(**overrides: object) -> Settings:
    """Factory conveniente para Settings com overrides opcionais."""
    return Settings(**overrides)  # type: ignore[arg-type]
