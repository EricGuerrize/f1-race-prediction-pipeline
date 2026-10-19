"""Orquestração da ingestão bronze (idempotente / incremental)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from f1_pipeline.config import Settings, get_settings
from f1_pipeline.ingest import fastf1_source, jolpica
from f1_pipeline.ingest.http_client import RateLimitedClient
from f1_pipeline.ingest.paths import bronze_parquet_path, relative_to_data
from f1_pipeline.logging_config import get_logger
from f1_pipeline.schemas.bronze import validate_bronze_frame
from f1_pipeline.warehouse import Warehouse

logger = get_logger(__name__)

JOLPICA_DATASETS = ("results", "driver_standings")
FASTF1_DATASETS = ("results", "laps", "pit_stops", "stints", "weather")


@dataclass
class IngestResult:
    season: int
    rounds_requested: list[int]
    rounds_processed: list[int] = field(default_factory=list)
    rounds_skipped: list[int] = field(default_factory=list)
    datasets_written: int = 0
    rows_written: int = 0


class IngestPipeline:
    """Ingere um season/round para bronze + registra no DuckDB."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        jolpica_client: RateLimitedClient | None = None,
        fastf1_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = None,
        force: bool = False,
    ) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_dirs()
        self.force = force
        self._owns_client = jolpica_client is None
        self._client = jolpica_client or RateLimitedClient(
            base_url=self.settings.jolpica_base_url,
            timeout=self.settings.http_timeout_seconds,
            max_retries=self.settings.http_max_retries,
            min_interval=self.settings.http_min_interval_seconds,
            user_agent=self.settings.user_agent,
        )
        self._fastf1_fetcher = fastf1_fetcher or fastf1_source.fetch_session_bundle

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> IngestPipeline:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def resolve_rounds(self, season: int, round_n: int | None) -> list[int]:
        if round_n is not None:
            return [round_n]
        rounds = jolpica.list_season_rounds(self._client, season)
        if not rounds:
            raise ValueError(f"Nenhum round encontrado para season={season}")
        return rounds

    def run(self, season: int, round_n: int | None = None) -> IngestResult:
        rounds = self.resolve_rounds(season, round_n)
        result = IngestResult(season=season, rounds_requested=rounds)

        assert self.settings.warehouse_path is not None
        assert self.settings.bronze_dir is not None

        with Warehouse(self.settings.warehouse_path) as wh:
            for rnd in rounds:
                if self._round_fully_ingested(wh, season, rnd) and not self.force:
                    logger.info("Skip round já ingerido: season=%s round=%s", season, rnd)
                    result.rounds_skipped.append(rnd)
                    continue

                written, rows = self._ingest_round(wh, season, rnd)
                result.datasets_written += written
                result.rows_written += rows
                result.rounds_processed.append(rnd)

            wh.refresh_bronze_views(self.settings.bronze_dir)

        logger.info(
            "Ingestão concluída season=%s processed=%s skipped=%s datasets=%s rows=%s",
            season,
            result.rounds_processed,
            result.rounds_skipped,
            result.datasets_written,
            result.rows_written,
        )
        return result

    def _round_fully_ingested(self, wh: Warehouse, season: int, round_n: int) -> bool:
        for dataset in JOLPICA_DATASETS:
            if not wh.is_ingested("jolpica", dataset, season, round_n):
                return False
        for dataset in FASTF1_DATASETS:
            if not wh.is_ingested("fastf1", dataset, season, round_n):
                return False
        return True

    def _ingest_round(self, wh: Warehouse, season: int, round_n: int) -> tuple[int, int]:
        written = 0
        rows = 0

        # Jolpica
        jolpica_frames = {
            "results": jolpica.fetch_race_results(self._client, season, round_n),
            "driver_standings": jolpica.fetch_driver_standings(self._client, season, round_n),
        }
        for dataset, df in jolpica_frames.items():
            n = self._write_dataset(
                wh,
                source="jolpica",
                dataset=dataset,
                season=season,
                round_n=round_n,
                df=df,
                schema_key=f"jolpica_{dataset}",
            )
            if n >= 0:
                written += 1
                rows += n

        # FastF1
        assert self.settings.fastf1_cache_dir is not None
        fastf1_frames = self._fastf1_fetcher(
            season,
            round_n,
            cache_dir=self.settings.fastf1_cache_dir,
        )
        for dataset, df in fastf1_frames.items():
            n = self._write_dataset(
                wh,
                source="fastf1",
                dataset=dataset,
                season=season,
                round_n=round_n,
                df=df,
                schema_key=f"fastf1_{dataset}",
            )
            if n >= 0:
                written += 1
                rows += n

        return written, rows

    def _write_dataset(
        self,
        wh: Warehouse,
        *,
        source: str,
        dataset: str,
        season: int,
        round_n: int,
        df: pd.DataFrame,
        schema_key: str,
    ) -> int:
        """Escreve parquet + manifest. Retorna row count, ou -1 se skip."""
        if not self.force and wh.is_ingested(source, dataset, season, round_n):
            logger.debug(
                "Skip dataset já no manifest: %s/%s s=%s r=%s",
                source,
                dataset,
                season,
                round_n,
            )
            return -1

        assert self.settings.bronze_dir is not None
        assert self.settings.data_dir is not None

        if df is None:
            df = pd.DataFrame()

        if not df.empty:
            df = validate_bronze_frame(schema_key, df)

        out_path = bronze_parquet_path(self.settings.bronze_dir, source, dataset, season, round_n)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # Mesmo vazio: marca partição (idempotência / descoberta de schema)
        df.to_parquet(out_path, index=False)

        rel = relative_to_data(out_path, self.settings.data_dir)
        wh.record_ingestion(
            source=source,
            dataset=dataset,
            season=season,
            round_n=round_n,
            relative_path=rel,
            row_count=len(df),
        )
        logger.info(
            "Bronze escrito %s/%s season=%s round=%s rows=%s → %s",
            source,
            dataset,
            season,
            round_n,
            len(df),
            out_path,
        )
        return len(df)


def copy_sample_to_bronze(sample_dir: Path, bronze_dir: Path) -> None:
    """Copia árvore sample → bronze (para demo offline)."""
    import shutil

    sample_dir = Path(sample_dir)
    bronze_dir = Path(bronze_dir)
    if not sample_dir.exists():
        raise FileNotFoundError(f"Sample não encontrado: {sample_dir}")

    for src in sample_dir.rglob("*.parquet"):
        rel = src.relative_to(sample_dir)
        dest = bronze_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        logger.debug("Sample copiado %s → %s", src, dest)
