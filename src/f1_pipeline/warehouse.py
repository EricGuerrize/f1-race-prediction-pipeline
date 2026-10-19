"""Registro de datasets bronze no DuckDB (lakehouse local)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from f1_pipeline.logging_config import get_logger

logger = get_logger(__name__)

MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_manifest (
    source VARCHAR NOT NULL,
    dataset VARCHAR NOT NULL,
    season INTEGER NOT NULL,
    round INTEGER NOT NULL,
    relative_path VARCHAR NOT NULL,
    row_count INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source, dataset, season, round)
);
"""


class Warehouse:
    """Wrapper fino sobre DuckDB para registrar parquet bronze."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.path))
        self._bootstrap()

    def _bootstrap(self) -> None:
        self._conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        self._conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self._conn.execute(MANIFEST_DDL)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def is_ingested(self, source: str, dataset: str, season: int, round_n: int) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM meta.ingestion_manifest
            WHERE source = ? AND dataset = ? AND season = ? AND round = ?
            """,
            [source, dataset, season, round_n],
        ).fetchone()
        return row is not None

    def record_ingestion(
        self,
        *,
        source: str,
        dataset: str,
        season: int,
        round_n: int,
        relative_path: str,
        row_count: int,
    ) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO meta.ingestion_manifest
                (source, dataset, season, round, relative_path, row_count, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
            """,
            [source, dataset, season, round_n, relative_path, row_count],
        )

    def register_parquet_view(self, view_name: str, parquet_glob: str) -> None:
        """Cria/substitui view bronze apontando para glob de parquet."""
        safe = view_name.replace("`", "").replace(";", "")
        # DuckDB não aceita prepared params em CREATE VIEW — escapamos o path.
        glob_lit = parquet_glob.replace("'", "''")
        self._conn.execute(
            f"""
            CREATE OR REPLACE VIEW bronze.{safe} AS
            SELECT * FROM read_parquet(
                '{glob_lit}',
                hive_partitioning=true,
                union_by_name=true
            )
            """
        )
        logger.debug("View bronze.%s registrada (%s)", safe, parquet_glob)

    def refresh_bronze_views(self, bronze_dir: Path) -> None:
        """Registra views para cada source/dataset encontrado no bronze."""
        bronze_dir = Path(bronze_dir)
        if not bronze_dir.exists():
            return
        for source_dir in sorted(p for p in bronze_dir.iterdir() if p.is_dir()):
            source = source_dir.name
            for dataset_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
                dataset = dataset_dir.name
                glob = str(dataset_dir / "**" / "*.parquet")
                view = f"{source}_{dataset}"
                try:
                    self.register_parquet_view(view, glob)
                except duckdb.Error as exc:
                    logger.warning("Falha ao registrar view %s: %s", view, exc)

    def load_manifest(self) -> pd.DataFrame:
        return self._conn.execute(
            "SELECT * FROM meta.ingestion_manifest ORDER BY season, round, source, dataset"
        ).fetchdf()
