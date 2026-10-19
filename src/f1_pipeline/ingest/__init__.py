"""Camada de ingestão (FastF1 + Jolpica → bronze parquet + DuckDB)."""

from f1_pipeline.ingest.pipeline import IngestPipeline, IngestResult

__all__ = ["IngestPipeline", "IngestResult"]
