"""CLI Typer: `f1p ingest`, `f1p demo`, `f1p status`."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from f1_pipeline import __version__
from f1_pipeline.config import get_settings
from f1_pipeline.ingest.pipeline import IngestPipeline, copy_sample_to_bronze
from f1_pipeline.logging_config import setup_logging
from f1_pipeline.warehouse import Warehouse

app = typer.Typer(
    name="f1p",
    help="Pipeline F1 — ingestão bronze (FastF1 + Jolpica) → DuckDB.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()


@app.callback()
def main(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Log DEBUG")] = False,
) -> None:
    setup_logging("DEBUG" if verbose else "INFO")


@app.command("version")
def version_cmd() -> None:
    """Mostra a versão do pacote."""
    console.print(f"f1-pipeline {__version__}")


@app.command("ingest")
def ingest_cmd(
    season: Annotated[int, typer.Option("--season", help="Temporada (ex.: 2024)")],
    round_n: Annotated[
        int | None,
        typer.Option("--round", help="Round específico (omitir = temporada inteira)"),
    ] = None,
    force: Annotated[
        bool, typer.Option("--force", help="Reingere mesmo se já estiver no manifest")
    ] = False,
    data_dir: Annotated[
        Path | None,
        typer.Option("--data-dir", help="Override do diretório data/"),
    ] = None,
) -> None:
    """Ingere FastF1 + Jolpica para bronze parquet e registra no DuckDB."""
    overrides: dict[str, object] = {}
    if data_dir is not None:
        overrides["data_dir"] = data_dir
        overrides["bronze_dir"] = data_dir / "bronze"
        overrides["warehouse_path"] = data_dir / "warehouse.duckdb"
        overrides["fastf1_cache_dir"] = data_dir / "cache" / "fastf1"
        overrides["sample_dir"] = data_dir / "sample"

    settings = get_settings(**overrides)
    with IngestPipeline(settings, force=force) as pipeline:
        result = pipeline.run(season=season, round_n=round_n)

    console.print(
        f"[green]OK[/green] season={result.season} "
        f"processed={result.rounds_processed} skipped={result.rounds_skipped} "
        f"datasets={result.datasets_written} rows={result.rows_written}"
    )


@app.command("demo")
def demo_cmd(
    data_dir: Annotated[
        Path | None,
        typer.Option("--data-dir", help="Override do diretório data/"),
    ] = None,
) -> None:
    """
    Demo offline: copia `data/sample` → bronze e registra views no DuckDB.

    Não acessa rede — adequado para CI e primeiro contato local.
    """
    overrides: dict[str, object] = {}
    if data_dir is not None:
        overrides["data_dir"] = data_dir
        overrides["bronze_dir"] = data_dir / "bronze"
        overrides["warehouse_path"] = data_dir / "warehouse.duckdb"
        overrides["sample_dir"] = data_dir / "sample"

    settings = get_settings(**overrides)
    settings.ensure_dirs()
    assert settings.sample_dir is not None
    assert settings.bronze_dir is not None
    assert settings.warehouse_path is not None

    copy_sample_to_bronze(settings.sample_dir, settings.bronze_dir)

    with Warehouse(settings.warehouse_path) as wh:
        # Registra manifest a partir dos arquivos sample
        for parquet in sorted(settings.bronze_dir.rglob("*.parquet")):
            parts = parquet.parts
            # .../bronze/<source>/<dataset>/season=YYYY/round=NN/file.parquet
            try:
                bronze_idx = parts.index("bronze")
            except ValueError:
                continue
            source = parts[bronze_idx + 1]
            dataset = parts[bronze_idx + 2]
            season_part = parts[bronze_idx + 3]
            round_part = parts[bronze_idx + 4]
            season = int(season_part.split("=")[1])
            round_n = int(round_part.split("=")[1])
            import pandas as pd

            df = pd.read_parquet(parquet)
            rel = str(parquet.relative_to(settings.data_dir))
            wh.record_ingestion(
                source=source,
                dataset=dataset,
                season=season,
                round_n=round_n,
                relative_path=rel,
                row_count=len(df),
            )
        wh.refresh_bronze_views(settings.bronze_dir)
        manifest = wh.load_manifest()

    console.print(
        f"[green]Demo OK[/green] — {len(manifest)} partições registradas em "
        f"{settings.warehouse_path}"
    )
    if not manifest.empty:
        table = Table(title="Manifest (amostra)")
        for col in ["source", "dataset", "season", "round", "row_count"]:
            table.add_column(col)
        for _, row in manifest.head(12).iterrows():
            table.add_row(
                str(row["source"]),
                str(row["dataset"]),
                str(row["season"]),
                str(row["round"]),
                str(row["row_count"]),
            )
        console.print(table)


@app.command("status")
def status_cmd(
    data_dir: Annotated[
        Path | None,
        typer.Option("--data-dir", help="Override do diretório data/"),
    ] = None,
) -> None:
    """Mostra o manifesto de ingestão no DuckDB."""
    overrides: dict[str, object] = {}
    if data_dir is not None:
        overrides["warehouse_path"] = data_dir / "warehouse.duckdb"

    settings = get_settings(**overrides)
    assert settings.warehouse_path is not None
    if not settings.warehouse_path.exists():
        console.print(
            "[yellow]Warehouse ainda não existe. Rode `make demo` ou `f1p ingest`.[/yellow]"
        )
        raise typer.Exit(code=1)

    with Warehouse(settings.warehouse_path) as wh:
        manifest = wh.load_manifest()

    if manifest.empty:
        console.print("Manifest vazio.")
        return

    table = Table(title="Ingestion manifest")
    for col in manifest.columns:
        table.add_column(str(col))
    for _, row in manifest.iterrows():
        table.add_row(*[str(row[c]) for c in manifest.columns])
    console.print(table)


if __name__ == "__main__":
    app()
