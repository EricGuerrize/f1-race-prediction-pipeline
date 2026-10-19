#!/usr/bin/env python3
"""
Gera `data/sample/` com 2–3 rounds reais de 2024.

Estratégia:
  1) Jolpica (resultados, standings, laps, pit stops) — sempre via API.
  2) FastF1 — tenta livetiming; se indisponível (ex.: egress), deriva partições
     bronze no schema FastF1 a partir dos timings reais da Jolpica.

Uso:
  uv run python scripts/build_sample.py --rounds 1,2,3
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.config import get_settings
from f1_pipeline.ingest import fastf1_source, jolpica
from f1_pipeline.ingest.http_client import RateLimitedClient
from f1_pipeline.ingest.paths import bronze_parquet_path
from f1_pipeline.logging_config import get_logger, setup_logging
from f1_pipeline.schemas.bronze import validate_bronze_frame

logger = get_logger(__name__)

# Códigos de piloto (Jolpica driverId → abreviação FastF1)
DRIVER_CODES: dict[str, str] = {
    "max_verstappen": "VER",
    "perez": "PER",
    "hamilton": "HAM",
    "russell": "RUS",
    "leclerc": "LEC",
    "sainz": "SAI",
    "norris": "NOR",
    "piastri": "PIA",
    "alonso": "ALO",
    "stroll": "STR",
    "ocon": "OCO",
    "gasly": "GAS",
    "albon": "ALB",
    "sargeant": "SAR",
    "tsunoda": "TSU",
    "ricciardo": "RIC",
    "bottas": "BOT",
    "zhou": "ZHO",
    "hulkenberg": "HUL",
    "magnussen": "MAG",
    "kevin_magnussen": "MAG",
}


def fetch_jolpica_laps(client: RateLimitedClient, season: int, round_n: int) -> pd.DataFrame:
    """Laps paginadas da Jolpica (reais)."""
    rows: list[dict[str, Any]] = []
    limit = 100
    offset = 0
    total = None
    while total is None or offset < total:
        payload = client.get_json(
            f"/{season}/{round_n}/laps.json",
            params={"limit": limit, "offset": offset},
        )
        mr = payload["MRData"]
        total = int(mr["total"])
        races = mr.get("RaceTable", {}).get("Races", [])
        for race in races:
            for lap in race.get("Laps", []):
                lap_no = int(lap["number"])
                for timing in lap.get("Timings", []):
                    rows.append(
                        {
                            "season": season,
                            "round": round_n,
                            "session": "R",
                            "driverId": timing.get("driverId"),
                            "LapNumber": lap_no,
                            "position": _to_int(timing.get("position")),
                            "LapTime": timing.get("time"),
                        }
                    )
        offset += limit
        if not races:
            break
    return pd.DataFrame(rows)


def fetch_jolpica_pitstops(
    client: RateLimitedClient, season: int, round_n: int
) -> pd.DataFrame:
    payload = client.get_json(f"/{season}/{round_n}/pitstops.json", params={"limit": 100})
    races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    rows: list[dict[str, Any]] = []
    for race in races:
        for stop in race.get("PitStops", []):
            rows.append(
                {
                    "season": season,
                    "round": round_n,
                    "session": "R",
                    "driverId": stop.get("driverId"),
                    "LapNumber": _to_int(stop.get("lap")),
                    "stop": _to_int(stop.get("stop")),
                    "time": stop.get("time"),
                    "duration": stop.get("duration"),
                }
            )
    return pd.DataFrame(rows)


def to_fastf1_from_jolpica(
    results: pd.DataFrame,
    laps: pd.DataFrame,
    pits: pd.DataFrame,
    season: int,
    round_n: int,
) -> dict[str, pd.DataFrame]:
    """Mapeia timings reais Jolpica → schemas bronze FastF1 (sample offline)."""
    code_map = {
        row["driverId"]: row.get("code") or DRIVER_CODES.get(row["driverId"], "UNK")
        for _, row in results.iterrows()
    }
    team_map = {row["driverId"]: row.get("constructorName") for _, row in results.iterrows()}

    ff_results = pd.DataFrame(
        [
            {
                "season": season,
                "round": round_n,
                "session": "R",
                "DriverNumber": None,
                "Abbreviation": code_map.get(r["driverId"], DRIVER_CODES.get(r["driverId"])),
                "TeamName": r.get("constructorName"),
                "Position": r.get("position"),
                "Points": r.get("points"),
                "Status": r.get("status"),
                "GridPosition": r.get("grid"),
            }
            for _, r in results.iterrows()
        ]
    )

    # Trim laps: ~10 por piloto
    ff_laps_rows: list[dict[str, Any]] = []
    if not laps.empty:
        for driver_id, group in laps.groupby("driverId"):
            abbr = code_map.get(driver_id, DRIVER_CODES.get(str(driver_id), "UNK"))
            g = group.sort_values("LapNumber")
            if len(g) > 10:
                idx = sorted(
                    set(
                        list(range(0, len(g), max(1, len(g) // 10)))[:10]
                        + [len(g) - 1]
                    )
                )
                g = g.iloc[idx]
            pit_laps = set()
            if not pits.empty:
                pit_laps = set(
                    pits.loc[pits["driverId"] == driver_id, "LapNumber"].dropna().astype(int)
                )
            stint = 1
            tyre_life = 0
            for _, lap in g.iterrows():
                tyre_life += 1
                lap_no = int(lap["LapNumber"])
                ff_laps_rows.append(
                    {
                        "season": season,
                        "round": round_n,
                        "session": "R",
                        "Driver": abbr,
                        "LapNumber": lap_no,
                        "LapTime": lap.get("LapTime"),
                        "Compound": "UNKNOWN",
                        "TyreLife": tyre_life,
                        "Stint": stint,
                        "Position": lap.get("position"),
                        "Team": team_map.get(driver_id),
                        "PitInTime": "PIT" if lap_no in pit_laps else None,
                        "PitOutTime": None,
                    }
                )
                if lap_no in pit_laps:
                    stint += 1
                    tyre_life = 0

    ff_laps = pd.DataFrame(ff_laps_rows)
    ff_pits = pd.DataFrame(
        [
            {
                "season": season,
                "round": round_n,
                "session": "R",
                "Driver": code_map.get(p["driverId"], DRIVER_CODES.get(str(p["driverId"]), "UNK")),
                "LapNumber": p.get("LapNumber"),
                "PitInTime": p.get("time"),
                "PitOutTime": None,
                "Stint": p.get("stop"),
            }
            for _, p in pits.iterrows()
        ]
    )

    ff_stints = pd.DataFrame()
    if not ff_laps.empty:
        ff_stints = (
            ff_laps.groupby(["Driver", "Stint", "Compound"], dropna=False)
            .agg(LapStart=("LapNumber", "min"), LapEnd=("LapNumber", "max"))
            .reset_index()
        )
        ff_stints["season"] = season
        ff_stints["round"] = round_n
        ff_stints["session"] = "R"

    # Weather: poucas linhas sintéticas leves (Jolpica não expõe weather)
    # Marcadas com source_note — schema permite colunas extras (strict=False)
    ff_weather = pd.DataFrame(
        [
            {
                "season": season,
                "round": round_n,
                "session": "R",
                "AirTemp": None,
                "TrackTemp": None,
                "Humidity": None,
                "Rainfall": None,
                "note": "weather unavailable offline; placeholder partition",
            }
        ]
    )

    return {
        "results": ff_results,
        "laps": ff_laps,
        "pit_stops": ff_pits,
        "stints": ff_stints,
        "weather": ff_weather,
    }


def trim_laps(df: pd.DataFrame, max_rows_per_driver: int = 10) -> pd.DataFrame:
    if df.empty or "Driver" not in df.columns:
        return df
    parts: list[pd.DataFrame] = []
    for _, group in df.groupby("Driver", dropna=False):
        if len(group) <= max_rows_per_driver:
            parts.append(group)
        else:
            step = max(1, len(group) // max_rows_per_driver)
            parts.append(group.iloc[::step].head(max_rows_per_driver))
    return pd.concat(parts, ignore_index=True) if parts else df


def write_parquet(
    root: Path,
    source: str,
    dataset: str,
    season: int,
    round_n: int,
    df: pd.DataFrame,
    schema_key: str,
) -> Path:
    if not df.empty:
        df = validate_bronze_frame(schema_key, df)
    path = bronze_parquet_path(root, source, dataset, season, round_n)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression="zstd")
    return path


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument("--rounds", type=str, default="1,2,3")
    parser.add_argument(
        "--try-fastf1",
        action="store_true",
        help="Tenta FastF1 livetiming antes do fallback Jolpica",
    )
    args = parser.parse_args()
    setup_logging("INFO")

    settings = get_settings()
    sample_root = settings.project_root / "data" / "sample"
    if sample_root.exists():
        shutil.rmtree(sample_root)
    sample_root.mkdir(parents=True)

    rounds = [int(x.strip()) for x in args.rounds.split(",") if x.strip()]
    total_bytes = 0
    files: list[str] = []
    used_fastf1 = False

    with RateLimitedClient(
        base_url=settings.jolpica_base_url,
        timeout=settings.http_timeout_seconds,
        max_retries=max(settings.http_max_retries, 6),
        min_interval=max(settings.http_min_interval_seconds, 0.8),
        user_agent=settings.user_agent,
    ) as client:
        for rnd in rounds:
            results = jolpica.fetch_race_results(client, args.season, rnd)
            standings = jolpica.fetch_driver_standings(client, args.season, rnd)
            laps_j = fetch_jolpica_laps(client, args.season, rnd)
            pits_j = fetch_jolpica_pitstops(client, args.season, rnd)

            for dataset, df, key in [
                ("results", results, "jolpica_results"),
                ("driver_standings", standings, "jolpica_driver_standings"),
            ]:
                path = write_parquet(
                    sample_root, "jolpica", dataset, args.season, rnd, df, key
                )
                total_bytes += path.stat().st_size
                files.append(path.relative_to(sample_root).as_posix())
                print(f"wrote {path} rows={len(df)}")

            ff_frames: dict[str, pd.DataFrame] | None = None
            if args.try_fastf1:
                try:
                    assert settings.fastf1_cache_dir is not None
                    ff_frames = fastf1_source.fetch_session_bundle(
                        args.season, rnd, cache_dir=settings.fastf1_cache_dir
                    )
                    if ff_frames.get("laps", pd.DataFrame()).empty:
                        logger.warning(
                            "FastF1 retornou laps vazios; usando fallback Jolpica"
                        )
                        ff_frames = None
                    else:
                        used_fastf1 = True
                except Exception as exc:  # noqa: BLE001
                    logger.warning("FastF1 falhou (%s); fallback Jolpica", exc)
                    ff_frames = None

            if ff_frames is None:
                ff_frames = to_fastf1_from_jolpica(
                    results, laps_j, pits_j, args.season, rnd
                )

            if "laps" in ff_frames:
                ff_frames["laps"] = trim_laps(ff_frames["laps"], 10)

            for dataset, df in ff_frames.items():
                path = write_parquet(
                    sample_root,
                    "fastf1",
                    dataset,
                    args.season,
                    rnd,
                    df,
                    f"fastf1_{dataset}",
                )
                total_bytes += path.stat().st_size
                files.append(path.relative_to(sample_root).as_posix())
                print(f"wrote {path} rows={len(df)}")

    source_note = (
        "FastF1 livetiming (quando disponível)"
        if used_fastf1
        else "Jolpica timings mapeados para schema FastF1 (livetiming indisponível no build)"
    )
    manifest = sample_root / "MANIFEST.md"
    manifest.write_text(
        "# Sample bronze (offline)\n\n"
        f"- Season: {args.season}\n"
        f"- Rounds: {rounds} (Bahrain, Jeddah, Australia — 2024)\n"
        f"- Arquivos: {len(files)}\n"
        f"- Tamanho total: {total_bytes / 1024:.1f} KiB\n"
        f"- FastF1 partitions: {source_note}\n\n"
        "Fontes: [Jolpica](https://api.jolpi.ca/ergast/f1/) (Ergast-compatible) "
        "+ schema FastF1. Laps/pits são **reais** (API Jolpica), trimados "
        "(~10 laps/piloto) para caber no git.\n\n"
        "Uso: portfolio / não-comercial. Não afiliado à FIA/F1.\n\n"
        "## Arquivos\n\n"
        + "\n".join(f"- `{f}`" for f in files)
        + "\n",
        encoding="utf-8",
    )
    print(f"\nSample pronto em {sample_root} ({total_bytes / 1024:.1f} KiB)")


if __name__ == "__main__":
    main()
