"""Ingestão Jolpica (Ergast-compatible) → bronze parquet."""

from __future__ import annotations

from typing import Any

import pandas as pd

from f1_pipeline.ingest.http_client import RateLimitedClient
from f1_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def list_season_rounds(client: RateLimitedClient, season: int) -> list[int]:
    """Lista rounds disponíveis para a temporada."""
    payload = client.get_json(f"/{season}.json")
    races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    rounds: list[int] = []
    for race in races:
        try:
            rounds.append(int(race["round"]))
        except (KeyError, TypeError, ValueError):
            continue
    return sorted(rounds)


def fetch_race_results(client: RateLimitedClient, season: int, round_n: int) -> pd.DataFrame:
    """Resultados oficiais da corrida (Jolpica)."""
    payload = client.get_json(f"/{season}/{round_n}/results.json")
    races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    rows: list[dict[str, Any]] = []
    for race in races:
        circuit = race.get("Circuit", {})
        for result in race.get("Results", []):
            driver = result.get("Driver", {})
            constructor = result.get("Constructor", {})
            rows.append(
                {
                    "season": int(race.get("season", season)),
                    "round": int(race.get("round", round_n)),
                    "raceName": race.get("raceName"),
                    "circuitId": circuit.get("circuitId"),
                    "circuitName": circuit.get("circuitName"),
                    "date": race.get("date"),
                    "driverId": driver.get("driverId"),
                    "code": driver.get("code"),
                    "givenName": driver.get("givenName"),
                    "familyName": driver.get("familyName"),
                    "constructorId": constructor.get("constructorId"),
                    "constructorName": constructor.get("name"),
                    "grid": _to_int(result.get("grid")),
                    "position": _to_int(result.get("position")),
                    "positionText": result.get("positionText"),
                    "points": _to_float(result.get("points")),
                    "status": result.get("status"),
                    "laps": _to_int(result.get("laps")),
                }
            )
    df = pd.DataFrame(rows)
    logger.info("Jolpica results season=%s round=%s rows=%s", season, round_n, len(df))
    return df


def fetch_driver_standings(client: RateLimitedClient, season: int, round_n: int) -> pd.DataFrame:
    """Classificação de pilotos após o round."""
    payload = client.get_json(f"/{season}/{round_n}/driverStandings.json")
    lists = payload.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    rows: list[dict[str, Any]] = []
    for standing_list in lists:
        for entry in standing_list.get("DriverStandings", []):
            driver = entry.get("Driver", {})
            constructors = entry.get("Constructors", [])
            constructor_id = constructors[0].get("constructorId") if constructors else None
            rows.append(
                {
                    "season": int(standing_list.get("season", season)),
                    "round": int(standing_list.get("round", round_n)),
                    "driverId": driver.get("driverId"),
                    "code": driver.get("code"),
                    "givenName": driver.get("givenName"),
                    "familyName": driver.get("familyName"),
                    "constructorId": constructor_id,
                    "position": _to_int(entry.get("position")),
                    "points": _to_float(entry.get("points")),
                    "wins": _to_int(entry.get("wins")),
                }
            )
    df = pd.DataFrame(rows)
    logger.info("Jolpica driver_standings season=%s round=%s rows=%s", season, round_n, len(df))
    return df


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
