"""Fixtures compartilhadas — zero rede no CI."""

from __future__ import annotations

from pathlib import Path

import httpx
import pandas as pd
import pytest

from f1_pipeline.config import Settings
from f1_pipeline.ingest.http_client import RateLimitedClient


@pytest.fixture
def tmp_settings(tmp_path: Path) -> Settings:
    data = tmp_path / "data"
    return Settings(
        project_root=tmp_path,
        data_dir=data,
        bronze_dir=data / "bronze",
        sample_dir=data / "sample",
        warehouse_path=data / "warehouse.duckdb",
        fastf1_cache_dir=data / "cache" / "fastf1",
    )


@pytest.fixture
def mock_jolpica_transport() -> httpx.MockTransport:
    """Respostas Jolpica mínimas para season 2024 rounds 1–2."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/2024.json") or path.endswith("/2024.json/"):
            return httpx.Response(
                200,
                json={
                    "MRData": {
                        "RaceTable": {
                            "Races": [
                                {"round": "1", "raceName": "Bahrain Grand Prix"},
                                {"round": "2", "raceName": "Saudi Arabian Grand Prix"},
                            ]
                        }
                    }
                },
            )
        if "/2024/1/results.json" in path:
            return httpx.Response(
                200, json=_results_payload(2024, 1, "Bahrain Grand Prix", "bahrain")
            )
        if "/2024/2/results.json" in path:
            return httpx.Response(
                200, json=_results_payload(2024, 2, "Saudi Arabian Grand Prix", "jeddah")
            )
        if "/2024/1/driverStandings.json" in path:
            return httpx.Response(200, json=_standings_payload(2024, 1))
        if "/2024/2/driverStandings.json" in path:
            return httpx.Response(200, json=_standings_payload(2024, 2))
        return httpx.Response(404, json={"error": f"unmocked {path}"})

    return httpx.MockTransport(handler)


@pytest.fixture
def jolpica_client(mock_jolpica_transport: httpx.MockTransport) -> RateLimitedClient:
    return RateLimitedClient(
        base_url="https://api.jolpi.ca/ergast/f1",
        timeout=5.0,
        max_retries=2,
        min_interval=0.0,
        user_agent="f1-pipeline-test",
        transport=mock_jolpica_transport,
    )


def _results_payload(season: int, round_n: int, name: str, circuit_id: str) -> dict:
    return {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "season": str(season),
                        "round": str(round_n),
                        "raceName": name,
                        "date": "2024-03-02",
                        "Circuit": {
                            "circuitId": circuit_id,
                            "circuitName": circuit_id.title(),
                        },
                        "Results": [
                            {
                                "grid": "1",
                                "position": "1",
                                "positionText": "1",
                                "points": "25",
                                "status": "Finished",
                                "laps": "57",
                                "Driver": {
                                    "driverId": "max_verstappen",
                                    "code": "VER",
                                    "givenName": "Max",
                                    "familyName": "Verstappen",
                                },
                                "Constructor": {
                                    "constructorId": "red_bull",
                                    "name": "Red Bull",
                                },
                            },
                            {
                                "grid": "2",
                                "position": "2",
                                "positionText": "2",
                                "points": "18",
                                "status": "Finished",
                                "laps": "57",
                                "Driver": {
                                    "driverId": "perez",
                                    "code": "PER",
                                    "givenName": "Sergio",
                                    "familyName": "Perez",
                                },
                                "Constructor": {
                                    "constructorId": "red_bull",
                                    "name": "Red Bull",
                                },
                            },
                        ],
                    }
                ]
            }
        }
    }


def _standings_payload(season: int, round_n: int) -> dict:
    return {
        "MRData": {
            "StandingsTable": {
                "StandingsLists": [
                    {
                        "season": str(season),
                        "round": str(round_n),
                        "DriverStandings": [
                            {
                                "position": "1",
                                "points": str(25 * round_n),
                                "wins": str(round_n),
                                "Driver": {
                                    "driverId": "max_verstappen",
                                    "code": "VER",
                                    "givenName": "Max",
                                    "familyName": "Verstappen",
                                },
                                "Constructors": [{"constructorId": "red_bull"}],
                            }
                        ],
                    }
                ]
            }
        }
    }


def fake_fastf1_bundle(season: int, round_n: int, **_: object) -> dict[str, pd.DataFrame]:
    """FastF1 fake para testes — sem rede / sem pacote pesado."""
    meta = {"season": season, "round": round_n, "session": "R"}
    results = pd.DataFrame(
        [
            {
                **meta,
                "DriverNumber": "1",
                "Abbreviation": "VER",
                "TeamName": "Red Bull Racing",
                "Position": 1.0,
                "Points": 25.0,
                "Status": "Finished",
            }
        ]
    )
    laps = pd.DataFrame(
        [
            {
                **meta,
                "Driver": "VER",
                "LapNumber": 1,
                "LapTime": "0 days 00:01:35.000000",
                "Compound": "SOFT",
                "TyreLife": 1,
                "Stint": 1,
                "PitInTime": None,
                "PitOutTime": None,
            },
            {
                **meta,
                "Driver": "VER",
                "LapNumber": 10,
                "LapTime": "0 days 00:01:36.100000",
                "Compound": "SOFT",
                "TyreLife": 10,
                "Stint": 1,
                "PitInTime": "0 days 00:25:00",
                "PitOutTime": None,
            },
        ]
    )
    pit_stops = laps[laps["PitInTime"].notna()][
        ["Driver", "LapNumber", "PitInTime", "Stint", "season", "round", "session"]
    ].copy()
    stints = pd.DataFrame(
        [
            {
                **meta,
                "Driver": "VER",
                "Stint": 1,
                "Compound": "SOFT",
                "LapStart": 1,
                "LapEnd": 10,
            }
        ]
    )
    weather = pd.DataFrame(
        [
            {
                **meta,
                "AirTemp": 28.5,
                "TrackTemp": 35.0,
                "Humidity": 40.0,
                "Rainfall": False,
            }
        ]
    )
    return {
        "results": results,
        "laps": laps,
        "pit_stops": pit_stops,
        "stints": stints,
        "weather": weather,
    }


@pytest.fixture
def fastf1_fetcher():
    """Fixture que injeta o fake FastF1 no pipeline."""
    return fake_fastf1_bundle
