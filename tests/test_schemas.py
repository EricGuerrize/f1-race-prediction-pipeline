"""Testes de validação bronze."""

from __future__ import annotations

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from f1_pipeline.schemas.bronze import validate_bronze_frame


def test_jolpica_results_schema_ok() -> None:
    df = pd.DataFrame(
        [
            {
                "season": 2024,
                "round": 1,
                "raceName": "Bahrain Grand Prix",
                "circuitId": "bahrain",
                "driverId": "max_verstappen",
                "constructorId": "red_bull",
                "position": 1,
                "points": 25.0,
                "status": "Finished",
            }
        ]
    )
    out = validate_bronze_frame("jolpica_results", df)
    assert len(out) == 1


def test_fastf1_laps_schema_ok() -> None:
    df = pd.DataFrame(
        [
            {
                "season": 2024,
                "round": 1,
                "session": "R",
                "Driver": "VER",
                "LapNumber": 1,
                "LapTime": "0 days 00:01:35",
                "Compound": "SOFT",
                "TyreLife": 1,
            }
        ]
    )
    out = validate_bronze_frame("fastf1_laps", df)
    assert out.loc[0, "Driver"] == "VER"


def test_unknown_dataset_raises() -> None:
    with pytest.raises(ValueError, match="desconhecido"):
        validate_bronze_frame("not_a_dataset", pd.DataFrame({"a": [1]}))


def test_invalid_season_fails() -> None:
    df = pd.DataFrame(
        [
            {
                "season": 1800,
                "round": 1,
                "raceName": "X",
                "circuitId": "x",
                "driverId": "a",
                "constructorId": "b",
                "position": 1,
                "points": 0,
                "status": "Finished",
            }
        ]
    )
    with pytest.raises(SchemaErrors):
        validate_bronze_frame("jolpica_results", df)
