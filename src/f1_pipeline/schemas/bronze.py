"""Validação Pandera dos datasets bronze."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class FastF1ResultsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    session: Series[str]
    DriverNumber: Series[Any] = pa.Field(nullable=True)
    Abbreviation: Series[str] = pa.Field(nullable=True)
    TeamName: Series[str] = pa.Field(nullable=True)
    Position: Series[Any] = pa.Field(nullable=True)
    Points: Series[Any] = pa.Field(nullable=True)
    Status: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class FastF1LapsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    session: Series[str]
    Driver: Series[str] = pa.Field(nullable=True)
    LapNumber: Series[Any] = pa.Field(nullable=True)
    LapTime: Series[Any] = pa.Field(nullable=True)
    Compound: Series[str] = pa.Field(nullable=True)
    TyreLife: Series[Any] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class FastF1PitStopsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    session: Series[str]
    Driver: Series[str] = pa.Field(nullable=True)
    LapNumber: Series[Any] = pa.Field(nullable=True)
    PitInTime: Series[Any] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class FastF1StintsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    session: Series[str]
    Driver: Series[str] = pa.Field(nullable=True)
    Stint: Series[Any] = pa.Field(nullable=True)
    Compound: Series[str] = pa.Field(nullable=True)
    LapStart: Series[Any] = pa.Field(nullable=True)
    LapEnd: Series[Any] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class FastF1WeatherSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    session: Series[str]
    AirTemp: Series[Any] = pa.Field(nullable=True)
    TrackTemp: Series[Any] = pa.Field(nullable=True)
    Humidity: Series[Any] = pa.Field(nullable=True)
    Rainfall: Series[Any] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class JolpicaResultsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    raceName: Series[str]
    circuitId: Series[str]
    driverId: Series[str]
    constructorId: Series[str]
    position: Series[Any] = pa.Field(nullable=True)
    points: Series[Any] = pa.Field(nullable=True)
    status: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


class JolpicaDriverStandingsSchema(pa.DataFrameModel):
    season: Series[int] = pa.Field(ge=1950, le=2100)
    round: Series[int] = pa.Field(ge=1, le=30)
    driverId: Series[str]
    position: Series[Any] = pa.Field(nullable=True)
    points: Series[Any] = pa.Field(nullable=True)
    wins: Series[Any] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


SCHEMA_REGISTRY: dict[str, type[pa.DataFrameModel]] = {
    "fastf1_results": FastF1ResultsSchema,
    "fastf1_laps": FastF1LapsSchema,
    "fastf1_pit_stops": FastF1PitStopsSchema,
    "fastf1_stints": FastF1StintsSchema,
    "fastf1_weather": FastF1WeatherSchema,
    "jolpica_results": JolpicaResultsSchema,
    "jolpica_driver_standings": JolpicaDriverStandingsSchema,
}


def validate_bronze_frame(dataset: str, df: pd.DataFrame) -> pd.DataFrame:
    """Valida um DataFrame bronze; falha com SchemaError se inválido."""
    schema = SCHEMA_REGISTRY.get(dataset)
    if schema is None:
        raise ValueError(f"Dataset bronze desconhecido: {dataset}")
    if df.empty:
        return df
    return schema.validate(df, lazy=True)
