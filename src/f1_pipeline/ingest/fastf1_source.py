"""Ingestão FastF1 (sessões, laps, pits, stints, weather) → bronze."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.logging_config import get_logger

logger = get_logger(__name__)

# Sessões relevantes para features de corrida / qualificação
DEFAULT_SESSIONS = ("R", "Q")


def enable_fastf1_cache(cache_dir: Path) -> None:
    """Configura cache em disco do FastF1."""
    import fastf1

    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(cache_dir))


def fetch_session_bundle(
    season: int,
    round_n: int,
    *,
    sessions: tuple[str, ...] = DEFAULT_SESSIONS,
    cache_dir: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Baixa datasets FastF1 para um weekend.

    Retorna dict dataset -> DataFrame (já com season/round/session).
    Em ambientes sem acesso ao livetiming, retorna frames vazios em vez de crashar.
    """
    import fastf1
    from fastf1.exceptions import DataNotLoadedError

    if cache_dir is not None:
        enable_fastf1_cache(cache_dir)

    buckets: dict[str, list[pd.DataFrame]] = {
        "results": [],
        "laps": [],
        "pit_stops": [],
        "stints": [],
        "weather": [],
    }

    for session_code in sessions:
        try:
            session = fastf1.get_session(season, round_n, session_code)
            session.load(laps=True, telemetry=False, weather=True, messages=False)
        except Exception as exc:  # noqa: BLE001 — FastF1 levanta tipos variados
            logger.warning(
                "FastF1 session indisponível season=%s round=%s session=%s: %s",
                season,
                round_n,
                session_code,
                exc,
            )
            continue

        meta = {"season": season, "round": round_n, "session": session_code}

        try:
            results = _normalize_results(session.results, meta)
            if not results.empty:
                buckets["results"].append(results)
        except DataNotLoadedError:
            logger.warning(
                "FastF1 results não carregados season=%s round=%s session=%s",
                season,
                round_n,
                session_code,
            )

        try:
            laps = _normalize_laps(session.laps, meta)
            if not laps.empty:
                buckets["laps"].append(laps)
                pits = _extract_pit_stops(laps, meta)
                if not pits.empty:
                    buckets["pit_stops"].append(pits)
                stints = _extract_stints(laps, meta)
                if not stints.empty:
                    buckets["stints"].append(stints)
        except DataNotLoadedError:
            logger.warning(
                "FastF1 laps não carregados season=%s round=%s session=%s",
                season,
                round_n,
                session_code,
            )

        try:
            weather = _normalize_weather(session.weather_data, meta)
            if not weather.empty:
                buckets["weather"].append(weather)
        except DataNotLoadedError:
            logger.warning(
                "FastF1 weather não carregado season=%s round=%s session=%s",
                season,
                round_n,
                session_code,
            )

    out: dict[str, pd.DataFrame] = {}
    for name, frames in buckets.items():
        out[name] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        logger.info(
            "FastF1 %s season=%s round=%s rows=%s",
            name,
            season,
            round_n,
            len(out[name]),
        )
    return out


def _with_meta(df: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for key, value in meta.items():
        out[key] = value
    return out


def _normalize_results(results: Any, meta: dict[str, Any]) -> pd.DataFrame:
    if results is None:
        return pd.DataFrame()
    df = results.copy() if hasattr(results, "copy") else pd.DataFrame(results)
    if df.empty:
        return df
    keep = [
        c
        for c in [
            "DriverNumber",
            "Abbreviation",
            "FullName",
            "TeamName",
            "Position",
            "ClassifiedPosition",
            "GridPosition",
            "Points",
            "Status",
            "Time",
        ]
        if c in df.columns
    ]
    return _with_meta(df[keep], meta)


def _normalize_laps(laps: Any, meta: dict[str, Any]) -> pd.DataFrame:
    if laps is None:
        return pd.DataFrame()
    df = laps.copy() if hasattr(laps, "copy") else pd.DataFrame(laps)
    if df.empty:
        return df
    drop_cols = [c for c in df.columns if str(c).startswith("Sector") and "Session" in str(c)]
    df = df.drop(columns=drop_cols, errors="ignore")
    for col in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[col]) or str(df[col].dtype).startswith("timedelta"):
            df[col] = df[col].astype("string")
    keep_priority = [
        "Driver",
        "DriverNumber",
        "LapNumber",
        "LapTime",
        "Sector1Time",
        "Sector2Time",
        "Sector3Time",
        "Compound",
        "TyreLife",
        "Stint",
        "PitOutTime",
        "PitInTime",
        "TrackStatus",
        "IsPersonalBest",
        "Position",
        "Team",
    ]
    keep = [c for c in keep_priority if c in df.columns]
    for col in df.columns:
        if col not in keep and col not in meta and df[col].dtype != "object" and len(keep) < 40:
            keep.append(col)
    return _with_meta(df[keep], meta)


def _extract_pit_stops(laps: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    if laps.empty or "PitInTime" not in laps.columns:
        return pd.DataFrame()
    pits = laps[laps["PitInTime"].notna()].copy()
    if pits.empty:
        return pd.DataFrame()
    keep = [
        c
        for c in ["Driver", "DriverNumber", "LapNumber", "PitInTime", "PitOutTime", "Stint"]
        if c in pits.columns
    ]
    return _with_meta(pits[keep], meta)


def _extract_stints(laps: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    needed = {"Driver", "Stint", "Compound", "LapNumber"}
    if laps.empty or not needed.issubset(set(laps.columns)):
        return pd.DataFrame()
    grouped = (
        laps.dropna(subset=["Stint"])
        .groupby(["Driver", "Stint", "Compound"], dropna=False)
        .agg(
            LapStart=("LapNumber", "min"),
            LapEnd=("LapNumber", "max"),
            TyreLifeMax=("TyreLife", "max"),
        )
        .reset_index()
    )
    return _with_meta(grouped, meta)


def _normalize_weather(weather: Any, meta: dict[str, Any]) -> pd.DataFrame:
    if weather is None:
        return pd.DataFrame()
    df = weather.copy() if hasattr(weather, "copy") else pd.DataFrame(weather)
    if df.empty:
        return df
    if len(df) > 60:
        step = max(1, len(df) // 40)
        df = df.iloc[::step].copy()
    keep = [
        c
        for c in [
            "Time",
            "AirTemp",
            "Humidity",
            "Pressure",
            "Rainfall",
            "TrackTemp",
            "WindDirection",
            "WindSpeed",
        ]
        if c in df.columns
    ]
    out = _with_meta(df[keep], meta)
    if "Time" in out.columns and pd.api.types.is_timedelta64_dtype(out["Time"]):
        out["Time"] = out["Time"].astype("string")
    return out
