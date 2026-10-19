"""Testes Jolpica com MockTransport (sem rede)."""

from __future__ import annotations

from f1_pipeline.ingest import jolpica
from f1_pipeline.ingest.http_client import RateLimitedClient


def test_list_season_rounds(jolpica_client: RateLimitedClient) -> None:
    rounds = jolpica.list_season_rounds(jolpica_client, 2024)
    assert rounds == [1, 2]


def test_fetch_race_results(jolpica_client: RateLimitedClient) -> None:
    df = jolpica.fetch_race_results(jolpica_client, 2024, 1)
    assert len(df) == 2
    assert set(df["driverId"]) == {"max_verstappen", "perez"}
    assert df.loc[df["position"] == 1, "points"].iloc[0] == 25.0


def test_fetch_driver_standings(jolpica_client: RateLimitedClient) -> None:
    df = jolpica.fetch_driver_standings(jolpica_client, 2024, 1)
    assert len(df) == 1
    assert df.iloc[0]["driverId"] == "max_verstappen"
