"""Cliente HTTP com rate-limit e retry educado."""

from __future__ import annotations

import threading
import time
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from f1_pipeline.logging_config import get_logger

logger = get_logger(__name__)


class RateLimitedClient:
    """Cliente httpx com intervalo mínimo entre requests e retries."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float,
        max_retries: int,
        min_interval: float,
        user_agent: str,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._min_interval = min_interval
        self._max_retries = max_retries
        self._lock = threading.Lock()
        self._last_request_at = 0.0
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            transport=transport,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RateLimitedClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = self._min_interval - (now - self._last_request_at)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_request_at = time.monotonic()

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET JSON com retry exponencial em erros transitórios e 429/5xx."""

        @retry(
            reraise=True,
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(max=12.0),
            retry=retry_if_exception_type(
                (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError)
            ),
        )
        def _do() -> dict[str, Any]:
            self._throttle()
            url = path if path.startswith("/") else f"/{path}"
            logger.debug("GET %s params=%s", url, params)
            response = self._client.get(url, params=params)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "5"))
                logger.warning("Rate limited (429); aguardando %.1fs", retry_after)
                time.sleep(retry_after)
                response.raise_for_status()
            if response.status_code >= 500:
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Resposta JSON inesperada em {url}")
            return payload

        return _do()
