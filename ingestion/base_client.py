# ingestion/base_client.py

from __future__ import annotations

import asyncio
import aiohttp
import logging

from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class BaseClient(ABC):
    """
    Базовый абстрактный клиент для API-источников.
    """

    def __init__(
            self,
            base_url: str,
            *,
            token: str | None = None,
            headers: dict[str, str] | None = None,
            min_interval: float = 0.1,
            max_retries: int = 3,
            timeout: int = 30,
            backoff_max_tries: int | None = None,
    ) -> None:
        """
        Инициализация базового клиента.

        Параметры:
        - base_url: базовый URL API (без завершающего '/')
        - token: значение для Authorization (опционально). Если задан — будет добавлено в headers.
        - headers: дополнительные заголовки (словарь)
        - min_interval: минимальный интервал между запросами (сек) — простая защита от flood
        - max_retries: число попыток при ошибках (логическая верхняя граница)
        - timeout: общий таймаут (сек) для HTTP запросов
        - backoff_max_tries: максимальное число попыток для backoff (если не задано — берётся max_retries)
        """
        self.base_url: str = base_url.rstrip("/")
        self.token: str | None = token
        self.headers: dict[str, str] = headers.copy() if headers else {}

        if token:
            self.headers.setdefault("Authorization", f"Bearer {token}")

        self.min_interval: float = min_interval
        self.max_retries: int = max_retries
        self.timeout: int = timeout
        self.backoff_max_tries: int = (
        backoff_max_tries if backoff_max_tries is not None else max_retries
        )

        self._session: aiohttp.ClientSession | None = None

        self._last_request_ts: float = 0.0
        self._rate_lock: asyncio.Lock = asyncio.Lock()

        self._requests_made: int = 0
        self._errors_count: int = 0

        logger.debug(
            "BaseClient initialized: url=%s | timeout=%s | retries=%s | min_interval=%.2fs",
            self.base_url,
            self.timeout,
            self.max_retries,
            self.min_interval,
        )

    async def __aenter__(self) -> BaseClient:
        """
        Асинхронный вход в контекстный менеджер.
        Открывает aiohttp-сессию через вспомогательный метод _ensure_session().
        """
        await self._ensure_session()
        logger.debug("Aiohttp session entered for %s", self.base_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Асинхронный выход из контекстного менеджера.
        Корректно закрывает сессию aiohttp.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Aiohttp session closed for %s", self.base_url)

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                timeout=timeout
            )
            logger.debug("Created aiohttp ClientSession for %s", self.base_url)
