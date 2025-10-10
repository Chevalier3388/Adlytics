# ingestion/base_client.py

from __future__ import annotations

import asyncio
import aiohttp
import backoff
import logging

from abc import ABC, abstractmethod
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class BaseClient(ABC):
    """
    Базовый асинхронный клиент для API-источников.

    Отвечает за:
      - управление сессией aiohttp;
      - базовую авторизацию (токен);
      - ограничение частоты запросов (через aiolimiter);
      - настройку backoff-ретраев в потомках.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        max_rate: int = 5,
        rate_period: int = 1,
        max_retries: int = 3,
        timeout: int = 30,
        backoff_max_tries: int | None = None,
        limiter: AsyncLimiter | None = None,
    ) -> None:
        """
        Параметры:
        - base_url: базовый URL API (без завершающего '/')
        - token: строка авторизации (Bearer token)
        - headers: дополнительные HTTP-заголовки
        - max_rate: лимит количества запросов (по умолчанию 5)
        - rate_period: интервал (в секундах), на который распространяется лимит
        - max_retries: число попыток при ошибках
        - timeout: таймаут HTTP-запросов
        - backoff_max_tries: ограничение числа ретраев при backoff
        - limiter: внешний AsyncLimiter (опционально)
        """
        self.base_url: str = base_url.rstrip("/")
        self.token: str | None = token
        self.headers: dict[str, str] = headers.copy() if headers else {}

        if token:
            self.headers.setdefault("Authorization", f"Bearer {token}")

        self.max_retries: int = max_retries
        self.timeout: int = timeout
        self.backoff_max_tries: int = backoff_max_tries or max_retries

        # aiolimiter — ограничение скорости
        self.limiter: AsyncLimiter = limiter or AsyncLimiter(max_rate, rate_period)

        self._session: aiohttp.ClientSession | None = None

        logger.debug(
            "BaseClient initialized: %s (timeout=%ss, rate=%d/%ds)",
            self.base_url,
            self.timeout,
            max_rate,
            rate_period,
        )

    async def __aenter__(self) -> BaseClient:
        """Асинхронный вход в контекстный менеджер — создаёт aiohttp-сессию."""
        await self._ensure_session()
        logger.debug("Aiohttp session entered for %s", self.base_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Асинхронный выход из контекста — безопасно закрывает aiohttp-сессию."""
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

