# ingestion/base_client.py

from __future__ import annotations

import asyncio
import aiohttp
import backoff
import logging

from abc import ABC, abstractmethod
from aiolimiter import AsyncLimiter
from typing import TypeVar, TypeAlias

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

JSONPrimitive: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONPrimitive | list["JSONValue"] | dict[str, "JSONValue"]
JSONType: TypeAlias = dict[str, JSONValue] | list[JSONValue]
ResponseBody: TypeAlias = JSONType | str | bytes

T = TypeVar("T", bound=ResponseBody)


class BaseClient(ABC):
    """
    Базовый асинхронный клиент для API-источников.
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
        """
        Асинхронный вход в контекстный менеджер.
        """
        await self._ensure_session()
        logger.debug("Aiohttp session entered for %s", self.base_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Асинхронный выход из контекста.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Aiohttp session closed for %s", self.base_url)

    async def _ensure_session(self) -> None:
        """
        Гарантирует наличие открытой aiohttp-сессии.
        Создаёт новую сессию, если текущая отсутствует или закрыта.
        """
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
            logger.debug("Created aiohttp ClientSession for %s", self.base_url)

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=lambda self: self.backoff_max_tries,
        jitter=backoff.full_jitter,
        logger=logger,
    )
    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, str] | None = None,
        json: JSONType | None = None,
        data: bytes | str | None = None,
        headers: dict[str, str] | None = None,
    ) -> T:
        """
        Выполняет асинхронный HTTP-запрос с учётом лимитов и повторных попыток.
        """

        await self._ensure_session()

        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        merged_headers = {**self.headers, **(headers or {})}

        async with self.limiter:
            async with self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json,
                data=data,
                headers=merged_headers,
            ) as resp:
                if resp.status >= 400:
                    # читаемый и безопасный вывод ошибки
                    body = await resp.text()
                    logger.warning(
                        "Request %s %s failed (%d): %s", method, url, resp.status, body
                    )
                    resp.raise_for_status()

                try:
                    return await resp.json()
                except aiohttp.ContentTypeError:
                    return await resp.text()

    async def get(
        self,
        endpoint: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ResponseBody:
        """Выполняет GET-запрос к API."""
        return await self._request("GET", endpoint, params=params, headers=headers)

    async def post(
        self,
        endpoint: str,
        *,
        json: JSONType | None = None,
        data: bytes | str | None = None,
        headers: dict[str, str] | None = None,
    ) -> ResponseBody:
        """Выполняет POST-запрос к API."""
        return await self._request(
            "POST", endpoint, json=json, data=data, headers=headers
        )

    @abstractmethod
    async def normalize(self, data: ResponseBody) -> ResponseBody:
        """Нормализует данные ответа API. Реализуется в наследниках."""
        raise NotImplementedError
