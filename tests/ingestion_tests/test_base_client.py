# tests/test_base_client.py

import pytest
import aiohttp
from aioresponses import aioresponses
from ingestion.base_client import ResponseBody
from tests.conftest import DummyClient


@pytest.mark.asyncio
async def test_get_request(client: DummyClient):
    """
    - pytest вызывает тест и видит, что нужен client.
    - Фикстура создаёт DummyClient и открывает сессию.
    - _ensure_session() гарантирует, что сессия aiohttp готова.
    - aioresponses ловит GET-запрос и возвращает заранее заданный мок.
    - _request() использует limiter и сессию, получает ответ.
    - Тест проверяет assert — совпадает ли ответ с ожидаемым.
    - Фикстура закрывает сессию, чтобы ресурсы очистились.

    """
    url = "https://api.test.com/test-endpoint"
    expected = {"message": "ok"}

    with aioresponses() as m:
        m.get(url, payload=expected, status=200)

        response: ResponseBody = await client.get("test-endpoint")
        assert response == expected


@pytest.mark.asyncio
async def test_post_request(client: DummyClient):
    """
    - pytest вызывает тест и видит, что нужен client.
    - Фикстура создаёт DummyClient и открывает сессию.
    - _ensure_session() гарантирует, что сессия aiohttp готова.
    - aioresponses ловит POST-запрос и возвращает заранее заданный мок.
    - _request() использует limiter и сессию, получает ответ.
    - Тест проверяет assert — совпадает ли ответ с ожидаемым.
    - Фикстура закрывает сессию, чтобы ресурсы очистились.
    """
    url = "https://api.test.com/test-endpoint"
    payload = {"field": "value"}
    expected = {"result": "ok"}

    with aioresponses() as m:
        m.post(url, payload=expected, status=200)

        response = await client.post("test-endpoint", json=payload)
        assert response == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_session_state, expect_new_session",
    [
        (None, True),
        ("open", False),
        ("closed", True),
    ],
)
async def test_ensure_session_parametrized(initial_session_state, expect_new_session):
    client = DummyClient("https://api.test.com")

    if initial_session_state == "open":
        client._session = aiohttp.ClientSession()
    elif initial_session_state == "closed":
        client._session = aiohttp.ClientSession()
        await client._session.close()
    else:
        client._session = None

    old_session = client._session
    await client._ensure_session()

    if expect_new_session:

        assert client._session is not None
        assert client._session != old_session or (old_session and old_session.closed)
        assert not client._session.closed
    else:

        assert client._session == old_session
        assert not client._session.closed

    if client._session and not client._session.closed:
        await client._session.close()
