import pytest_asyncio
from ingestion.base_client import BaseClient, ResponseBody

class DummyClient(BaseClient):
    """
    Наследник BaseClient для тестов.
    """

    async def normalize(self, data: ResponseBody) -> ResponseBody:
        return data

@pytest_asyncio.fixture
async def client() -> DummyClient:
    """
    Фикстура создаёт асинхронный клиент и закрывает сессию после теста.
    """
    async with DummyClient("https://api.test.com") as c:
        yield c
