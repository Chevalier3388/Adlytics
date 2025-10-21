# notifications_service/tg_sender/bot_factory.py

from telegram import Bot
from infrastructure_layer.settings import settings

_bot: Bot | None = None

def get_bot() -> Bot:
    """
    Проверяет наличие бота и создаёт его если отсутствует.
    :return: Экземпляр Telegram-бота (Singleton).
    """
    global _bot
    if _bot is None:
        _bot = Bot(token=settings.TELEGRAM_BOT_TOKEN.get_secret_value())

    return _bot
