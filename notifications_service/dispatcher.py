# notifications_service/dispatcher.py
import logging
from notifications_service.tg_sender.telegram_sender import TelegramSender

logger = logging.getLogger("dispatcher")

class NotificationDispatcher:
    """Центральный диспетчер для отправки уведомлений через разные каналы."""

    def __init__(self):
        self.senders = {
            "telegram": TelegramSender(),
            # "email": EmailSender(),
            # "sms": SMSSender(),
        }

    async def send(self, message: dict):
        """
        Унифицированный метод отправки уведомлений.
        """
        channel = message.get("channel")
        sender = self.senders.get(channel)

        if not sender:
            logger.error(f"Dispatcher: канал '{channel}' не поддерживается")
            return False

        logger.info(f"Dispatcher: начинаю отправку через {channel}")
        success = await sender._send(message)
        logger.info(f"Dispatcher: отправка завершена, статус={success}")

        return success
