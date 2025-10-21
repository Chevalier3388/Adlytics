from telegram import Bot, InputFile
from telegram.error import TelegramError
from notifications_service.base_sender import BaseSender
from .bot_factory import get_bot
import logging

logger = logging.getLogger("telegram_sender")


class TelegramSender(BaseSender):
    def __init__(self):
        self.bot: Bot = get_bot()
        logger.info("TelegramSender: отправитель создан")

        # Словарь для вызова нужного метода по типу сообщения
        self._send_methods = {
            "text": self._send_text,
            "photo": self._send_photo,
            "document": self._send_document
        }

    async def _send_text(self, chat_id, content):
        await self.bot.send_message(chat_id=chat_id, text=content)

    async def _send_photo(self, chat_id, content):
        await self.bot.send_photo(chat_id=chat_id, photo=InputFile(content))

    async def _send_document(self, chat_id, content):
        await self.bot.send_document(chat_id=chat_id, document=InputFile(content))

    async def _send(self, message: dict):
        chat_id = message.get("to")
        content = message.get("content")
        msg_type = message.get("type", "text")

        if not chat_id or not content:
            logger.error("TelegramSender: сообщение некорректное, отсутствует 'to' или 'content'")
            return False

        send_method = self._send_methods.get(msg_type)
        if not send_method:
            logger.error(f"TelegramSender: неизвестный тип сообщения '{msg_type}'")
            return False

        try:
            await send_method(chat_id, content)
            logger.info(f"TelegramSender: сообщение успешно отправлено, chat_id={chat_id}, тип={msg_type}")
            return True
        except TelegramError as e:
            logger.error(f"TelegramSender: ошибка при отправке сообщения Telegram: {e}")
            return False
