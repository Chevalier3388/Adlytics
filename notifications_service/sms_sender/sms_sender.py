import logging
import httpx
from notifications_service.base_sender import BaseSender
from infrastructure_layer.settings import settings

logger = logging.getLogger("sms_sender")


class SmsSender(BaseSender):
    """Класс отправки SMS."""

    async def _send(self, data: dict):
        to = data.get("to")
        content = data.get("content")

        if not to or not content:
            logger.error("SmsSender: отсутствует 'to' или 'content'")
            return False

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    settings.SMS_PROVIDER_URL,
                    headers={
                        "Authorization": f"Bearer {settings.SMS_API_TOKEN.get_secret_value()}",
                        "Accept": "application/json",
                    },
                    json={
                        "to": to,
                        "from": settings.SMS_SENDER_ID,
                        "text": content,
                    },
                )
            logger.info(f"SmsSender: отправлено на {to}, статус={response.status_code}")
            return response.status_code in (200, 201)
        except Exception as e:
            logger.error(f"SmsSender: ошибка при отправке — {e}")
            return False
