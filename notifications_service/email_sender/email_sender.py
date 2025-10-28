import logging
from email.message import EmailMessage
from aiosmtplib import SMTP, SMTPException
from notifications_service.base_sender import BaseSender
from infrastructure_layer.settings import settings

logger = logging.getLogger("email_sender")


class EmailSender(BaseSender):
    """
    Класс для отправки сообщений по email.
    """

    def __init__(self):
        logger.info("EmailSender: отправитель создан")

    async def _send(self, message: dict):
        """
        Асинхронная отправка email.
        """
        to_email = message.get("to")
        subject = message.get("subject", "Без темы")
        content = message.get("content")
        msg_type = message.get("type", "plain")

        if not to_email or not content:
            logger.error("EmailSender: некорректное сообщение, отсутствует 'to' или 'content'")
            return False

        email_msg = EmailMessage()
        email_msg["From"] = settings.SMTP_FROM_EMAIL
        email_msg["To"] = to_email
        email_msg["Subject"] = subject
        if msg_type == "html":
            email_msg.add_alternative(content, subtype="html")
        else:
            email_msg.set_content(content)

        try:
            async with SMTP(
                    hostname=settings.SMTP_HOST,
                    port=settings.SMTP_PORT,
                    start_tls=settings.SMTP_USE_TLS
            ) as smtp:
                if settings.SMTP_USER and settings.SMTP_PASSWORD.get_secret_value():
                    await smtp.login(
                        settings.SMTP_USER,
                        settings.SMTP_PASSWORD.get_secret_value()
                    )
                await smtp.send_message(email_msg)

            logger.info(f"EmailSender: письмо успешно отправлено, to={to_email}")
            return True

        except SMTPException as e:
            logger.error(f"EmailSender: ошибка при отправке email: {e}")
            return False