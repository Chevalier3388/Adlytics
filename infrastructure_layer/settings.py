# adlytics/infrastructure_layer/settings.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    Глобальные настройки проекта Adlytics.
    Все сервисы используют этот класс для доступа к конфигурации.
    """

    # Telegram
    TELEGRAM_BOT_TOKEN: str

    # Kafka
    KAFKA_BROKER_URL: str
    KAFKA_TOPIC_NOTIFICATIONS: str
    KAFKA_TOPIC_FAILED: str  # топик для неотправленных сообщений

    # Redis
    REDIS_URL: str
    REDIS_CACHE_TTL: int = 300  # по умолчанию 5 минут

    # SQLite
    SQLITE_DB_PATH: str = "data/notifications.db"

    # Логирование
    LOG_LEVEL: str = "INFO"
    LOG_FILE_PATH: str = "logs/notifications.log"


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Создаём единый экземпляр для импорта во все сервисы
settings = Settings()
