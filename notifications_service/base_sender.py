from abc import ABC, abstractmethod

class BaseSender(ABC):
    """Базовый класс для всех каналов отправки уведомлений."""

    @abstractmethod
    async def _send(self, message: dict):
        """
        Абстрактный метод: дочерние классы реализуют конкретный способ отправки.
        message: {"to": ..., "content": ...}
        """
        pass

