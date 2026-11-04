from abc import ABC, abstractmethod



class BaseParser(ABC):
    """Базовый абстрактный класс для всех WebSocket-парсеров бирж

    Attributes:
        url_wss(str): URL-адрес WebSocket биржи
        exchange_name(str): Название биржи (например, 'Binance', 'Bitget')
    """

    def __init__(self, url_wss: str, exchange_name: str) -> None:
        self.url_wss = url_wss
        self.exchange_name = exchange_name


    @abstractmethod
    async def process_data(self, data: dict) -> None:
        """ 
        Абстрактный метод для преобразования ответа от биржи
        """
        pass


    @abstractmethod
    async def start(self) -> None:
        """
        Абстрактный метод для запуска соединения с WebSocket-биржей
        """
        pass