from abc import ABC, abstractmethod



class BaseParser(ABC):
    def __init__(self, url_wss, exchange_name):
        self.url_wss = url_wss
        self.exchange_name = exchange_name


    @abstractmethod
    async def process_data(self, data):
        pass


    @abstractmethod
    async def start(self):
        pass