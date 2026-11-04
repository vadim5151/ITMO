import asyncio
import json
import logging
from abc import abstractmethod

import requests
import websockets

from base.Parser import BaseParser
from base.exceptions import *



class ParserSubscribe(BaseParser):
    """Парсер для бирж, требующих подписки для получения торговых пар

    Сначала получает список доступных торговых пар, затем подписывается на них 

    Attributes:
        url_get_pairs (str): REST API URL для получения списка всех торговых пар
        ping_interval (int): Интервал между отправками ping-сообщений
        ping_format (any): Формат ping-сообщений для биржи
        symbols (list[str]): Отфармированный список торговых пар (тикеров) биржи
    """
    def __init__(
        self, 
        url_wss: str, 
        exchange_name: str, 
        url_get_pairs: str, 
        ping_interval: int, 
        ping_format: any,
    ) -> None:
        super().__init__(url_wss, exchange_name)
        self.url_get_pairs = url_get_pairs
        self.ping_interval = ping_interval
        self.ping_format = ping_format
        self.symbols = {}


    @abstractmethod
    def serialize_pairs(self, pairs: dict) -> list[str]:
        """
        Преобразует json-ответ от биржи в словарь тикеров
        return {'token': {'trading': True, 'sub': 'True'}}
        """
        pass


    @abstractmethod
    async def subscribe_to_symbols(self, ws) -> None:
        """
        Отправляет подписки на каждый тикер через ws-соединение
        """
        pass


    async def symbols_updating(self):
        """
        Каждые 5 минут вызывает функцию get_market_symbols, для обновления словаря символов
        """
        try:
            while True:
                await asyncio.sleep(300)
                await self.get_market_symbols()
                logging.info('Повторное обновление списка торговых пар')
        except Exception as e:
            logging.error(f'Непредвиденная ошибка: {e}')


    def fetch_market_symbols(self):
        """
        Запрашивает список пар через REST API и вызывает сериализацию
        """
        try:
            res = requests.get(self.url_get_pairs)
            res.raise_for_status()
            current_tokens = self.serialize_pairs(res.json())

            for symbol in current_tokens:
                if symbol not in self.symbols:
                    self.symbols[symbol] = {'trading': True, 'sub': False}
    
            for symbol in self.symbols:
                if symbol not in current_tokens:
                    self.symbols[symbol]['trading'] = False

            if not self.symbols:
                logging.error('Получен пустой список торговых пар')
            else:
                logging.info('Список торговых пар был получен')     
        except RequestError:
            logging.exception('Сетевая ошибка REST API')

    
    async def send_ping(self, ws) -> None:
        """
        Отправляет ping-сообщения с интервалов в ping_interval
        """
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                await ws.send(self.ping_format)
        except SendPingError:
            logging.warning(f'Ошибка при отправке ping-сообщения')


    async def _listen_socket(self, ws) -> None:
        """
        Внутренний цикл для чтения сообщений из WebSocket сокета
        """
        async for message in ws:
            if message == 'pong':
                continue
            
            try:
                data = json.loads(message)
                await self.process_data(data)
            except Exception as e:
                logging.warning(f'Получено не JSON сообщение {e}')


    async def start(self):
        """
        Устанавливает соединение с биржей через WebSocket
        """
        self.fetch_market_symbols()
        while True:
            try:
                logging.info('Подключение к WebSocket')
                async with websockets.connect(self.url_wss) as ws: 
                    logging.info('Соединение установлено')

                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self.subscribe_to_symbols(ws))
                        tg.create_task(self.send_ping(ws))
                        tg.create_task(self.symbols_updating()) 
                        tg.create_task(self._listen_socket(ws)) 

            except WebSocketTimeoutError:
                logging.error(f'Сервер перестал присылать данные, но не закрыл сокет корректно')
            except ConnectionClosedError:
                logging.error(f'соединение прервано')
            except:
                logging.error(f'Непредвиденная ошибка')

