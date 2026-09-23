import asyncio
import json
from abc import abstractmethod

import requests
import websockets

from base.Parser import BaseParser



# написать общий класс для всез бирж
class ParserSubscribe(BaseParser):
    def __init__(self, url_wss, exchange_name, url_get_pairs, ping_interval, ping_format):
        super().__init__(url_wss, exchange_name)
        self.url_get_pairs = url_get_pairs
        self.ping_interval = ping_interval
        self.ping_format = ping_format
        self.symbols = self.get_market_symbols()


    @abstractmethod
    # преобразует json-ответ от биржи в ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ETHBTC']
    def serialize_pairs(self, pairs:dict):
        pass


    @abstractmethod
    async def subscribe_to_symbols(self, ws):
        pass


    def get_market_symbols(self):
        # return ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ETHBTC' ...]
        res = requests.get(self.url_get_pairs)
        symbols = self.serialize_pairs(res.json())
        return symbols

    
    async def send_ping(self, ws):
        while True:
            await asyncio.sleep(self.ping_interval)
            await ws.send(self.ping_format)


    async def start(self):
        async with websockets.connect(self.url_wss) as ws:
            await self.subscribe_to_symbols(ws)  

            # Запускаем ping в фоновом режиме
            asyncio.create_task(self.send_ping(ws))

            while True:  
                try:
                    res = await ws.recv()
                    change_ticker = json.loads(res)
                    await self.process_data(change_ticker)
                except websockets.exceptions.ConnectionClosedError:
                    print('соединение прервано')
                    break