import json
import logging

import websockets

from base.Parser import BaseParser



class Parser24h(BaseParser):
    """
    Парсер для бирж, требующих переподключение каждые 24 часа

    Attributes:
        subscribe_to_all_format (list): 
    """
    def __init__(self, url_wss: str, exchange_name: str, subscribe_to_all_format: list={}) -> None:
        super().__init__(url_wss, exchange_name)
        self.subscribe_to_all_format = subscribe_to_all_format


    async def start(self) -> None:
        try:
            async with websockets.connect(self.url_wss, ping_interval=20) as ws: 
                if self.subscribe_to_all_format:
                    await ws.send(json.dumps(self.subscribe_to_all_format))
                async for message in ws:
                    result = json.loads(message)
                    await self.process_data(result)

        except websockets.exceptions.ConnectionClosedOK:
            logging.error('Соединение потеряно')