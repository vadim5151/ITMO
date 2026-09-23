import json

import websockets

from base.Parser import BaseParser



class Parser24h(BaseParser):
    def __init__(self, url_wss, exchange_name, subscribe_to_all_format={}):
        super().__init__(url_wss, exchange_name)
        self.subscribe_to_all_format = subscribe_to_all_format


    async def start(self):
        try:
            async with websockets.connect(self.url_wss, ping_interval=20) as ws: 
                if self.subscribe_to_all_format:
                    await ws.send(json.dumps(self.subscribe_to_all_format))
                async for message in ws:
                    result = json.loads(message)
                    await self.process_data(result)

        except websockets.exceptions.ConnectionClosedOK:
            print('Соединение потеряно')