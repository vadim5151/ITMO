import asyncio
import json
import logging

from base.Parser_subscribe import ParserSubscribe
from base.exceptions import InvalidDataError
from bitget_shared.exceptions import SystemError, ServiceReturnError



class BitgetParser(ParserSubscribe):
    """Парсер биржи Bitget, наследуемый от Parser_subscribe

    Подписывается на торговые пары, форматирует данные, полученные от биржи, в вид:
        'exchange': имя биржи,
        'symbol': название символа,
        'lastprice': последняя цена символа,
        'volume': объем торгов за 24 часа,
        'timestamp': время обновления данных (в секундах)
    Отправляет их в redis 
    """
    def serialize_pairs(self, pairs: dict) -> None:
        """
        Преобразует json-ответ от биржи в список тикеров
        return ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ETHBTC']
        """
        current_tokens = [symbol['symbol'] for symbol in pairs.get('data', [])]
        return current_tokens

       
    async def first_subscribe_to_symbols(self, ws, batch_size=10) -> None:
        """
        Отправляет подписки на каждый тикер через ws-соединение батчами при первом запуске
        """
        symbols_keys = list(self.symbols.keys())
        for i in range(0, len(symbols_keys), batch_size):
            batch = symbols_keys[i : i + batch_size]
            payload = {
                "op":'subscribe',
                "args": [
                    {
                        "instType": "SPOT",
                        "channel": "ticker",
                        "instId": symbol,
                    } for symbol in batch
                ]
            }

            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.02)
        logging.info('первичные подписки отправлены')


    async def subscribe_updating(self, ws) -> None:
        """
        Актуализация данных по подписке 
        """
        while True:
            try:
                for symbol in self.symbols:
                    op = None
                    if self.symbols[symbol]['trading'] and not self.symbols[symbol]['sub']:
                        op = 'subscribe'   
                    elif not self.symbols[symbol]['trading'] and self.symbols[symbol]['sub']:
                        op = 'unsubscribe'

                    if op:
                        payload = {
                            "op": op,
                            "args":[
                                {
                                    "instType": "SPOT",
                                    "channel": "ticker",
                                    "instId": symbol
                                }    
                            ]
                        }
                        await ws.send(json.dumps(payload))
                await asyncio.sleep(300)
            except Exception as e:
                logging.error(f'Ошибка в subscribe_to_symbols {e}')
            
    
    async def process_data(self, data: dict) -> None:
        """
        Форматирует ответ от биржи и отправляет полученные данные в redis
        """
        event = data.get('event')
        code = str(data.get('code', ''))

        if event and event == 'error':
            if code == '25000':
                raise SystemError
            elif code == '40725':
                raise ServiceReturnError 
            
        if event in ('subscribe', 'unsubscribe'):
            await self.process_sub(data)
        else:
            await self.process_ticker(data)

    
    async def process_sub(self, data: dict) -> None:
        """
        обрабатывает ответы на подписку/отписку
        """
        event = data.get('event')
        arg = data.get("arg", {})
        inst_id = arg.get("instId")
        
        if not inst_id or inst_id not in self.symbols:
            return

        if event == "subscribe":
            self.symbols[inst_id]['sub'] = True
            self.symbols[inst_id]['trading'] = True
        elif event == 'unsubscribe':
            self.symbols[inst_id]['sub'] = False
            self.symbols[inst_id]['trading'] = False

    async def process_ticker(self, data: dict) -> None:
        """
        Обрабатывает ответ на тикеры
        """
        ticker_list = data.get("data")
        if not ticker_list or not isinstance(ticker_list, list):
            raise InvalidDataError
        
        ticker_data = ticker_list[0]

        normalized_msg = {
            'exchange': self.exchange_name,
            'symbol': ticker_data.get("instId"),
            'lastprice': ticker_data.get('lastPr'),
            'volume': ticker_data.get('quoteVolume'),
            'timestamp': data.get('ts')
        }
        print(normalized_msg)
