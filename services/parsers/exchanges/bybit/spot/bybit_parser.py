import asyncio
import json
import logging

from base.Parser_subscribe import ParserSubscribe
from bybit_shared.exceptions import *



class BybitParser(ParserSubscribe):
    """Парсер биржи Bybit, наследуемый от Parser_subscribe

    Подписывается на торговые пары, форматирует данные, полученные от биржи, в вид:
        'exchange': имя биржи,
        'symbol': название символа,
        'lastprice': последняя цена символа,
        'volume': объем торгов за 24 часа,
        'timestamp': время обновления данных (в секундах)
    Отправляет их в redis 
    """

    def serialize_pairs(self, pairs: dict) -> list[str]:
        """
        Преобразует json-ответ от биржи в список тикеров
        return ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ETHBTC']
        """
        return [symbol['symbol'] for symbol in pairs['result']['list']]


    async def subscribe_to_symbols(self, ws, batch_size:int=10) -> None:
        """
        Отправляет подписки на каждый тикер через ws-соединение
        """
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i : i + batch_size]
            payload = {
                "op": "subscribe",
                "args": [f"tickers.{s}" for s in batch],
            }

            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.02)
        
        logging.info('Подписки отправлены')

    
    async def process_data(self, data: dict) -> None:
        """
        Форматирует ответ от биржи и отправляет полученные данные в redis
        """
        topic = data.get('topic', '')
        if topic.startswith('tickers.'):
            ticker_data = data.get('data', {})
            normalized_msg = {
                'exchange': self.exchange_name,
                'symbol': ticker_data.get('symbol'),
                'lastprice': ticker_data.get('lastPrice'),
                'volume': ticker_data.get('volume24h'),
                'timestamp': data.get('ts')
            }
            # print(normalized_msg)
            # await redis_client.publish("market_data", json.dumps(normalized_msg))