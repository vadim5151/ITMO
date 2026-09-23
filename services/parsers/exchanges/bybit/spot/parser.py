import asyncio
import json

from base.Parser_subscribe import ParserSubscribe
import redis.asyncio as aioredis



# redis_client = aioredis.Redis(host='localhost', port=6379, decode_responses=True)

class BybitParser(ParserSubscribe):
    def serialize_pairs(self, pairs:dict):
        return [symbol['symbol'] for symbol in pairs['result']['list']]


    async def subscribe_to_symbols(self, ws, batch_size:int=10):
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i : i + batch_size]
            payload = {
                "op": "subscribe",
                "args": [f"tickers.{s}" for s in batch],
            }

            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.02)
        print('Подписки отправлены')

    
    async def process_data(self, data):
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
            print(normalized_msg)
            # await redis_client.publish("market_data", json.dumps(normalized_msg))