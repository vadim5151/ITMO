import json
import asyncio
import traceback
import logging

from bybit_parser import BybitParser
from bybit_shared import *
from configs.logging_config import setup_logging



async def main():
    setup_logging()

    try:
        bybit_parser = BybitParser(
            url_wss="wss://stream.bybit.com/v5/public/spot",
            exchange_name= 'bybit',
            url_get_pairs="https://api.bybit.com/v5/market/instruments-info?category=spot",
            ping_interval=20,
            ping_format=json.dumps({"op": "ping", "req_id": "100001"}),
        )
    except:
        logging.error(
            msg=f'Не удалось инициализировать объект класса Bybit_Parser {traceback.format_exc()}'
        )

    await bybit_parser.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info(msg='Работа парсера остановлена пользователем')
    except:
        logging.error(msg=f'Непредвиденная ошибка {traceback.format_exc()}')





    