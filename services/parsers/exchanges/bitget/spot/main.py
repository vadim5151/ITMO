import asyncio
import traceback
import logging

from bitget_parser import BitgetParser
from configs.logging_config import setup_logging




async def main():
    setup_logging()

    try:
        bitget_parser = BitgetParser(
            url_wss="wss://ws.bitget.com/v2/ws/public",
            exchange_name= 'bitget',
            url_get_pairs="https://api.bitget.com/api/v2/spot/public/symbols",
            ping_interval=20,
            ping_format='ping',
        )
        
    except:
        logging.error(
            msg=f'Не удалось инициализировать объект класса Bitget_Parser'
        )

    await bitget_parser.start()

    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info(msg='Работа парсера остановлена пользователем')
    except:
        logging.error(msg=f'Непредвиденная ошибка')


