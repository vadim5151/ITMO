import json
import asyncio
import traceback

from parser import BybitParser



if __name__ == "__main__":
    bybit_parser = BybitParser(
        url_wss="wss://stream.bybit.com/v5/public/spot",
        exchange_name= 'bybit',
        url_get_pairs="https://api.bybit.com/v5/market/instruments-info?category=spot",
        ping_interval=20,
        ping_format=json.dumps({"op": "ping", "req_id": "100001"}),
    )

    try:
        asyncio.run(bybit_parser.start())
    except KeyboardInterrupt:
        print("Работа парсера остановлена пользователем.")
    except Exception as e:
        print(f"Непредвиденная ошибка {traceback.format_exc()}")





    