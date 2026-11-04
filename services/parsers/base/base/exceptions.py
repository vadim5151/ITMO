class SendPingError(Exception):
    """Ошибка при отправке ping"""
    pass


class RequestError(Exception):
    """Ошибка соединения на уровне TCP/IP или DNS"""
    pass 


class ConnectionClosedError(Exception):
    """Соединение было закрыто не корректно"""
    pass


class WebSocketTimeoutError(Exception):
    """Превышено время ожидания при чтении кадра recv()"""
    pass


class InvalidDataError(Exception):
    """Биржа прислала некорректный ответ"""
    pass
