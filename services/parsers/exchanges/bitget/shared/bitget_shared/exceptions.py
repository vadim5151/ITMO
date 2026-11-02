class SystemError(Exception):
    """code: 25000 Системная ошибка со стороны сервера"""
    pass


class ServiceReturnError(Exception):
    """code: 40725 Сервер возвращает ошибку"""
    pass

