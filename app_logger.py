import logging

# создаем форматтер
_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"


def get_file_handler():
    """
    создаем файловый обработчик, который регистрирует отладочные сообщения
    """
    file_handler = logging.FileHandler('errors.log')
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler():
    """
    создаем консольный обработчик с более высоким уровнем журнала
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler


def get_logger(name):
    """
    Создаем регистратор и добавляем настроенные обработчики в логгер.
    Изменение logging с .INFO на .DEBUG включит все логи на экран
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler())
    logger.addHandler(get_stream_handler())
    return logger
