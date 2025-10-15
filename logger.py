import logging
from logging.handlers import RotatingFileHandler


def get_logger() -> logging.Logger:
    """Функция для настройки и получения логгера"""
    # Создаем логгер с именем 'my_app'
    logger = logging.getLogger('my_app')
    # Проверяем, не настроен ли уже логгер (чтобы избежать дублирования обработчиков)

    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)  # Уровень логирования можно настроить на DEBUG для максимального охвата
        # Формат логирования
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Путь к файлу логов
        log_file = 'app.log'
        # Настройка ротации файлов (максимум 1 МБ, до 5 резервных копий)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=1000000,  # Максимальный размер файла 1 МБ
            backupCount=5,     # Хранить до 5 резервных копий
            encoding='utf-8'   # Использовать кодировку UTF-8
        )

        # Устанавливаем формат и уровень для обработчика файлов
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)  # Логировать все сообщения начиная с DEBUG
        # Добавляем обработчик в логгер
        logger.addHandler(file_handler)

    return logger
