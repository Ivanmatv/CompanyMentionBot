import os
import re
import gspread

import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

from logger import get_logger

load_dotenv()

logger = get_logger()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_SHEET_KEY = os.getenv("GOOGLE_SHEET_KEY")
GOOGLE_SHEET = os.getenv("GOOGLE_SHEET")
CREDENTIALS_FILE = "credentials.json"
DATA_DIRECTORY = 'data'

# Списки для фильтрации
GENERIC_STOP_WORDS = {
    "стажировка", "вакансия", "практика", "кафедра", "факультет", "центр", "департамент", "управление",
    "гк", "ооо", "зао", "пао", "ао", "ao", "pjsc", "llc", "inc", "corp", "co", "gmbh",
    "компания", "университет", "институт", "колледж", "академия", "лаборатория", "школа", "обучение",
    "работа", "карьера", "команда", "проект", "приглашает", "ищет", "набор"
}

COMPANY_LEGAL_FORMS = {"ооо", "ао", "пао", "зао", "ao", "pjsc", "llc", "inc", "co", "corp", "gmbh"}


def normalize_text(text: str) -> str:
    """
    Нормализует текст: приводит к нижнему регистру, заменяет ё на е, удаляет лишние пробелы и символы.
    Args: text: Исходный текст
    Returns: Нормализованный текст
    """
    if pd.isna(text):
        return ""

    normalized = (str(text)
                  .replace("ё", "е")
                  .strip()
                  .lower())
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip("«»\"'()[]")

    return normalized


def extract_company_mentions_from_text(text: str) -> list[str]:
    """
    Извлекает упоминания компаний из текста, обработанного GPT.
    Args: text: Текст с упоминаниями компаний
    Returns: Список найденных упоминаний компаний
    """
    if pd.isna(text):
        return []

    # Удаляем префикс перед двоеточием если есть
    text_after_colon = text.split(":", 1)[1] if ":" in text else text

    # Разделяем текст по различным разделителям
    separators = r"[;,•/|—\-–\n\.]"
    parts = re.split(separators, text_after_colon)

    mentions = []
    for part in parts:
        normalized_part = normalize_text(part)
        if normalized_part:
            mentions.append(normalized_part)

    return mentions


def is_valid_company_name(company_name: str) -> bool:
    """
    Проверяет, является ли название компании валидным для обработки.
    Args: company_name: Название компании для проверки
    Returns: True если название валидно, иначе False
    """
    if not company_name:
        return False

    # Исключаем общие стоп-слова
    if company_name in {"vk", "вк", "vk.com"}:
        return False

    # Исключаем чисто числовые значения
    if company_name.isdigit():
        return False
    # Исключаем слишком короткие названия
    if len(company_name) <= 2:
        return False

    # Исключаем общие стоп-слова
    if company_name in GENERIC_STOP_WORDS:
        return False

    # Удаляем юридические формы и проверяем остаток
    legal_forms_pattern = r"\b(" + "|".join(COMPANY_LEGAL_FORMS) + r")\b\.?"
    name_without_legal_form = re.sub(legal_forms_pattern, "", company_name).strip()

    return bool(name_without_legal_form)


def build_company_mappings(companies_dataframe: pd.DataFrame) -> tuple[dict, dict]:
    """
    Создает отображения псевдонимов компаний на канонические названия и канонических названий на данные CRM.
    Args: companies_dataframe: DataFrame с данными о компаниях
    Returns: Кортеж (alias_to_canonical, canonical_to_crm_data)
    """
    logger.info("Начало построения маппингов компаний")
    
    alias_to_canonical = {}
    canonical_to_crm_data = {}

    for index, company_row in companies_dataframe.iterrows():
        canonical_name = normalize_text(company_row.get("Полное имя", ""))
        if not canonical_name:
            logger.debug(f"Пропуск строки {index}: отсутствует каноническое название")
            continue

        # Сохраняем данные CRM для канонического названия
        canonical_to_crm_data[canonical_name] = company_row.to_dict()
        alias_to_canonical[canonical_name] = canonical_name

        # Обрабатываем псевдонимы (Also Known As)
        aka_names = company_row.get("Also known as (AKA)", "")
        if not pd.isna(aka_names):
            for alias in str(aka_names).split(","):
                normalized_alias = normalize_text(alias)
                if normalized_alias:
                    alias_to_canonical[normalized_alias] = canonical_name

    # Очищаем маппинг от невалидных значений
    valid_two_letter_names = {name for name in canonical_to_crm_data.keys() if len(name) == 2}
    invalid_aliases = {",", ".", "-", "–", "—", "/", "|", "vk", "вк"}

    for alias in list(alias_to_canonical.keys()):
        if (alias in invalid_aliases or
            (len(alias) <= 2 and alias not in valid_two_letter_names)):
            alias_to_canonical.pop(alias, None)

    logger.info(f"Построение маппингов завершено: {len(canonical_to_crm_data)} компаний, {len(alias_to_canonical)} алиасов")
    return alias_to_canonical, canonical_to_crm_data


def find_company_mentions_in_post(post_gpt_text: str, alias_to_canonical_mapping: dict) -> set[str]:
    """
    Находит упоминания компаний в тексте поста.
    Args: post_gpt_text: Текст поста, обработанный GPT
        alias_to_canonical_mapping: Маппинг псевдонимов на канонические названия
    Returns: Множество найденных компаний (канонические названия и валидные свободные упоминания)
    """
    mentioned_companies = set()
    extracted_mentions = extract_company_mentions_from_text(post_gpt_text)

    logger.debug(f"Извлечено упоминаний из текста: {len(extracted_mentions)}")

    for mention in extracted_mentions:
        # Проверяем прямое соответствие в маппинге
        if mention in alias_to_canonical_mapping:
            mentioned_companies.add(alias_to_canonical_mapping[mention])
            continue

        # Пробуем удалить юридическую форму и проверить снова
        legal_forms_pattern = r"\b(" + "|".join(COMPANY_LEGAL_FORMS) + r")\b\.?"
        mention_without_legal_form = re.sub(legal_forms_pattern, "", mention).strip()

        if (mention_without_legal_form and 
            mention_without_legal_form in alias_to_canonical_mapping):
            mentioned_companies.add(alias_to_canonical_mapping[mention_without_legal_form])
            continue

        # Добавляем валидные свободные упоминания
        if is_valid_company_name(mention):
            mentioned_companies.add(mention)

    logger.debug(f"Найдено компаний в посте: {len(mentioned_companies)}")
    return mentioned_companies


def process_uploaded_file(file_path: str) -> pd.DataFrame:
    """
    Обрабатывает загруженный файл с данными о постах и компаниях.
    Args: file_path: Путь к загруженному файлу
    Returns: DataFrame с результатами обработки
    """
    logger.info(f"Начало обработки файла: {file_path}")
    
    try:
        excel_data = pd.ExcelFile(file_path)
        posts_dataframe = pd.read_excel(excel_data, sheet_name="vk")
        companies_dataframe = pd.read_excel(excel_data, sheet_name="для ВПР")
        
        logger.info(f"Загружено постов: {len(posts_dataframe)}, компаний: {len(companies_dataframe)}")
    except Exception as e:
        logger.error(f"Ошибка загрузки файла {file_path}: {str(e)}")
        raise

    # Строим маппинги компаний
    alias_to_canonical, canonical_to_crm = build_company_mappings(companies_dataframe)
    canonical_company_names = set(canonical_to_crm.keys())

    # Собираем статистику упоминаний
    company_mentions = {}
    processed_posts = 0

    for index, post_row in posts_dataframe.iterrows():
        # Получаем ссылку на пост
        post_link = post_row.get("Пост")
        if pd.isna(post_link) or not str(post_link).strip():
            post_link = post_row.get("Группа", "")

        # Находим компании, упомянутые в посте
        gpt_text = post_row.get("GPT", "")
        companies_in_post = find_company_mentions_in_post(gpt_text, alias_to_canonical)

        # Обновляем статистику для каждой найденной компании
        for company in companies_in_post:
            crm_data = canonical_to_crm.get(company) if company in canonical_company_names else None

            if company not in company_mentions:
                company_mentions[company] = {
                    "mention_count": 0,
                    "post_links": [],
                    "crm_data": crm_data
                }

            company_mentions[company]["mention_count"] += 1

            if post_link and str(post_link) not in company_mentions[company]["post_links"]:
                company_mentions[company]["post_links"].append(str(post_link))
        
        processed_posts += 1
        if processed_posts % 100 == 0:
            logger.info(f"Обработано постов: {processed_posts}/{len(posts_dataframe)}")

    logger.info(f"Обработка постов завершена. Упоминаний найдено: {len(company_mentions)}")

    # Формируем итоговый отчет
    report_rows = []
    for company, mention_data in company_mentions.items():
        crm_data = mention_data["crm_data"]

        report_rows.append({
            "#": crm_data.get("#") if crm_data else "",
            "Компания": company,
            "Количество упоминаний": mention_data["mention_count"],
            "Ссылки на посты": ", ".join(mention_data["post_links"]),
            "Ответственный Ивенты": crm_data.get("Ответственный ДК") if crm_data else "",
            "Ответственный Медиа": crm_data.get("Ответственный Media") if crm_data else "",
            "Есть в СРМ": "Да" if company in canonical_company_names else "Нет",
        })

    # Определяем колонки для итогового DataFrame
    report_columns = [
        "#", "Компания", "Количество упоминаний", "Ссылки на посты",
        "Ответственный Ивенты", "Ответственный Медиа", "Есть в СРМ"
    ]

    report_dataframe = pd.DataFrame(report_rows, columns=report_columns)
    sorted_report = report_dataframe.sort_values(by="Количество упоминаний", ascending=False)

    logger.info(f"Формирование отчета завершено: {len(report_rows)} записей")
    return sorted_report.reset_index(drop=True)


def get_google_sheet_client():
    """Создает и возвращает клиент для работы с Google Sheets"""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        logger.info("Успешное подключение к Google Sheets")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        raise


def save_to_google_sheets(dataframe: pd.DataFrame, worksheet_name: str = "Обработанные данные") -> str:
    """
    Сохраняет DataFrame в Google Таблицу на указанный лист
    Returns: Ссылка на таблицу
    """
    logger.info(f"Начало сохранения данных в Google Sheets. Записей: {len(dataframe)}")
    
    try:
        client = get_google_sheet_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_KEY)

        try:
            # Пытаемся получить существующий лист
            worksheet = spreadsheet.worksheet(worksheet_name)
            logger.info(f"Лист '{worksheet_name}' найден, очищаем и обновляем данные...")
        except gspread.WorksheetNotFound:
            # Если лист не существует, создаем новый
            logger.info(f"Лист '{worksheet_name}' не найден, создаем новый...")
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

        # Очищаем лист
        worksheet.clear()
        logger.debug("Лист очищен")

        # Подготавливаем данные для загрузки
        data_to_upload = [dataframe.columns.tolist()]  # Заголовки
        data_to_upload.extend(dataframe.fillna('').values.tolist())  # Данные

        # Загружаем все данные одной операцией (более эффективно)
        worksheet.update(data_to_upload, 'A1')
        logger.debug("Данные загружены в таблицу")

        # Форматируем заголовки
        worksheet.format('A1:Z1', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
        })
        logger.debug("Форматирование заголовков применено")

        # Автоматически подбираем ширину колонок
        try:
            worksheet.columns_auto_resize(0, len(dataframe.columns))
            logger.debug("Автоподбор ширины колонок выполнен")
        except Exception as e:
            logger.warning(f"Автоподбор ширины колонок не поддерживается: {str(e)}")

        logger.info(f"Данные успешно загружены на лист '{worksheet_name}'")
        return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_KEY}/edit#gid={worksheet.id}"

    except Exception as e:
        logger.error(f"Ошибка при сохранении в Google Sheets: {str(e)}")
        raise


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает команду /start"""
    logger.info(f"Команда /start от пользователя {update.effective_user.id}")
    welcome_message = (
        "Привет! Отправьте файл с постами для обработки. "
        "Размер файла не должен превышать 20 МБ"
    )
    await update.message.reply_text(welcome_message)


async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает загруженные файлы"""
    user_id = update.effective_user.id
    uploaded_file = update.message.document
    
    logger.info(f"Получен файл от пользователя {user_id}: {uploaded_file.file_name}")
    
    file_object = await uploaded_file.get_file()
    file_path = os.path.join(DATA_DIRECTORY, uploaded_file.file_name)

    try:
        # Скачиваем файл
        await file_object.download_to_drive(custom_path=file_path)
        logger.info(f"Файл сохранен: {file_path}")
        
        await update.message.reply_text(f"Файл {uploaded_file.file_name} успешно загружен. Обрабатываю...")

        # Обрабатываем файл
        processed_data = process_uploaded_file(file_path)

        # Сохраняем в Google Таблицу на лист "Обработанные данные"
        sheet_url = save_to_google_sheets(processed_data, "Обработанные данные")

        # Отправляем подтверждение и ссылку на таблицу
        success_message = (
            f"✅ Обработка завершена!\n"
            f"📊 Данные сохранены в Google Таблицу на лист 'Обработанные данные':\n"
            f"{sheet_url}"
        )

        await update.message.reply_text(success_message)
        logger.info(f"Обработка файла завершена для пользователя {user_id}")

    except Exception as error:
        error_message = f"Ошибка при обработке файла: {str(error)}"
        logger.error(f"Ошибка обработки файла для пользователя {user_id}: {str(error)}", exc_info=True)
        await update.message.reply_text(error_message)


def setup_bot_handlers(application) -> None:
    """Настраивает обработчики команд и сообщений для бота"""
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    logger.info("Обработчики бота настроены")


def main() -> None:
    """Основная функция запуска бота"""
    logger.info("Запуск бота...")
    
    # Создаем папку для данных если она не существует
    if not os.path.exists(DATA_DIRECTORY):
        os.makedirs(DATA_DIRECTORY)
        logger.info(f"Создана директория {DATA_DIRECTORY}")

    # Проверяем наличие необходимых переменных окружения
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Не установлена переменная окружения TELEGRAM_TOKEN")
        raise ValueError("TELEGRAM_TOKEN не установлен")
    
    if not GOOGLE_SHEET_KEY:
        logger.error("Не установлена переменная окружения GOOGLE_SHEET_KEY")
        raise ValueError("GOOGLE_SHEET_KEY не установлен")

    try:
        # Создаем и настраиваем приложение бота
        bot_application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        setup_bot_handlers(bot_application)

        # Запускаем бота
        logger.info("Бот запущен и готов к работе")
        bot_application.run_polling()
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {str(e)}", exc_info=True)
        raise


if __name__ == '__main__':
    main()