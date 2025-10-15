import os
import re

import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATA_DIRECTORY = 'data'
PROCESSED_FILENAME = 'processed_report.xlsx'

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
    alias_to_canonical = {}
    canonical_to_crm_data = {}

    for _, company_row in companies_dataframe.iterrows():
        canonical_name = normalize_text(company_row.get("Полное имя", ""))
        if not canonical_name:
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

    return mentioned_companies


def process_uploaded_file(file_path: str) -> pd.DataFrame:
    """
    Обрабатывает загруженный файл с данными о постах и компаниях.
    Args: file_path: Путь к загруженному файлу
    Returns: DataFrame с результатами обработки
    """
    excel_data = pd.ExcelFile(file_path)
    posts_dataframe = pd.read_excel(excel_data, sheet_name="vk")
    companies_dataframe = pd.read_excel(excel_data, sheet_name="для ВПР")

    # Строим маппинги компаний
    alias_to_canonical, canonical_to_crm = build_company_mappings(companies_dataframe)
    canonical_company_names = set(canonical_to_crm.keys())

    # Собираем статистику упоминаний
    company_mentions = {}

    for _, post_row in posts_dataframe.iterrows():
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

    return sorted_report.reset_index(drop=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает команду /start"""
    welcome_message = (
        "Привет! Отправьте файл с постами для обработки. "
        "Размер файла не должен превышать 20 МБ"
    )
    await update.message.reply_text(welcome_message)


async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает загруженные файлы"""
    uploaded_file = update.message.document
    file_object = await uploaded_file.get_file()
    file_path = os.path.join(DATA_DIRECTORY, uploaded_file.file_name)

    # Скачиваем файл
    await file_object.download_to_drive(custom_path=file_path)
    await update.message.reply_text(f"Файл {uploaded_file.file_name} успешно загружен. Обрабатываю...")

    try:
        # Обрабатываем файл
        processed_data = process_uploaded_file(file_path)

        # Сохраняем и отправляем результат
        result_path = os.path.join(DATA_DIRECTORY, PROCESSED_FILENAME)
        processed_data.to_excel(result_path, index=False)

        with open(result_path, 'rb') as result_file:
            await update.message.reply_document(result_file)

    except Exception as error:
        await update.message.reply_text(f"Ошибка при обработке файла: {error}")


def setup_bot_handlers(application) -> None:
    """Настраивает обработчики команд и сообщений для бота"""
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))


def main() -> None:
    """Основная функция запуска бота"""
    # Создаем папку для данных если она не существует
    if not os.path.exists(DATA_DIRECTORY):
        os.makedirs(DATA_DIRECTORY)

    # Создаем и настраиваем приложение бота
    bot_application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    setup_bot_handlers(bot_application)

    # Запускаем бота
    bot_application.run_polling()


if __name__ == '__main__':
    main()