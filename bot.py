import os
import re

import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

load_dotenv()


TOKEN = os.getenv("TELEGRAM_TOKEN")


# --- обработчик файлов ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Функция для обработки команды /start"""
    await update.message.reply_text("Привет! Отправьте файл с постами для обработки.")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Функция для обработки загрузки файла"""
    document = update.message.document
    # Get the File object from the Document
    file_obj = await document.get_file()
    file_path = os.path.join('data', document.file_name)

    # Скачиваем файл
    await file_obj.download_to_drive(custom_path=file_path)
    await update.message.reply_text(f"Файл {document.file_name} успешно загружен. Обрабатываю...")

    # Обрабатываем файл
    try:
        result = process_file(file_path)
        # Отправляем обработанный файл обратно пользователю 
        result_file_path = 'data/processed_report.xlsx'
        result.to_excel(result_file_path, index=False)

        with open(result_file_path, 'rb') as f:
            await update.message.reply_document(f)
    except Exception as e:
        await update.message.reply_text(f"Ошибка при обработке файла: {e}")


def process_file(file_path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(file_path)
    df_vk = pd.read_excel(xls, sheet_name='vk')
    df_companies = pd.read_excel(xls, sheet_name='для ВПР')

    # Нормализация справочника компаний
    df_companies['Полное имя'] = (
        df_companies['Полное имя'].astype(str).str.lower().str.strip().replace('nan', '')
    )
    df_companies['Also known as (AKA)'] = (
        df_companies['Also known as (AKA)'].astype(str).str.lower().str.strip().replace('nan', '')
    )

    # Быстрый индекс CRM-строк по полному имени
    crm_index = {row['Полное имя']: row for _, row in df_companies.iterrows()}

    # Создаем словарь для быстрого поиска компаний
    company_map = {}
    crm_data_map = {}

    for _, row in df_companies.iterrows():
        company = row['Полное имя']
        if company:
            # Сопоставление компании с её данными
            crm_data_map[company] = row

            # Добавляем основное имя компании
            company_map[company] = company

            # Добавляем альтернативные названия
            akas = str(row['Also known as (AKA)']).split(',')
            for aka in akas:
                aka = aka.strip()
                if aka:
                    company_map[aka] = company

    # Создаем regex-паттерн для поиска всех компаний
    all_keywords = sorted(company_map.keys(), key=len, reverse=True)
    pattern = re.compile("|".join(map(re.escape, all_keywords))) if all_keywords else None

    # Словарь для подсчета упоминаний
    mentions = {}

    for _, row in df_vk.iterrows():
        text = str(row.get('GPT', '')).lower()
        post_link = row.get('Ссылка на оригинальный пост (если репост)', '')
        post_link = post_link if post_link not in (None, '-', '') else row.get('Группа', '')

        found_companies = set()

        if pattern:
            # Находим все упоминания за один проход
            matches = pattern.findall(text)
            for match in matches:
                company = company_map.get(match)
                if company:
                    found_companies.add(company)

        # Обновляем счетчики для найденных компаний
        for company in found_companies:
            if company not in mentions:
                mentions[company] = {
                    'count': 0,
                    'links': set(),
                    'crm_data': crm_data_map.get(company)
                }
            mentions[company]['count'] += 1
            if post_link:
                mentions[company]['links'].add(str(post_link))

    # Сбор итоговой таблицы без .append
    rows = []
    for comp, data in mentions.items():
        crm = data['crm_data']
        rows.append({
            '#': crm['#'] if crm is not None else '',
            'Компания': comp,
            'Количество упоминаний': data['count'],
            'Ссылки на посты': ', '.join(dict.fromkeys(data['links'])),
            'Ответственный Ивенты': crm['Ответственный ДК'] if (crm is not None and 'Ответственный ДК' in crm) else '',
            'Ответственный Медиа': crm['Ответственный Media'] if (crm is not None and 'Ответственный Media' in crm) else '',
            'Работаем ли': 'Да' if crm is not None else 'Нет',
        })

    # Сортируем по количеству упоминаний
    cols = [
        '#', 'Компания', 'Количество упоминаний', 'Ссылки на посты',
        'Ответственный Ивенты', 'Ответственный Медиа', 'Работаем ли'
    ]
    return pd.DataFrame(rows, columns=cols).sort_values(
        by='Количество упоминаний', ascending=False
    ).reset_index(drop=True)


def main():
    application = ApplicationBuilder().token(TOKEN).build()

    # Команды и обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    application.run_polling()


if __name__ == '__main__':
    # Создаем папку для хранения файлов, если ее нет
    if not os.path.exists('data'):
        os.makedirs('data')

    main()
