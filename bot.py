import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
import os
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

    # Подсчёт упоминаний
    company_mentions = {}
    for _, row in df_vk.iterrows():
        text = str(row['GPT']).lower()
        post_link = row['Ссылка на оригинальный пост (если репост)'] \
            if row.get('Ссылка на оригинальный пост (если репост)') not in (None, '-', '') \
            else row.get('Группа', '')

        for _, crow in df_companies.iterrows():
            comp = crow['Полное имя']
            aka = crow['Also known as (AKA)']
            if (comp and comp in text) or (aka and aka in text):
                key = comp or aka
                entry = company_mentions.setdefault(
                    key,
                    {'count': 0, 'links': [], 'crm_data': crm_index.get(comp)}
                )
                entry['count'] += 1
                if post_link:
                    entry['links'].append(str(post_link))

    # Сбор итоговой таблицы без .append
    rows = []
    for comp, data in company_mentions.items():
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
