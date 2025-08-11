import pandas as pd
import zipfile
import shutil
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
import os
from dotenv import load_dotenv

load_dotenv()


TOKEN = os.getenv("TELEGRAM_TOKEN")

# --- настройки/константы вверху файла ---
DATA_DIR = "data"
MAX_BYTES = 19_000_000  # оставляем запас до 20 МБ лимита Bot API
ALLOWED_EXTS = {"xlsx", "zip"}


# --- утилиты ---
def _is_safe_zip_member(name: str) -> bool:
    # защита от zip-slip: запрещаем абсолютные пути и выход наверх
    return not (name.startwith("/") or name.startswith("\\") or ".." in name.replace("\\", "/"))


def unpack_zip_find_excel(zip_path: str, extract_to: str) -> str | None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        safe_members = [m for m in zf.namelist() if _is_safe_zip_member(m)]
        # извлекаем только безопасные элементы
        for m in safe_members:
            zf.extract(m, extract_to)

    # Ищем первый .xlsx
    for root, _, files in os.walk(extract_to):
        for fn in files:
            if fn.lower().endswith(".xlsx"):
                return os.path.join(root, fn)
    return None


# --- обработчик файлов ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Функция для обработки команды /start"""
    await update.message.reply_text("Привет! Отправьте файл с постами для обработки.")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Функция для обработки загрузки файла"""
    document = update.message.document
    # Get the File object from the Document
    file_obj = await document.get_file()
    file_path = os.path.join('data', file_obj.file_name)

    # Скачиваем файл
    await file_obj.download(file_path)
    await update.message.reply_text(f"Файл {file_obj.file_name} успешно загружен. Обрабатываю...")

    # Обрабатываем файл
    try:
        result = process_file(file_path)
        # Отправляем обработанный файл обратно пользователю 
        result_file_path = 'data/processed_report.xlsx'
        result.to_excel(result_file_path, index=False)

        with open(result_file_path, 'rb') as f:
            update.message.reply_document(f)
    except Exception as e:
        update.message.reply_text(f"Ошибка при обработке файла: {e}")
    # doc = update.message.document
    # if not doc:
    #     await update.message.reply_text("Пришлите .xlsx или .zip файлом-документом.")
    #     return

    # # предвалидация размера до лимита Bot API
    # if doc.file_size and doc.file_size > MAX_BYTES:
    #     mb = round(doc.file_size / (1024 * 1024), 1)
    #     await update.message.reply_text(
    #         f"Файл слишком большой ({mb}). Уложитесь до ~19 МБ или пришлите ссылку на файл — добавлю загрузку по URL."
    #     )
    #     return

    # # проверяем расширение
    # ext = (doc.file_name or "").split(".")[-1].lower()
    # if ext not in ALLOWED_EXTS:
    #     await update.message.reply_text("Поддерживаю только .xlsx и .zip. Пришлите один из них.")
    #     return

    # # скачиваем во временную папку
    # os.makedirs(DATA_DIR, exist_ok=True)
    # tg_file = await doc.get_file()
    # src_path = os.path.join(DATA_DIR, doc.file_name)
    # await tg_file.download_to_drive(custom_path=src_path)

    # await update.message.reply_text(f"Файл «{doc.file_name}» получен. Обрабатываю…")

    # # если zip - распаковываем и ищем первый .xlsx
    # excel_path = src_path
    # cleanup_paths = [src_path]
    # extracted_dir = None

    # try:
    #     if ext == "zip":
    #         extracted_dir = os.path.join(DATA_DIR, f"extracted_{doc.file_unique_id}")
    #         os.makedirs(extracted_dir, exist_ok=True)
    #         excel_path = unpack_zip_find_excel(src_path, extracted_dir)
    #         if not excel_path:
    #             await update.message.reply_text("В архиве не найден .xlsx. Добавьте Excel внутрь ZIP и пришлите снова.")
    #             return

    #         cleanup_paths.append(extracted_dir)

    #     # запускаем обработку Excel
    #     result_df = process_file(excel_path)

    #     out_path = os.path.join(DATA_DIR, "processed_report.xlsx")
    #     result_df.to_excel(out_path, index=False)

    #     caption = "Готово ✅"
    #     if ext == "zip":
    #         rel = os.path.relpath(excel_path,start=extracted_dir) if extracted_dir else os.path.basename(excel_path)
    #         caption += f"\nОбработан файл из архива: {rel}"

    #     with open(out_path, "rb") as f:
    #         await update.message.reply_document(f, caption=caption)

    # except Exception as e:
    #     await update.message.reply_text(f"Ошибка при обработке: {e}")
    # finally:
    #     # необязательно: подчистим распаковку
    #     try:
    #         for p in cleanup_paths:
    #             if os.path.isdir(p):
    #                 shutil.rmtree(p, ignore_errors=True)
    #             elif os.path.isfile(p):
    #                 os.remove(p)
    #     except Exception:
    #         pass


def process_file(file_path: str) -> pd.DataFrame:
    """Функция для обработки выгрузки данных и создания отчета"""
    # Загружаем данные из файла
    xls = pd.ExcelFile(file_path)
    df_vk = pd.read_excel(xls, sheet_name='vk')
    df_companies = pd.read_excel(xls, sheet_name='для ВПР')

    # Приводим все компании и упоминания к нижнему регистру
    df_companies['Полное имя'] = df_companies['Полное имя'].str.lower().fillna('')
    df_companies['Also known as (AKA)'] = df_companies['Also known as (AKA)'].str.lower().fillna('')

    # Извлекаем упомянутые компании из столбца "GPT"
    company_mentions = {}

    for idx, row in df_vk.iterrows():
        text = row['GPT'].lower()
        post_link = row['Ссылка на оригинальный пост (если репост)'] if row['Ссылка на оригинальный пост (если репост)'] != '-' else row['Группа']

        for company, aka in df_companies[['Полное имя', 'Also known as (AKA)']].values:
            company = company.lower()
            aka = aka.lower()

            if company in text or aka in text:
                if company not in company_mentions:
                    company_mentions[company] = {
                        'count': 0,
                        'links': [],
                        'crm_data': df_companies[df_companies['Полное имя'] == company].iloc[0] if company in df_companies['Полное имя'].values else None
                    }

                company_mentions[company]['count'] += 1
                company_mentions[company]['links'].append(post_link)

    # Составляем итоговый отчёт
    final_report = pd.DataFrame(columns=['Компания', 'Количество упоминаний', 'Ссылки на посты', 'Ответственный Ивенты', 'Ответственный Медиа', 'Работаем ли'])

    for company, data in company_mentions.items():
        crm_data = data['crm_data']
        final_report = final_report.append({
            'Компания': company,
            'Количество упоминаний': data['count'],
            'Ссылки на посты': ', '.join(data['links']),
            'Ответственный Ивенты': crm_data['Ответственный ДК'] if crm_data is not None else '',
            'Ответственный Медиа': crm_data['Ответственный Media'] if crm_data is not None else '',
            'Работаем ли': 'Да' if crm_data is not None else 'Нет'
        }, ignore_index=True)

    return final_report


def main():
    # bot = Bot(token=TELEGRAM_TOKEN)
    # botQueue = Queue()
    # # Создаём Updater и получаем диспетчер
    # updater = Updater(bot, botQueue)

    # # Получаем диспетчер для обработки сообщений
    # dispatcher = updater.dispatcher

    application = ApplicationBuilder().token(TOKEN).build()

    # Команды и обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    application.run_polling()
    # Запускаем бота
    # updater.start_polling()
    # updater.idle()


if __name__ == '__main__':
    # Создаем папку для хранения файлов, если ее нет
    if not os.path.exists('data'):
        os.makedirs('data')

    main()
