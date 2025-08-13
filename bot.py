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


# ---------- helpers ----------
def _norm(s: str) -> str:
    if pd.isna(s): return ""
    s = str(s).replace("ё", "е").strip().lower()
    s = re.sub(r"\s+", " ", s).strip("«»\"'()[]")
    return s


def _split_gpt(cell: str) -> list[str]:
    if pd.isna(cell): return []
    text = str(cell)
    rhs = text.split(":", 1)[1] if ":" in text else text
    parts = re.split(r"[;,•/|—\-–\n\.]", rhs)
    out = []
    for p in parts:
        p = _norm(p)
        if p:
            out.append(p)
    return out


def _build_index(df_vpr: pd.DataFrame):
    alias2canon = {}
    canon2crm = {}
    for _, row in df_vpr.iterrows():
        full = _norm(row.get("Полное имя", ""))
        if not full:
            continue
        canon2crm[full] = row.to_dict()
        alias2canon[full] = full
        aka = row.get("Also known as (AKA)", "")
        if not pd.isna(aka):
            for t in str(aka).split(","):
                a = _norm(t)
                if a:
                    alias2canon[a] = full

    keep2 = {k for k in canon2crm.keys() if len(k) == 2}
    drop = set()
    for a in list(alias2canon.keys()):
        if a in {",", ".", "-", "–", "—", "/", "|"}: drop.add(a)
        if a in {"vk", "вк"}: drop.add(a)
        if len(a) <= 2 and a not in keep2: drop.add(a)
    for a in drop: alias2canon.pop(a, None)
    return alias2canon, canon2crm


_GENERIC_STOP = {
    "стажировка", "вакансия", "практика", "кафедра", "факультет", "центр", "департамент", "управление",
    "гк", "ооо", "зао", "пао", "ао", "ao", "pjsc", "llc", "inc", "corp", "co", "gmbh",
    "компания", "университет", "институт", "колледж", "академия", "лаборатория", "школа", "обучение",
    "работа", "карьера", "команда", "проект", "приглашает", "ищет", "набор"
}


def _is_valid_free_token(tok: str) -> bool:
    if not tok:
        return False
    if tok in {"vk", "вк", "vk.com"}:
        return False
    if tok.isdigit():
        return False
    if len(tok) <= 2:
        return False
    if tok in _GENERIC_STOP:
        return False
    tok2 = re.sub(r"\b(ооо|ао|пао|зао|ao|pjsc|llc|inc|co|corp|gmbh)\b\.?", "", tok).strip()
    return bool(tok2)


def process_file(file_path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(file_path)
    df_vk = pd.read_excel(xls, sheet_name="vk")
    df_comp = pd.read_excel(xls, sheet_name="для ВПР")

    alias2canon, canon2crm = _build_index(df_comp)
    canon_names = set(canon2crm.keys())

    mentions = {}
    for _, row in df_vk.iterrows():
        post_link = row.get("Пост")
        if pd.isna(post_link) or not str(post_link).strip():
            post_link = row.get("Группа", "")

        candidates = _split_gpt(row.get("GPT", ""))

        found = set()
        for c in candidates:
            if c in alias2canon:
                found.add(alias2canon[c]); continue
            c2 = re.sub(r"\b(ооо|ао|пао|зао|ao|pjsc|llc|inc|co|corp|gmbh)\b\.?", "", c).strip()
            if c2 in alias2canon:
                found.add(alias2canon[c2]); continue
            # Новое: добавляем "свободную" компанию, если токен выглядит валидным
            if _is_valid_free_token(c):
                found.add(c)

        for comp in found:
            crm = canon2crm.get(comp) if comp in canon_names else None
            if comp not in mentions:
                mentions[comp] = {"count": 0, "links": [], "crm": crm}
            mentions[comp]["count"] += 1
            if post_link and str(post_link) not in mentions[comp]["links"]:
                mentions[comp]["links"].append(str(post_link))

    rows = []
    for comp, data in mentions.items():
        crm = data["crm"]
        rows.append({
            "#": crm.get("#") if crm else "",
            "Компания": comp,
            "Количество упоминаний": data["count"],
            "Ссылки на посты": ", ".join(data["links"]),
            "Ответственный Ивенты": crm.get("Ответственный ДК") if crm else "",
            "Ответственный Медиа": crm.get("Ответственный Media") if crm else "",
            "Работаем ли": "Да" if comp in canon_names else "Нет",
        })

    cols = ["#", "Компания", "Количество упоминаний", "Ссылки на посты",
            "Ответственный Ивенты", "Ответственный Медиа", "Работаем ли"]
    return pd.DataFrame(rows, columns=cols).sort_values(
        by="Количество упоминаний", ascending=False
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
