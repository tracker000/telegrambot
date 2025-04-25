"""
GPT-4o destekli Telegram ihale bildirim botu
Tek dosya sürümü — python-telegram-bot 22.x
"""

# ── Standart / harici kütüphaneler ──────────────────────────────────────────────
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import feedparser
import openai
import requests
import sqlite3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# ── .env & sabitler ────────────────────────────────────────────────────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

FEED_URL = "https://www.contractsfinder.service.gov.uk/Published/Notices/Rss"
DB_FILE = "bot.db"
BACKUP_DIR = "backup"
LOG_DIR = "log"
CACHE_DIR = "cache"

os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# ── Yardımcı işlevler ──────────────────────────────────────────────────────────
def format_date(dt: datetime) -> str:
    months_tr = {
        "January": "Ocak",
        "February": "Şubat",
        "March": "Mart",
        "April": "Nisan",
        "May": "Mayıs",
        "June": "Haziran",
        "July": "Temmuz",
        "August": "Ağustos",
        "September": "Eylül",
        "October": "Ekim",
        "November": "Kasım",
        "December": "Aralık",
    }
    return f"{dt.day} {months_tr.get(dt.strftime('%B'), dt.strftime('%B'))} {dt.year}"


# ── Veritabanı katmanı ─────────────────────────────────────────────────────────
def db_conn():
    return sqlite3.connect(DB_FILE)


def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY, joined_at TEXT)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS subs (chat_id INTEGER, keyword TEXT, last_seen TEXT, PRIMARY KEY(chat_id, keyword))"
        )
        conn.commit()


def add_user(chat_id: int):
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (chat_id, joined_at) VALUES (?, ?)",
            (chat_id, datetime.now().isoformat()),
        )


def user_exists(chat_id: int) -> bool:
    with db_conn() as conn:
        cur = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (chat_id,))
        return cur.fetchone() is not None


def add_subscription(chat_id: int, keyword: str) -> bool:
    try:
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO subs (chat_id, keyword, last_seen) VALUES (?, ?, ?)",
                (chat_id, keyword, "1970-01-01T00:00:00"),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def remove_subscription(chat_id: int, keyword: str) -> bool:
    with db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM subs WHERE chat_id=? AND keyword=?", (chat_id, keyword)
        )
        return cur.rowcount > 0


def list_subscriptions(chat_id: int):
    with db_conn() as conn:
        cur = conn.execute("SELECT keyword FROM subs WHERE chat_id=?", (chat_id,))
        return [row[0] for row in cur.fetchall()]


def clear_subscriptions(chat_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM subs WHERE chat_id=?", (chat_id,))


def get_all_subscriptions():
    with db_conn() as conn:
        cur = conn.execute("SELECT chat_id, keyword, last_seen FROM subs")
        return cur.fetchall()


def update_last_seen(chat_id: int, keyword: str, timestamp: str):
    with db_conn() as conn:
        conn.execute(
            "UPDATE subs SET last_seen=? WHERE chat_id=? AND keyword=?",
            (timestamp, chat_id, keyword),
        )


# ── Feed çekme & ayrıştırma ────────────────────────────────────────────────────
def fetch_feed_entries():
    try:
        resp = requests.get(FEED_URL, timeout=10)
        resp.raise_for_status()
    except Exception:
        resp = requests.get(FEED_URL, verify=False, timeout=10)
    feed = feedparser.parse(resp.text)
    entries = []
    for e in feed.entries:
        entry = {
            "id": e.get("id", e.get("link")),
            "title": e.get("title", "").strip(),
            "link": e.get("link"),
            "summary": re.sub("<[^<]+?>", "", e.get("summary", "")),
            "published_parsed": getattr(e, "published_parsed", None),
            "updated_parsed": getattr(e, "updated_parsed", None),
            "closing_datetime": None,
            "budget": None,
            "pdf_link": None,
        }

        soup = BeautifulSoup(e.get("summary", ""), "html.parser")
        text = soup.get_text()

        # Son başvuru tarihi
        m = re.search(r"(\d{1,2} [A-Za-z]+ \d{4}(?: \d{1,2}:\d{2})?)", text)
        if m:
            for fmt in ("%d %B %Y %H:%M", "%d %B %Y"):
                try:
                    entry["closing_datetime"] = datetime.strptime(m.group(1), fmt)
                    break
                except ValueError:
                    continue

        # Bütçe
        m2 = re.search(r"£([0-9,]+)", text)
        if m2:
            entry["budget"] = m2.group(1).replace(",", "")

        # PDF link
        for l in e.get("links", []):
            if l.get("type") == "application/pdf":
                entry["pdf_link"] = l.get("href")
                break
        if not entry["pdf_link"]:
            a = soup.find("a", href=True, text=re.compile(r"PDF", re.I))
            if a:
                entry["pdf_link"] = a["href"]

        entries.append(entry)
    return entries


# ── GPT-4o özetleme (cache’li) ────────────────────────────────────────────────
def get_summary(text: str) -> str:
    content = BeautifulSoup(text, "html.parser").get_text()[:1000]
    cache_id = str(abs(hash(content)))
    cache_file = os.path.join(CACHE_DIR, cache_id + ".txt")
    if os.path.exists(cache_file):
        return open(cache_file, "r", encoding="utf-8").read()

    prompt = (
        "Aşağıdaki kamu ihalesini teknik jargon kullanmadan, kritik bilgileri vurgulayarak "
        "2–3 cümleyle özetle:\n"
        + content
    )
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "İhale özetleri yazan asistan."},
                {"role": "user", "content": prompt},
            ],
        )
        summary = resp.choices[0].message.content.strip()
    except Exception:
        summary = content[:120] + "…"

    with open(cache_file, "w", encoding="utf-8") as f:
        f.write(summary)
    return summary


# ── Mesaj biçimlendirme ────────────────────────────────────────────────────────
def build_message(entry: dict, summary: str, keyword: str, updated: bool = False):
    title = entry["title"] + (" (Güncellendi)" if updated else "")
    lines = [
        f"📝 *{title}*",
        f"🧾 {summary}",
    ]

    if entry["published_parsed"]:
        lines.append(
            f"📅 Yayın: {format_date(datetime(*entry['published_parsed'][:6]))}  "
        )
    if entry["closing_datetime"]:
        lines.append(f"⏳ Son Başvuru: {format_date(entry['closing_datetime'])}  ")
    if entry["budget"]:
        lines.append(f"💰 Bütçe: £{entry['budget']}  ")
    if entry["pdf_link"]:
        lines.append(f"📎 Belgeler: [Şartname PDF]({entry['pdf_link']})")

    lines.extend(
        [
            f"🔍 Eşleşen kelime: `{keyword}`",
            f"🔗 [İhaleyi Görüntüle]({entry['link']})",
        ]
    )
    text = "\n".join(lines)
    buttons = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Uygun", callback_data=f"suit:{entry['id']}"),
                InlineKeyboardButton("❌ Alakasız", callback_data=f"unsuit:{entry['id']}"),
            ]
        ]
    )
    return text, buttons


# ── Logging ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "bot.log"), maxBytes=5 * 1024 * 1024, backupCount=2
)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)


# ── Telegram komutları ─────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not user_exists(cid):
        add_user(cid)
        msg = (
            "Hoş geldiniz! /subscribe <kelimeler> komutuyla ihale anahtar kelimeleri ekleyin."
        )
        logger.info("New user %s registered.", cid)
    else:
        msg = "Zaten kayıtlısınız. /help ile komutları görebilirsiniz."
    await update.message.reply_text(msg)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start – Başlat\n"
        "/subscribe <k1 k2 …> – Kelimelere abone ol (max 5 kelime)\n"
        "/unsubscribe <k> – Abonelik sil\n"
        "/list – Abonelikleri göster\n"
        "/clear – Tüm abonelikleri sil"
    )


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    words = [w.lower() for w in context.args]
    if not words:
        await update.message.reply_text("En az bir kelime girin.")
        return
    if len(words) > 5:
        await update.message.reply_text("En fazla 5 kelime girebilirsiniz.")
        return
    current = list_subscriptions(cid)
    if len(current) + len(words) > 5:
        await update.message.reply_text("Toplam abonelik sınırı 5.")
        return
    added = [w for w in words if add_subscription(cid, w)]
    await update.message.reply_text(
        "Abone olunanlar: " + (", ".join(added) if added else "Hiçbiri (zaten kayıtlı).")
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Silmek istediğiniz kelimeyi yazın.")
        return
    word = context.args[0].lower()
    ok = remove_subscription(cid, word)
    await update.message.reply_text(
        f"{'Silindi' if ok else 'Bulunamadı'}: {word}"
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    subs = list_subscriptions(cid)
    await update.message.reply_text(
        "Abonelikleriniz: " + (", ".join(subs) if subs else "Yok")
    )


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    clear_subscriptions(cid)
    await update.message.reply_text("Tüm abonelikler silindi.")


async def cb_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cid = query.message.chat.id
    choice, _ = query.data.split(":")
    logger.info("User %s clicked %s", cid, choice)


# ── Arka plan işler ────────────────────────────────────────────────────────────
async def job_fetch(context: ContextTypes.DEFAULT_TYPE):
    try:
        entries = fetch_feed_entries()
        subs = get_all_subscriptions()
        now = datetime.now()

        for e in entries:
            pub_dt = datetime(*e["published_parsed"][:6]) if e["published_parsed"] else now
            upd_dt = (
                datetime(*e["updated_parsed"][:6])
                if e["updated_parsed"]
                else pub_dt
            )

            for cid, kw, last_seen in subs:
                if kw not in (e["title"] + e["summary"]).lower():
                    continue
                last_dt = datetime.fromisoformat(last_seen)
                is_new = pub_dt > last_dt
                is_upd = upd_dt > last_dt and upd_dt != pub_dt
                if is_new or is_upd:
                    summary = get_summary(e["summary"])
                    txt, btn = build_message(e, summary, kw, is_upd)
                    await context.bot.send_message(
                        cid, txt, parse_mode="Markdown", reply_markup=btn
                    )
                    update_last_seen(cid, kw, (upd_dt if is_upd else pub_dt).isoformat())

                # 48 saat kala hatırlatma
                if e["closing_datetime"]:
                    if (
                        last_dt < e["closing_datetime"] - timedelta(hours=48) <= now
                    ):  # henüz bildirilmediyse
                        summary = get_summary(e["summary"])
                        txt, btn = build_message(
                            e,
                            summary
                            + "\n⏰ *Hatırlatma: Son başvuruya 48 saat kaldı!*",
                            kw,
                        )
                        await context.bot.send_message(
                            cid, txt, parse_mode="Markdown", reply_markup=btn
                        )
                        update_last_seen(
                            cid, kw, e["closing_datetime"].isoformat()
                        )
    except Exception as exc:
        logger.error("fetch job error: %s", exc, exc_info=True)


async def job_backup(context: ContextTypes.DEFAULT_TYPE):
    try:
        if os.path.exists(DB_FILE):
            dst = os.path.join(
                BACKUP_DIR, f"bot_{datetime.now():%Y%m%d_%H%M%S}.db"
            )
            shutil.copy(DB_FILE, dst)
            logger.info("DB backed up.")
    except Exception as exc:
        logger.error("backup job error: %s", exc, exc_info=True)


# ── main() ─────────────────────────────────────────────────────────────────────
def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Komutlar
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CallbackQueryHandler(cb_buttons))

    # İşler
    jq = app.job_queue
    jq.run_repeating(job_fetch, interval=600, first=10)
    jq.run_daily(job_backup, time=datetime.now().time().replace(hour=0, minute=0))

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
