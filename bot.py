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

# Load environment variables
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

# Database setup
def db_conn():
    return sqlite3.connect(DB_FILE)

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY, joined_at TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS subs (chat_id INTEGER, keyword TEXT, last_seen TEXT, PRIMARY KEY(chat_id, keyword))")
        conn.commit()

# User functions
def add_user(chat_id: int):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO users (chat_id, joined_at) VALUES (?, ?)", (chat_id, datetime.now().isoformat()))

def user_exists(chat_id: int) -> bool:
    with db_conn() as conn:
        cur = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (chat_id,))
        return cur.fetchone() is not None

# Subscription functions
def add_subscription(chat_id: int, keyword: str) -> bool:
    try:
        with db_conn() as conn:
            conn.execute("INSERT INTO subs (chat_id, keyword, last_seen) VALUES (?, ?, ?)", (chat_id, keyword, "1970-01-01T00:00:00"))
        return True
    except sqlite3.IntegrityError:
        return False

def remove_subscription(chat_id: int, keyword: str) -> bool:
    with db_conn() as conn:
        cur = conn.execute("DELETE FROM subs WHERE chat_id=? AND keyword=?", (chat_id, keyword))
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
        conn.execute("UPDATE subs SET last_seen=? WHERE chat_id=? AND keyword=?", (timestamp, chat_id, keyword))

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(os.path.join(LOG_DIR, "bot.log"), maxBytes=5*1024*1024, backupCount=2)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Feed fetching
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
        entries.append(entry)
    return entries

# GPT Summary
def get_summary(text: str) -> str:
    content = BeautifulSoup(text, "html.parser").get_text()[:1000]
    cache_id = str(abs(hash(content)))
    cache_file = os.path.join(CACHE_DIR, cache_id + ".txt")
    if os.path.exists(cache_file):
        return open(cache_file, "r", encoding="utf-8").read()
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Tender summary generator."},
                {"role": "user", "content": content},
            ],
        )
        summary = resp.choices[0].message.content.strip()
    except Exception:
        summary = content[:120] + "…"
    with open(cache_file, "w", encoding="utf-8") as f:
        f.write(summary)
    return summary

# Message builder
def build_message(entry: dict, summary: str, keyword: str, updated: bool = False):
    title = entry["title"] + (" (Updated)" if updated else "")
    lines = [
        f"📝 *{title}*",
        f"🧾 {summary}",
        f"🔍 Matched keyword: {keyword}",
        f"🔗 [View Tender]({entry['link']})",
    ]
    text = "\n".join(lines)
    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Relevant", callback_data=f"suit:{entry['id']}"),
            InlineKeyboardButton("❌ Irrelevant", callback_data=f"unsuit:{entry['id']}"),
        ]
    ])
    return text, buttons

# Telegram commands
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not user_exists(cid):
        add_user(cid)
        await update.message.reply_text("Welcome! Use /subscribe <keywords> to get notifications.")
    else:
        await update.message.reply_text("You are already registered. Use /help for commands.")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start – Start\n"
        "/subscribe <kw> – Subscribe to keywords\n"
        "/unsubscribe <kw> – Remove subscription\n"
        "/list – List your subscriptions\n"
        "/clear – Clear all subscriptions"
    )

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    words = [w.lower() for w in context.args]
    if not words:
        await update.message.reply_text("Please provide at least one keyword.")
        return
    if len(words) > 5:
        await update.message.reply_text("You can subscribe to a maximum of 5 keywords.")
        return
    current = list_subscriptions(cid)
    if len(current) + len(words) > 5:
        await update.message.reply_text("Total subscription limit is 5.")
        return
    added = [w for w in words if add_subscription(cid, w)]
    await update.message.reply_text("Subscribed to: " + (", ".join(added) if added else "None (already subscribed)."))

async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Specify a keyword to remove.")
        return
    word = context.args[0].lower()
    ok = remove_subscription(cid, word)
    await update.message.reply_text(f"{'Removed' if ok else 'Not found'}: {word}")

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    subs = list_subscriptions(cid)
    await update.message.reply_text("Your subscriptions: " + (", ".join(subs) if subs else "None"))

async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    clear_subscriptions(cid)
    await update.message.reply_text("All subscriptions cleared.")

async def cb_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Feedback recorded ✔️", show_alert=False)

# Main function
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CallbackQueryHandler(cb_buttons))

    logger.info("Bot started.")
    app.run_polling()

if __name__ == "__main__":
    main()
