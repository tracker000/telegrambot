"""
Telegram İhale Botu · Async · PTB v22 · OpenAI 1.x
Python 3.11+ · Tek dosya
--------------------------------------------------
Abone olunan anahtar kelimelere göre Contracts Finder ihale bildirimleri
+ 48 saat kala hatırlatma + GPT-4o özetleme + admin komutları
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
import aiosqlite
import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from telegram.helpers import escape_markdown

# ──────────────────────────── Konfig & Ortam ────────────────────────────────
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")  # .env otomatik

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_ID", "").split(",") if x}
FEED_URL = os.getenv(
    "FEED_URL",
    "https://www.contractsfinder.service.gov.uk/Published/Notices/Rss",
)
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

DB_FILE = BASE_DIR / "bot.db"
BACKUP_DIR = BASE_DIR / "backup"
BACKUP_DIR.mkdir(exist_ok=True)

FETCH_INTERVAL = 3600      # s – feed poll
REMINDER_LOOKAHEAD = 48    # h – reminder penceresi
REMINDER_SCAN_EVERY = 1800 # s
BACKUP_INTERVAL = 86400    # s
HEALTH_INTERVAL = 43200    # s

# ──────────────────────────── Logging ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("tenderbot")

# ──────────────────────────── OpenAI Ayarı ───────────────────────────────────
try:
    import openai  # type: ignore

    openai.api_key = OPENAI_KEY
except ImportError:
    log.warning("openai paketi bulunamadı; özetler metin kısaltmasıyla döner")
    openai = None  # type: ignore

# ──────────────────────────── SQLite Şeması ─────────────────────────────────
DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS subscriptions (
    user_id INTEGER,
    keyword TEXT,
    PRIMARY KEY (user_id, keyword)
);

CREATE TABLE IF NOT EXISTS tenders (
    id TEXT PRIMARY KEY,
    title TEXT,
    link TEXT,
    published_utc TEXT,
    closing_utc TEXT,
    summary_html TEXT
);

CREATE TABLE IF NOT EXISTS sent (
    user_id INTEGER,
    tender_id TEXT,
    kind TEXT,     -- 'notify' | 'reminder'
    PRIMARY KEY (user_id, tender_id, kind)
);
"""


async def db_exec(sql: str, params: tuple = (), fetch: bool = False):
    """Lightweight wrapper for aiosqlite queries."""
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(sql, params)
        await db.commit()
        if fetch:
            return await cur.fetchall()
        return None


async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript(DDL)
        await db.commit()


# ──────────────────────────── Yardımcılar ───────────────────────────────────
md = lambda t: escape_markdown(t, version=2)
is_admin = lambda uid: uid in ADMIN_IDS

CLOSE_RE = re.compile(r"Closing date:? *(\d{1,2} \w+ \d{4})", re.I)


async def fetch_openai_summary(html: str) -> str:
    """GPT-4o özet; hata veya anahtar yoksa kısa fallback."""
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)[:1000]
    if not openai or not OPENAI_KEY:
        return "ℹ️ GPT özetleme devre dışı.\n" + text[:200] + " …"
    try:
        resp = await openai.ChatCompletion.acreate(
            model="gpt-4o",
            max_tokens=120,
            messages=[
                {"role": "system", "content": "İhale özetleri"},
                {"role": "user", "content": text},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("OpenAI hatası: %s", exc)
        return "⚠️ Özetleme başarısız.\n" + text[:200] + " …"


async def get_subscriptions_map() -> Dict[str, List[int]]:
    """keyword -> [user_id, ...]"""
    rows = await db_exec("SELECT user_id, keyword FROM subscriptions", fetch=True)
    subs: Dict[str, List[int]] = {}
    for uid, kw in rows:
        subs.setdefault(kw, []).append(uid)
    return subs


async def record_and_send(
    tender: Dict[str, Any],
    users: List[int],
    kind: str,
    ctx: ContextTypes.DEFAULT_TYPE,
):
    """Send message if not already sent."""
    for uid in users:
        exists = await db_exec(
            "SELECT 1 FROM sent WHERE user_id=? AND tender_id=? AND kind=?",
            (uid, tender["id"], kind),
            fetch=True,
        )
        if exists:
            continue

        if kind == "notify":
            summary = await fetch_openai_summary(tender["summary_html"])
            text = (
                f"📝 *{md(tender['title'])}*\n"
                f"🧾 {md(summary)}\n"
                f"🔗 {tender['link']}"
            )
        else:  # reminder
            text = (
                f"⏰ *{md(tender['title'])}*\n"
                f"48 saatten az kaldı!\n"
                f"🔗 {tender['link']}"
            )

        try:
            await ctx.bot.send_message(
                uid,
                text,
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Mesaj gönderilemedi uid=%s: %s", uid, e)

        await db_exec(
            "INSERT INTO sent VALUES (?,?,?)", (uid, tender["id"], kind)
        )


# ──────────────────────────── Komut Fonksiyonları ────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hoş geldiniz! `/subscribe <kelime>` yazarak abone olun.",
        parse_mode="Markdown",
    )


async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        return await update.message.reply_text("Kelime?")
    keyword = " ".join(ctx.args).lower()
    await db_exec(
        "INSERT OR IGNORE INTO subscriptions VALUES (?,?)",
        (update.effective_user.id, keyword),
    )
    await update.message.reply_text(f"'{keyword}' için abone oldunuz.")


async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        return await update.message.reply_text("Kelime?")
    keyword = " ".join(ctx.args).lower()
    await db_exec(
        "DELETE FROM subscriptions WHERE user_id=? AND keyword=?",
        (update.effective_user.id, keyword),
    )
    await update.message.reply_text(f"'{keyword}' aboneliğiniz silindi.")


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = await db_exec(
        "SELECT keyword FROM subscriptions WHERE user_id=?",
        (update.effective_user.id,),
        fetch=True,
    )
    txt = "\n".join(f"- {kw}" for (kw,) in rows) or "Abonelik yok."
    await update.message.reply_text(txt)


# ─────────────── Admin: /stats /push /backup ────────────────────────────────
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    users = (await db_exec("SELECT COUNT(DISTINCT user_id) FROM subscriptions", fetch=True))[0][0]
    subs = (await db_exec("SELECT COUNT(*) FROM subscriptions", fetch=True))[0][0]
    tenders = (await db_exec("SELECT COUNT(*) FROM tenders", fetch=True))[0][0]
    await update.message.reply_text(
        f"👥 {users} kullanıcı\n🔑 {subs} abonelik\n📂 {tenders} ihale"
    )


async def cmd_push(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        return await update.message.reply_text("/push <mesaj>")
    msg = " ".join(ctx.args)
    rows = await db_exec("SELECT DISTINCT user_id FROM subscriptions", fetch=True)
    for (uid,) in rows:
        try:
            await ctx.bot.send_message(uid, msg)
        except Exception as e:
            log.warning("Push fail %s: %s", uid, e)
    await update.message.reply_text("Toplu mesaj gönderildi ✔️")


async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    dest = BACKUP_DIR / f"bot_manual_{datetime.now():%Y%m%d_%H%M%S}.db"
    await asyncio.to_thread(shutil.copy, DB_FILE, dest)
    await update.message.reply_text(f"Yedek alındı: {dest.name}")


# ───────────────────────── Callback (inline buton) ───────────────────────────
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Kullanıcı geri bildirimi alındı."""
    query = update.callback_query
    await query.answer("Geri bildiriminiz kaydedildi ✔️", show_alert=False)


# ───────────────────────────── Job’lar ───────────────────────────────────────
async def parse_feed() -> List[Dict[str, Any]]:
    """RSS feed’i indirir ve parse eder."""
    async with aiohttp.ClientSession() as sess:
        async with sess.get(FEED_URL, timeout=20) as resp:
            raw = await resp.read()
    feed = feedparser.parse(raw)
    items: List[Dict[str, Any]] = []
    for e in feed.entries:
        tid = e.get("id") or e.get("link")
        if not tid:
            continue
        title = e.get("title", "Başlık yok")
        link = e.get("link", "")
        published = (
            datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            if e.get("published_parsed")
            else datetime.now(timezone.utc)
        )
        summary = e.get("summary", "")
        closing = None
        if (m := CLOSE_RE.search(summary)):
            try:
                closing = datetime.strptime(m.group(1), "%d %B %Y").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                pass
        items.append(
            dict(
                id=tid,
                title=title,
                link=link,
                published=published,
                closing=closing,
                summary_html=summary,
            )
        )
    return items


async def job_fetch(ctx: ContextTypes.DEFAULT_TYPE):
    """Yeni ihale verilerini çek ve bildirim gönder."""
    try:
        entries = await parse_feed()
        if not entries:
            return

        subs_map = await get_subscriptions_map()

        for tender in entries:
            await db_exec(
                "INSERT OR IGNORE INTO tenders VALUES (?,?,?,?,?,?)",
                (
                    tender["id"],
                    tender["title"],
                    tender["link"],
                    tender["published"].isoformat(),
                    tender["closing"].isoformat() if tender["closing"] else None,
                    tender["summary_html"],
                ),
            )

            # ilk bildirim
            lower_title = tender["title"].lower()
            for kw, users in subs_map.items():
                if kw in lower_title:
                    await record_and_send(tender, users, "notify", ctx)

        log.info("job_fetch: %s kayıt işlendi", len(entries))
    except Exception as exc:
        log.exception("job_fetch hata: %s", exc)


async def job_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Kapanışa 48 ±1 saat kalan ihaleler için hatırlatma gönder."""
    try:
        now = datetime.now(timezone.utc)
        win_start = (now + timedelta(hours=REMINDER_LOOKAHEAD - 1)).isoformat()
        win_end = (now + timedelta(hours=REMINDER_LOOKAHEAD)).isoformat()
        rows = await db_exec(
            """
            SELECT id, title, link, summary_html
            FROM tenders
            WHERE closing_utc BETWEEN ? AND ?
            """,
            (win_start, win_end),
            fetch=True,
        )
        if not rows:
            return
        subs_map = await get_subscriptions_map()
        for tid, title, link, summary_html in rows:
            tender = dict(id=tid, title=title, link=link, summary_html=summary_html)
            await record_and_send(tender, sum(subs_map.values(), []), "reminder", ctx)
        log.info("job_reminder: %s ihale", len(rows))
    except Exception as exc:
        log.exception("job_reminder hata: %s", exc)


async def job_backup(_ctx: ContextTypes.DEFAULT_TYPE):
    dest = BACKUP_DIR / f"bot_{datetime.now():%Y%m%d_%H%M%S}.db"
    await asyncio.to_thread(shutil.copy, DB_FILE, dest)
    log.info("DB yedeği: %s", dest)


async def job_health(_ctx: ContextTypes.DEFAULT_TYPE):
    log.info("Health ping OK")


# ───────────────────────────── main ──────────────────────────────────────────
async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN zorunlu")
    if not ADMIN_IDS:
        log.warning("ADMIN_ID tanımlanmadı; admin komutları pasif")

    await init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Kullanıcı komutları
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("list", cmd_list))

    # Admin komutları
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("push", cmd_push))
    app.add_handler(CommandHandler("backup", cmd_backup))

    app.add_handler(CallbackQueryHandler(on_callback))

    # Job’lar
    jq = app.job_queue
    jq.run_repeating(job_fetch, FETCH_INTERVAL, first=10)
    jq.run_repeating(job_reminder, REMINDER_SCAN_EVERY, first=600)
    jq.run_repeating(job_backup, BACKUP_INTERVAL, first=120)
    jq.run_repeating(job_health, HEALTH_INTERVAL, first=60)

    log.info("Bot başladı…")
    await app.run_polling(stop_signals=None)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Çıkılıyor…")
