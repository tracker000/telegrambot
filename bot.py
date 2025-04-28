#!/usr/bin/env python3
"""
Comprehensive Telegram Bot for UK Contracts Finder (Corrected & Enhanced)

Features:
- CSV feed support with HTTP retry and respect for Retry-After
- SQLite backend with error handling, indexing, and async-safe access
- Per-user timezone preferences (/settimezone), with default and validation
- Keyword subscription with AND/OR and quoted phrase support
- Inline and command-based subscription management
- Long expressions handled via hash mapping to respect Telegram callback_data limits
- /fetchnow for immediate fetch; scheduled polling every FETCH_INTERVAL
- Daily summary at 08:00 user local time
- /export subscriptions in JSON and CSV
- Smart sentence splitting for Telegram message limits
- Subscription limits and friendly error messages
"""

import asyncio
import csv
import json
import logging
import os
import re
import shlex
import sqlite3
import sys
import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, time as dtime
from io import StringIO, BytesIO

import requests
from dateutil import parser as date_parser
from dateutil import tz
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telegram import (
    InlineKeyboardButton, ReplyKeyboardMarkup,
    KeyboardButton, Update, InputFile, ParseMode
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    CallbackQueryHandler, ContextTypes, AIORateLimiter
)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    print("Error: BOT_TOKEN is not set. Please configure your .env file.")
    sys.exit(1)

DEFAULT_TZ = os.getenv('TIMEZONE', 'UTC')
if not tz.gettz(DEFAULT_TZ):
    logging.warning(f"Invalid DEFAULT_TZ '{DEFAULT_TZ}', falling back to UTC.")
    DEFAULT_TZ = 'UTC'

FETCH_INTERVAL_MINUTES = int(os.getenv('FETCH_INTERVAL', '10'))
FEED_URL = os.getenv(
    'FEED_URL',
    'https://www.contractsfinder.service.gov.uk/Harvester/Notices/Data/CSV/Daily'
)

# Constants
MSG_MAX = 8192  # Telegram now allows up to 8192 chars
MAX_SUBSCRIPTIONS_PER_USER = 50
CALLBACK_DATA_LIMIT = 60  # bytes for callback_data

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(handler)

# HTTP session with retries and respect for Retry-After headers
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount('https://', adapter)
session.mount('http://', adapter)

# Database setup
db_path = 'bot.db'
_db_lock = asyncio.Lock()

@contextmanager
def get_conn():
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

async def db_exec(sql, params=()):
    try:
        async with _db_lock:
            with get_conn() as conn:
                cur = conn.execute(sql, params)
                return cur.rowcount
    except sqlite3.Error as e:
        logger.error('DB exec error: %s | %s', e, sql)
        return 0

async def db_query(sql, params=()):
    try:
        async with _db_lock:
            with get_conn() as conn:
                cur = conn.execute(sql, params)
                return cur.fetchall()
    except sqlite3.Error as e:
        logger.error('DB query error: %s | %s', e, sql)
        return []

async def init_db():
    # Tables
    await db_exec(
        '''CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            tz TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            last_summary TEXT
        )'''
    )
    await db_exec(
        '''CREATE TABLE IF NOT EXISTS subs (
            chat_id INTEGER NOT NULL,
            expr_key TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY(chat_id, expr_key)
        )'''
    )
    await db_exec(
        '''CREATE TABLE IF NOT EXISTS sent (
            chat_id INTEGER NOT NULL,
            notice_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY(chat_id, notice_id)
        )'''
    )
    # For long expressions mapping
    await db_exec(
        '''CREATE TABLE IF NOT EXISTS expr_map (
            hash TEXT PRIMARY KEY,
            expr TEXT NOT NULL
        )'''
    )
    await db_exec("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
    # Indexes
    await db_exec('CREATE INDEX IF NOT EXISTS idx_sent_ts ON sent(ts)')
    await db_exec('CREATE INDEX IF NOT EXISTS idx_subs_chat ON subs(chat_id)')

# Timezone helpers
async def get_user_tz(chat_id):
    rows = await db_query('SELECT tz FROM users WHERE chat_id=?', (chat_id,))
    return rows[0][0] if rows else DEFAULT_TZ

def format_time(dt, tz_str):
    tzinfo = tz.gettz(tz_str) or tz.gettz(DEFAULT_TZ)
    return dt.astimezone(tzinfo).strftime('%Y-%m-%d %H:%M %Z')

# Expression evaluation with AND/OR and quoted phrases
def eval_expr(text, expr):
    text_lower = text.lower()
    try:
        parts = shlex.split(expr)
    except ValueError:
        parts = expr.split()
    result = None
    op = None
    for token in parts:
        upp = token.upper()
        if upp in ('AND', 'OR'):
            op = upp
            continue
        match = re.search(re.escape(token.lower()), text_lower) is not None
        if result is None:
            result = match
        elif op == 'AND':
            result = result and match
        elif op == 'OR':
            result = result or match
        else:
            result = result and match
        op = None
    return bool(result)

# Fetch feed (async offload)
async def fetch_feed(headers=None):
    try:
        resp = await asyncio.to_thread(session.get, FEED_URL, headers=headers or {}, timeout=15)
        if resp.status_code == 304:
            return []
        resp.raise_for_status()
    except requests.HTTPError as he:
        logger.error('Feed HTTP error: %s', he)
        return None
    except Exception as e:
        logger.error('Feed error: %s', e)
        return None
    lm = resp.headers.get('Last-Modified')
    if lm:
        await db_exec("REPLACE INTO meta(key,value) VALUES('last_modified',?)", (lm,))
    entries = []
    reader = csv.DictReader(StringIO(resp.text))
    for row in reader:
        try:
            dt = date_parser.parse(row.get('publishedDate', ''))
        except Exception:
            continue
        entries.append({
            'id': row.get('uri'),
            'title': (row.get('title') or '').strip(),
            'summary': (row.get('description') or '').strip(),
            'link': row.get('uri'),
            'published': dt
        })
    return entries

# Full fetch ignoring caching (for summary)
async def fetch_feed_force():
    return await fetch_feed(headers={})

# Message splitting
def split_text(text):
    parts, cur = [], ''
    for sent in re.split(r'(?<=[\.\!?])\s+', text):
        if len(cur) + len(sent) + 1 <= MSG_MAX:
            cur += sent + ' '
        else:
            parts.append(cur.strip()); cur = sent + ' '
    if cur:
        parts.append(cur.strip())
    return parts

# Send message helper
def make_markup(buttons):
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

async def send_msg(bot, chat_id, text, markup=None):
    for i, chunk in enumerate(split_text(text)):
        try:
            await bot.send_message(
                chat_id,
                chunk,
                parse_mode=ParseMode.HTML,
                reply_markup=markup if i == 0 else None,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error('Failed to send message: %s', e)

# Core scanning
def current_utc_iso():
    return datetime.now(timezone.utc).isoformat()

async def do_scan(app, chat_id=None):
    # Prepare headers once
    rows = await db_query("SELECT value FROM meta WHERE key='last_modified'")
    headers = {}
    if rows:
        headers['If-Modified-Since'] = rows[0][0]
    entries = await fetch_feed(headers=headers)
    if entries is None:
        if chat_id:
            await send_msg(app.bot, chat_id, '⚠️ Sunucuya ulaşılamıyor. Daha sonra tekrar deneyin.')
        return
    # Cleanup
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    await db_exec('DELETE FROM sent WHERE ts<?', (cutoff,))
    subs = await db_query('SELECT chat_id, expr_key, last_seen FROM subs')
    for cid, key, last_seen in subs:
        expr_map = await db_query('SELECT expr FROM expr_map WHERE hash=?', (key,))
        expr = expr_map[0][0] if expr_map else key
        try:
            last_dt = date_parser.parse(last_seen)
        except:
            last_dt = datetime(1970,1,1,tzinfo=timezone.utc)
        matches = [e for e in entries if e['published'] > last_dt and eval_expr((e['title'] + ' ' + e['summary']).lower(), expr)]
        if not matches:
            continue
        new_max = max(m['published'] for m in matches).isoformat()
        await db_exec('UPDATE subs SET last_seen=? WHERE chat_id=? AND expr_key=?', (new_max, cid, key))
        await send_msg(app.bot, cid, f"🔔 {len(matches)} new matches for '{expr}'")
        user_tz = await get_user_tz(cid)
        for m in matches:
            msg = (
                f"<b>{m['title']}</b>\n"
                f"{m['summary']}\n"
                f"<em>{format_time(m['published'], user_tz)}</em>\n"
                f"<a href='{m['link']}'>View</a>"
            )
            await send_msg(app.bot, cid, msg)
            await db_exec('INSERT OR IGNORE INTO sent VALUES(?,?,?)', (cid, m['id'], current_utc_iso()))

# Scheduled jobs
async def scan_job(ctx: ContextTypes.DEFAULT_TYPE):
    await do_scan(ctx)

async def summary_job(ctx: ContextTypes.DEFAULT_TYPE):
    users = await db_query('SELECT chat_id, tz, last_summary FROM users')
    if not users:
        return
    entries = await fetch_feed_force()
    if entries is None:
        return
    now_utc = datetime.now(timezone.utc)
    for cid, tz_str, last_sum in users:
        try:
            tzinfo = tz.gettz(tz_str) or tz.gettz(DEFAULT_TZ)
            local_now = now_utc.astimezone(tzinfo)
            if local_now.hour != 8 or last_sum == local_now.date().isoformat():
                continue
            subs = await db_query('SELECT expr_key FROM subs WHERE chat_id=?', (cid,))
            counts = {}
            start_utc = datetime.combine(local_now.date(), dtime.min, tzinfo).astimezone(timezone.utc)
            for (key,) in subs:
                expr_map = await db_query('SELECT expr FROM expr_map WHERE hash=?', (key,))
                expr = expr_map[0][0] if expr_map else key
                cnt = sum(1 for e in entries if e['published'] >= start_utc and eval_expr((e['title'] + ' ' + e['summary']).lower(), expr))
                if cnt:
                    counts[expr] = cnt
            if counts:
                text = '📊 Daily summary:' + ''.join(f"\n- '{k}': {v}" for k,v in counts.items())
                await send_msg(ctx.bot, cid, text)
            await db_exec('UPDATE users SET last_summary=? WHERE chat_id=?', (local_now.date().isoformat(), cid))
        except Exception as e:
            logger.error('Summary error for %s: %s', cid, e)

# Command handlers
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    await init_db()
    await db_exec('REPLACE INTO users(chat_id,tz,joined_at) VALUES(?,?,?)',
                  (cid, DEFAULT_TZ, datetime.now(timezone.utc).isoformat()))
    kb = make_markup([
        [KeyboardButton('/subscribe <term>'), KeyboardButton('/unsubscribe <term>')],
        [KeyboardButton('/list'), KeyboardButton('/clear')],
        [KeyboardButton('/settimezone <Zone/City>'), KeyboardButton('/fetchnow')]
    ])
    await update.message.reply_text(
        "👋 Welcome! I will notify you of new UK contract notices matching your keywords.\n"
        "Use /help for commands. Tip: /settimezone Europe/Istanbul.",
        reply_markup=kb
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "/subscribe <expr> - subscribe to a keyword or \"phrase\"\n"
        "/unsubscribe <expr> - remove a subscription\n"
        "/list - show your subscriptions\n"
        "/clear - remove all subscriptions\n"
        "/settimezone <Zone/City> - set your timezone\n"
        "/fetchnow - fetch the feed immediately\n"
        "/export - download subscriptions as JSON and CSV"
    )
    await update.message.reply_text(text)

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    expr_raw = ' '.join(ctx.args).strip()
    if not expr_raw:
        return await update.message.reply_text('Usage: /subscribe <keyword or "phrase">')
    # Determine key
    expr_bytes = expr_raw.encode('utf-8')
    if len(expr_bytes) > CALLBACK_DATA_LIMIT:
        key = hashlib.sha256(expr_bytes).hexdigest()[:16]
        await db_exec('INSERT OR IGNORE INTO expr_map(hash,expr) VALUES(?,?)', (key, expr_raw))
    else:
        key = expr_raw
    count = (await db_query('SELECT COUNT(*) FROM subs WHERE chat_id=?', (cid,)))[0][0]
    if count >= MAX_SUBSCRIPTIONS_PER_USER:
        return await update.message.reply_text(f'⚠️ Limit reached ({MAX_SUBSCRIPTIONS_PER_USER}). Remove some first.')
    await db_exec('INSERT OR IGNORE INTO subs(chat_id,expr_key,last_seen) VALUES(?,?,?)',
                  (cid, key, datetime(1970,1,1,tzinfo=timezone.utc).isoformat()))
    await update.message.reply_text(f'✅ Subscribed to: {expr_raw}')

async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    expr = ' '.join(ctx.args).strip()
    if not expr:
        return await update.message.reply_text('Usage: /unsubscribe <keyword or "phrase">')
    await db_exec('DELETE FROM subs WHERE chat_id=? AND expr_key=?', (cid, expr))
    await update.message.reply_text(f'✅ Unsubscribed from: {expr}')

async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    rows = await db_query('SELECT expr_key FROM subs WHERE chat_id=?', (cid,))
    if not rows:
        return await update.message.reply_text('ℹ️ You have no subscriptions.')
    buttons = []
    for (key,) in rows:
        mp = await db_query('SELECT expr FROM expr_map WHERE hash=?', (key,))
        expr = mp[0][0] if mp else key
        buttons.append([InlineKeyboardButton(expr, callback_data=key)])
    markup = InlineKeyboardMarkup(buttons)
    await update.message.reply_text('📋 Your subscriptions:', reply_markup=markup)

async def cmd_clear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    await db_exec('DELETE FROM subs WHERE chat_id=?', (cid,))
    await update.message.reply_text('🗑️ All subscriptions cleared.')

async def cmd_settimezone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    tz_input = ' '.join(ctx.args).strip()
    if not tz.gettz(tz_input):
        return await update.message.reply_text('Invalid timezone. Example: /settimezone Europe/Istanbul')
    await db_exec('UPDATE users SET tz=? WHERE chat_id=?', (tz_input, cid))
    await update.message.reply_text(f'🌐 Timezone set to {tz_input}')

async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    rows = await db_query('SELECT expr_key,last_seen FROM subs WHERE chat_id=?', (cid,))
    subs = []
    for key, last in rows:
        mp = await db_query('SELECT expr FROM expr_map WHERE hash=?', (key,))
        expr = mp[0][0] if mp else key
        subs.append({'expr': expr, 'last_seen': last})
    data_json = json.dumps({'subscriptions': subs}, indent=2)
    buf_json = BytesIO(data_json.encode()); buf_json.name = 'subscriptions.json'
    buf_csv = BytesIO()
    writer = csv.writer(buf_csv)
    writer.writerow(['expr', 'last_seen'])
    for s in subs:
        writer.writerow([s['expr'], s['last_seen']])
    buf_csv.seek(0); buf_csv.name = 'subscriptions.csv'
    await ctx.bot.send_document(cid, InputFile(buf_json, filename='subscriptions.json'))
    await ctx.bot.send_document(cid, InputFile(buf_csv, filename='subscriptions.csv'))

async def cmd_fetchnow(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await do_scan(ctx, update.effective_chat.id)

async def callback_q(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    cid = q.message.chat.id
    key = q.data
    mp = await db_query('SELECT expr FROM expr_map WHERE hash=?', (key,))
    expr = mp[0][0] if mp else key
    await db_exec('DELETE FROM subs WHERE chat_id=? AND expr_key=?', (cid, key))
    await q.answer(f'✅ Unsubscribed from: {expr}')
    await q.message.edit_text(f'✅ Unsubscribed from: {expr}')

# Main entrypoint
async def main():
    await init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).rate_limiter(AIORateLimiter()).build()
    # Handlers
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('help', cmd_help))
    app.add_handler(CommandHandler('subscribe', cmd_subscribe))
    app.add_handler(CommandHandler('unsubscribe', cmd_unsubscribe))
    app.add_handler(CommandHandler('list', cmd_list))
    app.add_handler(CommandHandler('clear', cmd_clear))
    app.add_handler(CommandHandler('settimezone', cmd_settimezone))
    app.add_handler(CommandHandler('export', cmd_export))
    app.add_handler(CommandHandler('fetchnow', cmd_fetchnow))
    app.add_handler(CallbackQueryHandler(callback_q))
    # Jobs
    app.job_queue.run_repeating(scan_job, interval=FETCH_INTERVAL_MINUTES*60, first=10)
    app.job_queue.run_repeating(summary_job, interval=3600, first=60)
    logger.info('Bot is starting...')
    try:
        await app.run_polling()
    except (KeyboardInterrupt, SystemExit):
        logger.info('Bot stopped.')

if __name__ == '__main__':
    asyncio.run(main())
