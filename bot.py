"""
GPT-4o destekli Telegram ihale bildirim botu – Tam Sürüm + Yönetim Araçları
python-telegram-bot 22.x

Eklenenler / Değişiklikler (2025‑04‑26):
• /stats, /push, /top_keywords admin komutları, sağlık mesajı, gelişmiş yedek + temizlik.
• Feed çekiminde otomatik 3 retry, TTL ile backup temizliği.
• Hata raporları admin’e; ADMIN_CHAT_ID .env’de zorunlu.
• REMINDER_WINDOW_HOURS sabiti; seen_at damgası → 24 saatlik istatistikler.
"""

from __future__ import annotations

import logging, os, re, shutil, sys, time
from contextlib import suppress
from datetime import datetime, timedelta, time as dtime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Tuple

import feedparser, openai, requests, sqlite3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    AIORateLimiter, ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, Defaults
)
from telegram.helpers import escape_markdown

# ── Ortam & Sabitler ──────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR/".env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
if not (BOT_TOKEN and OPENAI_API_KEY and ADMIN_CHAT_ID):
    sys.exit(".env’de BOT_TOKEN, OPENAI_API_KEY ve ADMIN_CHAT_ID zorunlu.")
ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
openai.api_key = OPENAI_API_KEY

FEED_URL               = "https://www.contractsfinder.service.gov.uk/Published/Notices/Rss"
DB_FILE                = BASE_DIR/"bot.db"
BACKUP_DIR, LOG_DIR, CACHE_DIR = (BASE_DIR/p for p in ("backup","log","cache"))
MAX_KEYWORDS           = 5
FETCH_INTERVAL_SEC     = 600        # 10 dk
CACHE_TTL_DAYS         = 30
REMINDER_WINDOW_HOURS  = (46.5, 49.5)  # min, max
HEALTHCHECK_INTERVAL_H = 6
RETRY_COUNT            = 3
RETRY_DELAY_SEC        = 1

for d in (BACKUP_DIR, LOG_DIR, CACHE_DIR): d.mkdir(exist_ok=True)

MONTHS_TR = {"January":"Ocak","February":"Şubat","March":"Mart","April":"Nisan","May":"Mayıs","June":"Haziran","July":"Temmuz","August":"Ağustos","September":"Eylül","October":"Ekim","November":"Kasım","December":"Aralık"}
md = lambda t: escape_markdown(t,version=2)
tr_date = lambda dt: f"{dt.day} {MONTHS_TR.get(dt.strftime('%B'),dt.strftime('%B'))} {dt.year}"

# ── Logging ────────────────────────────────────────────────────────────────────
logger = logging.getLogger("tenderbot"); logger.setLevel(logging.INFO)
_rot = RotatingFileHandler(LOG_DIR/"bot.log",maxBytes=5*1024*1024,backupCount=2)
_rot.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_rot)

# ── DB ─────────────────────────────────────────────────────────────────────────

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY, joined_at TEXT);
            CREATE TABLE IF NOT EXISTS subs  (chat_id INTEGER, keyword TEXT, PRIMARY KEY(chat_id,keyword));
            CREATE TABLE IF NOT EXISTS seen  (
                chat_id INTEGER, keyword TEXT, tender_id TEXT,
                seen_at TEXT, reminded INTEGER DEFAULT 0,
                PRIMARY KEY(chat_id,keyword,tender_id));
            """
        )
        # legacy → sütun ekle
        cols=[r[1] for r in c.execute("PRAGMA table_info(seen)")]  # type: ignore
        if "seen_at" not in cols:
            c.execute("ALTER TABLE seen ADD COLUMN seen_at TEXT DEFAULT ''")

# ── DB yardımcıları ───────────────────────────────────────────────────────────

def add_user(cid:int):
    with db() as c:
        c.execute("INSERT OR IGNORE INTO users VALUES(?,?)",(cid,datetime.now(timezone.utc).isoformat()))

def sub_list(cid:int)->List[str]:
    with db() as c:
        return [r[0] for r in c.execute("SELECT keyword FROM subs WHERE chat_id=?",(cid,))]

def sub_add(cid:int,kw:str)->bool:
    try:
        with db() as c: c.execute("INSERT INTO subs VALUES(?,?)",(cid,kw)); return True
    except sqlite3.IntegrityError: return False

def sub_del(cid:int,kw:str)->bool:
    with db() as c:
        cur=c.execute("DELETE FROM subs WHERE chat_id=? AND keyword=?",(cid,kw)); return cur.rowcount>0

def sub_clear(cid:int):
    with db() as c: c.execute("DELETE FROM subs WHERE chat_id=?",(cid,))

def subs_all()->List[Tuple[int,str]]:
    with db() as c: return c.execute("SELECT chat_id,keyword FROM subs").fetchall()

def seen_mark(cid:int,kw:str,tid:str):
    with db() as c:
        c.execute("INSERT OR IGNORE INTO seen VALUES(?,?,?,?,0)",(cid,kw,tid,datetime.now(timezone.utc).isoformat()))

def seen(cid:int,kw:str,tid:str)->bool:
    with db() as c:
        return c.execute("SELECT 1 FROM seen WHERE chat_id=? AND keyword=? AND tender_id=?",(cid,kw,tid)).fetchone() is not None

def reminded(cid:int,kw:str,tid:str)->bool:
    with db() as c:
        row=c.execute("SELECT reminded FROM seen WHERE chat_id=? AND keyword=? AND tender_id=?",(cid,kw,tid)).fetchone()
        return bool(row and row[0])

def set_reminded(cid:int,kw:str,tid:str):
    with db() as c:
        c.execute("UPDATE seen SET reminded=1 WHERE chat_id=? AND keyword=? AND tender_id=?",(cid,kw,tid))

def notif_last24h()->int:
    since=(datetime.now(timezone.utc)-timedelta(days=1)).isoformat()
    with db() as c:
        return c.execute("SELECT COUNT(*) FROM seen WHERE seen_at>?",(since,)).fetchone()[0]

def top_keywords(limit:int=5)->List[Tuple[str,int]]:
    with db() as c:
        return c.execute("SELECT keyword,COUNT(*) FROM subs GROUP BY keyword ORDER BY COUNT(*) DESC LIMIT ?",(limit,)).fetchall()

# ── Feed ───────────────────────────────────────────────────────────────────────

NETWORK_ERRORS=(requests.exceptions.Timeout,requests.exceptions.ConnectionError,requests.exceptions.SSLError)

def fetch_entries()->List[Dict[str,Any]]:
    attempt=0
    while attempt<RETRY_COUNT:
        try:
            r=requests.get(FEED_URL,timeout=10); r.raise_for_status(); break
        except NETWORK_ERRORS as e:
            logger.warning("Feed deneme %s/%s: %s",attempt+1,RETRY_COUNT,e); attempt+=1; time.sleep(RETRY_DELAY_SEC)
        except Exception as e:
            logger.error("Feed çekilemedi – fatal: %s",e); return []
    else:
        logger.error("Feed çekiminde %s deneme başarısız.",RETRY_COUNT); return []

    feed=feedparser.parse(r.text)
    items=[]
    for e in feed.entries:
        soup=BeautifulSoup(e.get("summary",""),"html.parser"); txt=soup.get_text(" ",strip=True)
        it={"id":e.get("id") or e.get("link") or str(hash(e.get("link",""))),"title":e.get("title",""),"link":e.get("link"),"summary_html":e.get("summary",""),"summary_text":txt,"published_parsed":getattr(e,"published_parsed",None),"updated_parsed":getattr(e,"updated_parsed",None),"closing_datetime":None,"budget":None,"pdf_link":None}
        if m:=re.search(r"(\d{1,2} [A-Za-z]+ \d{4}(?: \d{1,2}:\d{2})?)",txt):
            for f in ("%d %B %Y %H:%M","%d %B %Y"):
                with suppress(ValueError): it["closing_datetime"]=datetime.strptime(m.group(1),f); break
        if m2:=re.search(r"£([0-9,]+)",txt): it["budget"]=m2.group(1).replace(",","")
        for l in e.get("links",[]):
            if l.get("type")=="application/pdf": it["pdf_link"]=l["href"]; break
        if not it["pdf_link"]:
            a=soup.find("a",href=True,string=re.compile("PDF",re.I));
            if a: it["pdf_link"]=a["href"]
        items.append(it)
    return items

# ── Özet ───────────────────────────────────────────────────────────────────────

def summarise(html:str)->str:
    text=BeautifulSoup(html,"html.parser").get_text(" ",strip=True)[:1000]
    cid=str(abs(hash(text))); path=CACHE_DIR/f"{cid}.txt"
    if path.exists(): return path.read_text()
    try:
        r=openai.ChatCompletion.create(model="gpt-4o",messages=[{"role":"system","content":"İhale özetleri."},{"role":"user","content":"Aşağıdaki kamu ihalesini jargon kullanmadan, 2–3 cümlede özetle:\n"+text}],timeout=15)
        s=r.choices[0].message.content.strip()
    except Exception as e:
        logger.error("OpenAI: %s",e); s=text[:200]+" …"
    path.write_text(s); return s

# ── Mesaj ─────────────────────────────────────────────────────────────────────

def build_msg(ent:Dict[str,Any],summ:str,kw:str,upd=False)->Tuple[str,InlineKeyboardMarkup]:
    title=ent["title"]+(" (Güncellendi)" if upd else "")
    lines=[f"📝 *{md(title)}*",f"🧾 {md(summ)}"]
    if ent["published_parsed"]: lines.append(f"📅 Yayın: {md(tr_date(datetime(*ent['published_parsed'][:6])))}")
    if ent["closing_datetime"]: lines.append(f"⏳ Son Başvuru: {md(tr_date(ent['closing_datetime']))}")
    if ent["budget"]: lines.append(f"💰 Bütçe: £{md(ent['budget'])}")
    if ent["pdf_link"]: lines.append(f"📎 Belgeler: [Şartname PDF]({md(ent['pdf_link'])})")
    lines.extend([f"🔍 Eşleşen kelime: `{md(kw)}`",f"🔗 [İhaleyi Görüntüle]({md(ent['link'])})"])
    kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Uygun",callback_data=f"ok:{ent['id']}"),InlineKeyboardButton("❌ Alakasız",callback_data=f"no:{ent['id']}")]])
    return "\n".join(lines),kb

# ── Yardımcılar ───────────────────────────────────────────────────────────────

def is_admin(cid:int)->bool: return cid==ADMIN_CHAT_ID

# ── Komutlar ──────────────────────────────────────────────────────────────────
async def cmd_start(u:Update,c:ContextTypes.DEFAULT_TYPE):
    add_user(u.effective_chat.id); await u.message.reply_text("Hoş geldiniz! /subscribe ile kelime ekleyin.")

async def cmd_help(u:Update,c:ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text("/subscribe /unsubscribe /list /clear /stats /push /top_keywords")

async def cmd_subscribe(u:Update,c:ContextTypes.DEFAULT_TYPE):
    cid=u.effective_chat.id; kws=[w.lower() for w in c.args][:MAX_KEYWORDS]
    if not kws: return await u.message.reply_text("Kelime girin.")
    if len(sub_list(cid))+len(kws)>MAX_KEYWORDS: return await u.message.reply_text("Toplam 5 kelime.")
    added=[k for k in kws if sub_add(cid,k)]; await u.message.reply_text("Abone olunan: "+(", ".join(added) or "Yok"))

async def cmd_unsubscribe(u:Update,c:ContextTypes.DEFAULT_TYPE):
    if not c.args: return await u.message.reply_text("Kelime yaz")
    ok=sub_del(u.effective_chat.id,c.args[0].lower()); await u.message.reply_text("Silindi" if ok else "Bulunamadı")

async def cmd_list(u:Update,c:ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(", ".join(sub_list(u.effective_chat.id)) or "Yok")

async def cmd_clear(u:Update,c:ContextTypes.DEFAULT_TYPE):
    sub_clear(u.effective_chat.id); await u.message.reply_text("Temizlendi.")

async def cmd_stats(u:Update,c:ContextTypes.DEFAULT_TYPE):
    if not is_admin(u.effective_chat.id): return
    with db() as d:
        users=d.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        subs=d.execute("SELECT COUNT(*) FROM subs").fetchone()[0]
    notifs=notif_last24h()
    await u.message.reply_text(f"👥 Kullanıcı: {users}\n🔑 Abonelik: {subs}\n✉️ Son 24 saat bildirim: {notifs}")

async def cmd_push(u:Update,c:ContextTypes.DEFAULT_TYPE):
    if not is_admin(u.effective_chat.id): return
    msg=" ".join(c.args)
    if not msg: return await u.message.reply_text("/push <mesaj>")
    for (uid,) in db().execute("SELECT chat_id FROM users"):
        with suppress(Exception): await c.bot.send_message(uid,msg)
    await u.message.reply_text("Gönderildi.")

async def cmd_top(u:Update,c:ContextTypes.DEFAULT_TYPE):
    rows=top_keywords(); txt="\n".join(f"{i+1}. {k} – {n}" for i,(k,n) in enumerate(rows)) or "Henüz veri yok."
    await u.message.reply_text(txt)

async def cb_buttons(u:Update,c:ContextTypes.DEFAULT_TYPE):
    q=u.callback_query; await q.answer(); await q.edit_reply_markup()

# ── İşler ─────────────────────────────────────────────────────────────────────
async def job_fetch(ctx:ContextTypes.DEFAULT_TYPE):
    try:
        now=datetime.now(timezone.utc)
        for e in fetch_entries():
            pub=datetime(*e['published_parsed'][:6],tzinfo=timezone.utc) if e['published_parsed'] else now
            upd=datetime(*e['updated_parsed'][:6],tzinfo=timezone.utc) if e['updated_parsed'] else pub
            for cid,kw in subs_all():
                if kw not in (e['title']+e['summary_text']).lower(): continue
                tid=e['id']
                if not seen(cid,kw,tid):
                    text,kb=build_msg(e,summarise(e['summary_html']),kw,False)
                    with suppress(Exception): await ctx.bot.send_message(cid,text,parse_mode=ParseMode.MARKDOWN_V2,reply_markup=kb)
                    seen_mark(cid,kw,tid)
                # reminder
                if e['closing_datetime'] and not reminded(cid,kw,tid):
                    left=e['closing_datetime']-now
                    if timedelta(hours=REMINDER_WINDOW_HOURS[0])<left<=timedelta(hours=REMINDER_WINDOW_HOURS[1]):
                        text,kb=build_msg(e,summarise(e['summary_html'])+"\n⏰ *48 saat kaldı!*",kw)
                        with suppress(Exception): await ctx.bot.send_message(cid,text,parse_mode=ParseMode.MARKDOWN_V2,reply_markup=kb)
                        set_reminded(cid,kw,tid)
    except Exception as e:
        logger.exception("job_fetch crash")
        with suppress(Exception): await ctx.bot.send_message(ADMIN_CHAT_ID,f"❌ job_fetch hata: {e}")

async def job_backup(ctx:ContextTypes.DEFAULT_TYPE):
    try:
        if DB_FILE.exists(): shutil.copy(DB_FILE,BACKUP_DIR/f"bot_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.db")
        # TTL temizlik
        cutoff=datetime.now(timezone.utc)-timedelta(days=CACHE_TTL_DAYS)
        for p in BACKUP_DIR.glob("*.db"):
            if datetime.fromtimestamp(p.stat().st_mtime,timezone.utc)<cutoff: p.unlink()
    except Exception as e:
        logger.error("backup job error: %s",e)

async def job_health(ctx:ContextTypes.DEFAULT_TYPE):
    with suppress(Exception): await ctx.bot.send_message(ADMIN_CHAT_ID,"✅ Bot aktif.")

# ── main ───────────────────────────────────────────────────────────────────────

def main():
    init_db()
    app=(ApplicationBuilder().token(BOT_TOKEN).defaults(Defaults(parse_mode=ParseMode.MARKDOWN_V2,tzinfo=timezone.utc)).rate_limiter(AIORateLimiter()).build())

    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("help",cmd_help))
    app.add_handler(CommandHandler("subscribe",cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe",cmd_unsubscribe))
    app.add_handler(CommandHandler("list",cmd_list))
    app.add_handler(CommandHandler("clear",cmd_clear))
    app.add_handler(CommandHandler("stats",cmd_stats))
    app.add_handler(CommandHandler("push",cmd_push))
    app.add_handler(CommandHandler("top_keywords",cmd_top))
    app.add_handler(CallbackQueryHandler(cb_buttons))

    jq=app.job_queue
    jq.run_repeating(job_fetch,interval=FETCH_INTERVAL_SEC,first=10,name="fetch")
    jq.run_repeating(job_backup,interval=86400,first=60,name="backup")
    jq.run_repeating(job_health,interval=HEALTHCHECK_INTERVAL_H*3600,first=120,name="health")

    logger.info("Bot started")
    app.run_polling(stop_signals=None)

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: print("Çıkılıyor…")
