"""
ربات ارسال گروهی + پنل مدیریت داخل خود ربات
============================================
همه‌چیز از داخل خود تلگرام قابل تغییر است و در فایل sender_data.json ذخیره می‌شود.

نصب:
    pip install "python-telegram-bot>=20"

اجرا:
    python group_sender.py          (یا: py -3.15 group_sender.py)

سازگاری:
    پایتون ۳.۱۰ تا ۳.۱۵ — روی ۳.۱۴/۳.۱۵ که apscheduler قدیمی با PTB
    سازگار نیست، job_queue خودکار غیرفعال می‌شود (چون اصلاً استفاده نمی‌شود).

اولین اجرا:
    TOKEN را بگذار، اجرا کن، در پیوی ربات /start بزن → تو مالک می‌شوی.

دستورهای مالک:
    /start              متن خوش‌آمد + وضعیت
    /help               راهنما
    /id                 شناسهٔ عددی همین چت
    /groups             لیست گروه‌ها
    /addgroup <id>      افزودن گروه
    /delgroup <id>      حذف گروه
    /owners             لیست مالک‌ها
    /addowner <id>      افزودن مالک (یا ریپلای روی پیام طرف)
    /delowner <id>      حذف مالک
    /interval <عدد>     تغییر فاصله (۱ تا ۱۰۰۰ ثانیه) — مثال: /interval 300
    /interval <n>m      تغییر فاصله (دقیقه) — مثال: /interval 5m
    /send               ارسال فوری به همهٔ گروه‌ها
    /texts              لیست متن‌های گروه‌ها با شماره
    /settext <n> <متن>  ویرایش متن شماره n
    /settext <n>        با ریپلای روی یک پیام، همان متن جایگزین می‌شود
    /addtext <متن>      افزودن متن جدید (یا با ریپلای)
    /deltext <n>        حذف متن شماره n
    /starttexts         لیست متن‌های خوش‌آمد
    /setstart <n> <متن> ویرایش متن خوش‌آمد شماره n (یا با ریپلای)
    /addstart <متن>     افزودن متن خوش‌آمد
    /delstart <n>       حذف متن خوش‌آمد شماره n

چند مالک: هر تعداد نفر می‌توانند هم‌زمان به ربات دسترسی کامل داشته باشند.

نکته: بعد از اولین اجرا، فایل sender_data.json مرجع است. اگر خواستی مقادیر
بالای فایل دوباره اعمال شوند، RESET_ON_START را True کن و یک‌بار اجرا کن.
"""

import asyncio
import json
import logging
import random
import time
from pathlib import Path

from telegram import Update
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes

# ======================= تنظیمات اولیه =======================
TOKEN = "8602669633:AAFaCBzjMOYfTeTHdBOanuGO55AUtYu5nBQ"

# اگر True شود، در اجرای بعدی فایل ذخیره‌شده نادیده گرفته می‌شود
RESET_ON_START = False

# مقادیر اولیه — بعد از اولین اجرا از داخل بات هم قابل تغییرند
GROUP_IDS = [
    # -1001234567890,
]

TEXTS = [
    "سلام عشق‌های من ❤️\nاگه فیلم رایگان وطنی می‌خوای، بیا پیوی 😍",
    "فیلم وطنی رایگان می‌خوای؟ 🎬🔥\nهمه‌ش اینجاست، فقط کافیه پیوی بیای 😍",
    "بچه‌ها خبر خوب! 🎉\nفیلم‌های وطنی بدون پول و بدون تبلیغ اضافه 😎",
    "دنبال فیلم ایرانی می‌گردی؟ 🍿\nبیا پیوی، لینک اختصاصی خودت رو بگیر ❤️",
    "هر چی فیلم وطنی بخوای، اینجا هست 😍📽\nرایگان، سریع، بدون دردسر",
    "عشق‌های من، وقت فیلم دیدنِ! 🎥❤️\nفیلم رایگان وطنی فقط با یه پیام به من",
]

START_TEXTS = [
    "سلام {name} جان ❤️\nخوش اومدی! اینجا کلی فیلم رایگان وطنی منتظرته 😍\nهر وقت خواستی بگو، برات می‌فرستم.",
    "درود {name} 🙂\nبه ربات ما خوش اومدی!\nفیلم‌های وطنی رایگان، همین‌جا در دسترسه ❤️",
    "{name} عزیز، سلام 🌹\nخوشحالم که شروع کردی!\nفقط کافیه پیام بدی تا فیلم‌های رایگان رو بگیری 😊",
]

INTERVAL_SECONDS = 300   # ۵ دقیقه
SEND_ON_START = False
OWNER_ID = 7377638195            # ۰ = اولین کسی که /start بزند مالک می‌شود
# =============================================================

DATA_FILE = Path(__file__).with_name("sender_data.json")
MIN_INTERVAL = 1        # حداقل ۱ ثانیه
MAX_INTERVAL = 1000     # حداکثر ۱۰۰۰ ثانیه

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("group_sender")

DEFAULTS = {
    "owners": [int(OWNER_ID)] if OWNER_ID else [],
    "interval": INTERVAL_SECONDS,
    "groups": [int(g) for g in GROUP_IDS],
    "texts": list(TEXTS),
    "start_texts": list(START_TEXTS),
}

STATE = {}
_last_text_by_chat = {}
_next_run = 0.0


# ======================= ذخیره‌سازی =======================


def save_state():
    try:
        tmp = DATA_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
        tmp.replace(DATA_FILE)
    except OSError as err:
        log.error("ذخیرهٔ تنظیمات ناموفق بود: %s", err)


def load_state():
    global STATE

    if RESET_ON_START and DATA_FILE.exists():
        try:
            DATA_FILE.unlink()
            log.warning("RESET_ON_START فعال بود؛ فایل ذخیره‌شده حذف شد.")
        except OSError as err:
            log.error("حذف فایل ذخیره‌شده ناموفق: %s", err)

    if DATA_FILE.exists():
        try:
            with DATA_FILE.open("r", encoding="utf-8") as f:
                saved = json.load(f)
            STATE = {**DEFAULTS, **saved}
            STATE["groups"] = [int(g) for g in STATE["groups"]]
            STATE["interval"] = min(MAX_INTERVAL, max(MIN_INTERVAL, int(STATE["interval"])))

            # سازگاری با نسخهٔ قدیمی که فقط یک owner_id داشت
            migrated = False
            if "owners" not in saved:
                legacy = int(saved.get("owner_id") or 0)
                STATE["owners"] = [legacy] if legacy else []
                migrated = True
            STATE["owners"] = [int(o) for o in STATE["owners"] if int(o)]
            STATE.pop("owner_id", None)

            if migrated:
                save_state()

            log.info("تنظیمات از %s خوانده شد.", DATA_FILE.name)
            return
        except (OSError, ValueError) as err:
            log.error("خواندن %s ناموفق بود؛ مقادیر پیش‌فرض اعمال شد: %s",
                      DATA_FILE.name, err)

    STATE = {
        "owners": list(DEFAULTS["owners"]),
        "interval": DEFAULTS["interval"],
        "groups": list(DEFAULTS["groups"]),
        "texts": list(DEFAULTS["texts"]),
        "start_texts": list(DEFAULTS["start_texts"]),
    }
    save_state()


# ======================= ارسال =======================


def pick_text_for(chat_id):
    texts = STATE["texts"]
    if not texts:
        return None
    if len(texts) == 1:
        return texts[0]

    last = _last_text_by_chat.get(chat_id, -1)
    choices = [i for i in range(len(texts)) if i != last]
    index = random.choice(choices)
    _last_text_by_chat[chat_id] = index
    return texts[index]


async def send_round(app):
    groups = STATE["groups"]
    if not groups:
        log.warning("هیچ گروهی تنظیم نشده.")
        return 0, 0
    if not STATE["texts"]:
        log.warning("هیچ متنی برای ارسال وجود ندارد.")
        return 0, 0

    sent = 0
    failed = 0

    for chat_id in list(groups):
        text = pick_text_for(chat_id)
        try:
            await app.bot.send_message(chat_id=chat_id, text=text)
            sent += 1
        except RetryAfter as err:
            failed += 1
            log.warning("گروه %s: محدودیت ارسال، %s ثانیه.", chat_id, err.retry_after)
        except Forbidden:
            failed += 1
            log.warning("گروه %s: ربات عضو نیست یا اخراج شده.", chat_id)
        except TelegramError as err:
            failed += 1
            log.warning("گروه %s: %s", chat_id, err)

    log.info("دور ارسال → موفق: %s | ناموفق: %s", sent, failed)
    return sent, failed


async def auto_loop(app):
    global _next_run

    if SEND_ON_START:
        await send_round(app)

    _next_run = time.time() + STATE["interval"]

    while True:
        await asyncio.sleep(1)
        if time.time() < _next_run:
            continue
        try:
            await send_round(app)
        except Exception as err:  # حلقه نباید هیچ‌وقت بمیرد
            log.exception("خطای غیرمنتظره در دور ارسال: %s", err)
        _next_run = time.time() + STATE["interval"]


async def post_init(app):
    app.bot_data["job"] = asyncio.create_task(auto_loop(app))
    log.info("ربات روشن شد | گروه‌ها: %s | فاصله: %s ثانیه",
             len(STATE["groups"]), STATE["interval"])


# ======================= ابزارها =======================


def is_owner(update):
    user = update.effective_user
    return bool(user) and user.id in STATE.get("owners", [])


async def deny(update):
    await update.effective_message.reply_text("⛔️ این دستور فقط برای مالک ربات است.")


def split_cmd(message, maxsplit):
    """متن دستور را می‌شکند و بقیهٔ متن را دست‌نخورده نگه می‌دارد."""
    return message.text.split(None, maxsplit)


def numbered(items):
    return "\n".join(f"{i + 1}. {item}" for i, item in enumerate(items))


def replied_text(message):
    """اگر پیام ریپلای باشد، متن پیام ریپلای‌شده را برمی‌گرداند."""
    reply = getattr(message, "reply_to_message", None)
    if reply is None or not getattr(reply, "text", None):
        return None
    return reply.text


def parse_interval(raw):
    raw = raw.strip().lower()
    if raw.endswith("m"):
        return int(float(raw[:-1]) * 60)
    if raw.endswith("s"):
        return int(float(raw[:-1]))
    return int(float(raw))


# ======================= دستورها =======================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat is not None and chat.type != "private":
        return

    user = update.effective_user
    name = user.first_name if user and user.first_name else "دوست"

    if not STATE.get("owners") and user:
        STATE["owners"].append(user.id)
        save_state()
        log.info("مالک ربات ثبت شد: %s", user.id)
        await update.effective_message.reply_text(
            "👑 تو مالک این ربات شدی.\n"
            f"شناسهٔ تو: `{user.id}`\n\n"
            "می‌توانی با /addowner مالک دیگری هم اضافه کنی.\n"
            "برای راهنما /help را بزن.",
            parse_mode="Markdown",
        )

    if STATE["start_texts"]:
        await update.effective_message.reply_text(
            random.choice(STATE["start_texts"]).format(name=name)
        )

    if is_owner(update):
        await update.effective_message.reply_text(
            "⚙️ وضعیت فعلی:\n"
            f"• گروه‌ها: {len(STATE['groups'])}\n"
            f"• متن‌ها: {len(STATE['texts'])}\n"
            f"• متن‌های خوش‌آمد: {len(STATE['start_texts'])}\n"
            f"• مالک‌ها: {len(STATE['owners'])}\n"
            f"• فاصلهٔ ارسال: هر {STATE['interval']} ثانیه\n\n"
            "/help — لیست دستورها"
        )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    await update.effective_message.reply_text(
        "📋 دستورهای مدیریت\n\n"
        "🗂 گروه‌ها\n"
        "/groups — لیست گروه‌ها\n"
        "/addgroup <id> — افزودن گروه\n"
        "/delgroup <id> — حذف گروه\n"
        "/id — شناسهٔ همین چت\n\n"
        "👑 مالک‌ها\n"
        "/owners — لیست مالک‌ها\n"
        "/addowner <id> — افزودن مالک (یا ریپلای روی پیام طرف)\n"
        "/delowner <id> — حذف مالک\n\n"
        "⏱ زمان  (۱ تا ۱۰۰۰ ثانیه)\n"
        "/interval 300 — هر ۳۰۰ ثانیه\n"
        "/interval 1 — هر ۱ ثانیه\n"
        "/interval 5m — هر ۵ دقیقه\n\n"
        "📝 متن گروه‌ها\n"
        "/texts — لیست متن‌ها با شماره\n"
        "/settext <شماره> <متن> — ویرایش\n"
        "/settext <شماره> (با ریپلای) — ویرایش از روی پیام\n"
        "/addtext <متن> — افزودن\n"
        "/deltext <شماره> — حذف\n\n"
        "👋 متن خوش‌آمد\n"
        "/starttexts — لیست\n"
        "/setstart <شماره> <متن> — ویرایش\n"
        "/addstart <متن> — افزودن\n"
        "/delstart <شماره> — حذف\n\n"
        "🚀 /send — ارسال فوری به همهٔ گروه‌ها"
    )


async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.effective_message.reply_text(
        f"شناسهٔ این چت:\n`{chat.id}`", parse_mode="Markdown"
    )


async def cmd_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)
    if not STATE["groups"]:
        await update.effective_message.reply_text(
            "هیچ گروهی ثبت نشده.\nمثال: /addgroup -1001234567890"
        )
        return
    await update.effective_message.reply_text(
        "🗂 گروه‌های ثبت‌شده:\n" + "\n".join(f"• {g}" for g in STATE["groups"])
    )


async def cmd_addgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    if not context.args:
        await update.effective_message.reply_text("مثال: /addgroup -1001234567890")
        return

    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("شناسه باید عدد باشد (مثلاً -1001234567890).")
        return

    if chat_id in STATE["groups"]:
        await update.effective_message.reply_text("این گروه از قبل ثبت شده بود.")
        return

    STATE["groups"].append(chat_id)
    save_state()

    try:
        chat = await context.bot.get_chat(chat_id)
        title = f"\nنام: {chat.title}"
    except TelegramError:
        title = "\n⚠️ نتوانستم به گروه دسترسی بگیرم؛ مطمئن شو ربات عضو گروه است."

    await update.effective_message.reply_text(f"✅ گروه اضافه شد: {chat_id}{title}")


async def cmd_delgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    if not context.args:
        await update.effective_message.reply_text("مثال: /delgroup -1001234567890")
        return

    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("شناسه باید عدد باشد.")
        return

    if chat_id not in STATE["groups"]:
        await update.effective_message.reply_text("این گروه در لیست نبود.")
        return

    STATE["groups"].remove(chat_id)
    save_state()
    await update.effective_message.reply_text(f"🗑 گروه حذف شد: {chat_id}")


async def cmd_owners(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)
    if not STATE["owners"]:
        await update.effective_message.reply_text("هنوز مالکی ثبت نشده.")
        return
    await update.effective_message.reply_text(
        "👑 مالک‌های ربات:\n" + "\n".join(f"• {o}" for o in STATE["owners"])
    )


async def cmd_addowner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    user_id = None

    if context.args:
        try:
            user_id = int(context.args[0])
        except ValueError:
            await message.reply_text("شناسه باید عدد باشد — یا روی پیام طرف ریپلای کن.")
            return
    else:
        reply = getattr(message, "reply_to_message", None)
        if reply is not None and reply.from_user is not None:
            user_id = reply.from_user.id

    if not user_id:
        await message.reply_text(
            "مثال: /addowner 123456789\n"
            "یا روی پیام طرف ریپلای کن و /addowner بزن."
        )
        return

    if user_id in STATE["owners"]:
        await message.reply_text("این کاربر از قبل مالک است.")
        return

    STATE["owners"].append(user_id)
    save_state()
    await message.reply_text(f"✅ مالک جدید اضافه شد: {user_id}")


async def cmd_delowner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    user_id = None

    if context.args:
        try:
            user_id = int(context.args[0])
        except ValueError:
            await message.reply_text("شناسه باید عدد باشد — یا روی پیام طرف ریپلای کن.")
            return
    else:
        reply = getattr(message, "reply_to_message", None)
        if reply is not None and reply.from_user is not None:
            user_id = reply.from_user.id

    if not user_id:
        await message.reply_text("مثال: /delowner 123456789")
        return

    if user_id not in STATE["owners"]:
        await message.reply_text("این کاربر مالک نیست.")
        return

    if len(STATE["owners"]) == 1:
        await message.reply_text("آخرین مالک را نمی‌شود حذف کرد.")
        return

    STATE["owners"].remove(user_id)
    save_state()
    await message.reply_text(f"🗑 مالک حذف شد: {user_id}")


async def cmd_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _next_run

    if not is_owner(update):
        return await deny(update)

    if not context.args:
        minutes = STATE["interval"] / 60
        await update.effective_message.reply_text(
            f"فاصلهٔ فعلی: {STATE['interval']} ثانیه (≈ {minutes:.1f} دقیقه)\n"
            "مثال: /interval 300 یا /interval 5m"
        )
        return

    try:
        seconds = parse_interval(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("عدد نامعتبر. مثال: /interval 300 یا /interval 5m")
        return

    if seconds < MIN_INTERVAL or seconds > MAX_INTERVAL:
        await update.effective_message.reply_text(
            f"فاصله باید بین {MIN_INTERVAL} و {MAX_INTERVAL} ثانیه باشد."
        )
        return

    STATE["interval"] = seconds
    save_state()
    _next_run = time.time() + seconds

    if seconds >= 60:
        human = f"هر {seconds} ثانیه (≈ {seconds / 60:.1f} دقیقه)"
    else:
        human = f"هر {seconds} ثانیه"

    warning = "\n⚠️ کمتر از ۵ ثانیه احتمال محدودشدن توسط تلگرام را زیاد می‌کند." if seconds < 5 else ""
    await update.effective_message.reply_text(f"⏱ فاصله تغییر کرد: {human}{warning}")


async def cmd_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)
    if not STATE["texts"]:
        await update.effective_message.reply_text("لیست متن‌ها خالی است.")
        return
    await update.effective_message.reply_text("📝 متن‌های گروه:\n" + numbered(STATE["texts"]))


async def cmd_settext(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    parts = split_cmd(message, 2)

    if len(parts) < 2:
        await message.reply_text(
            "مثال: /settext 2 متن جدید\n"
            "یا روی پیام دلخواه ریپلای کن و /settext 2 بزن."
        )
        return

    try:
        index = int(parts[1]) - 1
    except ValueError:
        await message.reply_text("شمارهٔ متن نامعتبر است.")
        return

    if not 0 <= index < len(STATE["texts"]):
        await message.reply_text(f"شماره باید بین ۱ و {len(STATE['texts'])} باشد.")
        return

    new_text = parts[2].strip() if len(parts) >= 3 else None
    if not new_text:
        new_text = replied_text(message)

    if not new_text:
        await message.reply_text(
            f"متن فعلی شمارهٔ {index + 1}:\n\n{STATE['texts'][index]}\n\n"
            "برای تغییر: /settext 2 متن جدید\n"
            "یا روی یک پیام ریپلای کن و /settext 2 بزن."
        )
        return

    STATE["texts"][index] = new_text
    save_state()
    await message.reply_text(f"✅ متن {index + 1} ویرایش شد.")


async def cmd_addtext(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    parts = split_cmd(message, 1)
    new_text = parts[1].strip() if len(parts) >= 2 else None
    if not new_text:
        new_text = replied_text(message)

    if not new_text:
        await message.reply_text("مثال: /addtext متن جدید — یا روی پیام ریپلای کن.")
        return

    STATE["texts"].append(new_text)
    save_state()
    await message.reply_text(
        f"✅ متن اضافه شد. تعداد متن‌ها: {len(STATE['texts'])}"
    )


async def cmd_deltext(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    if not context.args:
        await update.effective_message.reply_text("مثال: /deltext 3")
        return

    try:
        index = int(context.args[0]) - 1
    except ValueError:
        await update.effective_message.reply_text("شماره نامعتبر است.")
        return

    if not 0 <= index < len(STATE["texts"]):
        await update.effective_message.reply_text(
            f"شماره باید بین ۱ و {len(STATE['texts'])} باشد."
        )
        return

    removed = STATE["texts"].pop(index)
    save_state()
    await update.effective_message.reply_text(f"🗑 حذف شد: {removed[:40]}")


async def cmd_starttexts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)
    if not STATE["start_texts"]:
        await update.effective_message.reply_text("لیست خوش‌آمد خالی است.")
        return
    await update.effective_message.reply_text(
        "👋 متن‌های خوش‌آمد:\n" + numbered(STATE["start_texts"])
    )


async def cmd_setstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    parts = split_cmd(message, 2)

    if len(parts) < 2:
        await message.reply_text(
            "مثال: /setstart 1 سلام {name} خوش اومدی\n"
            "یا روی پیام دلخواه ریپلای کن و /setstart 1 بزن."
        )
        return

    try:
        index = int(parts[1]) - 1
    except ValueError:
        await message.reply_text("شماره نامعتبر است.")
        return

    if not 0 <= index < len(STATE["start_texts"]):
        await message.reply_text(
            f"شماره باید بین ۱ و {len(STATE['start_texts'])} باشد."
        )
        return

    new_text = parts[2].strip() if len(parts) >= 3 else None
    if not new_text:
        new_text = replied_text(message)

    if not new_text:
        await message.reply_text(
            f"متن خوش‌آمد شمارهٔ {index + 1}:\n\n{STATE['start_texts'][index]}\n\n"
            "برای تغییر: /setstart 1 متن جدید\n"
            "یا روی یک پیام ریپلای کن و /setstart 1 بزن."
        )
        return

    STATE["start_texts"][index] = new_text
    save_state()
    await message.reply_text(f"✅ متن خوش‌آمد {index + 1} ویرایش شد.")


async def cmd_addstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    message = update.effective_message
    parts = split_cmd(message, 1)
    new_text = parts[1].strip() if len(parts) >= 2 else None
    if not new_text:
        new_text = replied_text(message)

    if not new_text:
        await message.reply_text("مثال: /addstart سلام {name} خوش اومدی — یا روی پیام ریپلای کن.")
        return

    STATE["start_texts"].append(new_text)
    save_state()
    await message.reply_text(
        f"✅ خوش‌آمد اضافه شد. تعداد: {len(STATE['start_texts'])}"
    )


async def cmd_delstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    if not context.args:
        await update.effective_message.reply_text("مثال: /delstart 2")
        return

    try:
        index = int(context.args[0]) - 1
    except ValueError:
        await update.effective_message.reply_text("شماره نامعتبر است.")
        return

    if not 0 <= index < len(STATE["start_texts"]):
        await update.effective_message.reply_text(
            f"شماره باید بین ۱ و {len(STATE['start_texts'])} باشد."
        )
        return

    removed = STATE["start_texts"].pop(index)
    save_state()
    await update.effective_message.reply_text(f"🗑 حذف شد: {removed[:40]}")


async def cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return await deny(update)

    await update.effective_message.reply_text("در حال ارسال...")
    sent, failed = await send_round(context.application)
    await update.effective_message.reply_text(f"✅ موفق: {sent} | ❌ ناموفق: {failed}")


def _disable_job_queue():
    """JobQueue را غیرفعال می‌کند تا روی پایتون‌هایی که apscheduler ناسازگار دارند کار کند.

    PTB موقع ساخت builder یک JobQueue می‌سازد؛ در پایتون ۳.۱۵ با apscheduler 3.6.3
    این کار خطا می‌دهد، هرچند ما اصلاً از job queue استفاده نمی‌کنیم. با جایگزین‌کردن
    کلاس، builder فکر می‌کند پکیج اضافی نصب نیست و job_queue=None می‌گذارد.
    """
    try:
        import telegram.ext._applicationbuilder as builder_module
    except ImportError:
        return

    class _NoJobQueue:
        def __init__(self, *args, **kwargs):
            raise RuntimeError(
                'PTB must be installed via `pip install "python-telegram-bot[job-queue]"`.'
            )

    builder_module.JobQueue = _NoJobQueue


def build_application():
    try:
        return Application.builder().token(TOKEN).post_init(post_init).build()
    except (RuntimeError, TypeError) as err:
        log.warning("ساخت JobQueue ناموفق بود (%s)؛ بدون job_queue ادامه می‌دهیم.", err)
        _disable_job_queue()
        return Application.builder().token(TOKEN).post_init(post_init).build()


def main():
    if TOKEN == "TOKEN_HERE":
        raise SystemExit("اول TOKEN را در بالای فایل بگذار.")

    load_state()

    app = build_application()

    for name, handler in (
        ("start", cmd_start),
        ("help", cmd_help),
        ("id", cmd_id),
        ("groups", cmd_groups),
        ("addgroup", cmd_addgroup),
        ("delgroup", cmd_delgroup),
        ("owners", cmd_owners),
        ("addowner", cmd_addowner),
        ("delowner", cmd_delowner),
        ("interval", cmd_interval),
        ("texts", cmd_texts),
        ("settext", cmd_settext),
        ("edittext", cmd_settext),
        ("addtext", cmd_addtext),
        ("deltext", cmd_deltext),
        ("starttexts", cmd_starttexts),
        ("setstart", cmd_setstart),
        ("editstart", cmd_setstart),
        ("addstart", cmd_addstart),
        ("delstart", cmd_delstart),
        ("send", cmd_send),
    ):
        app.add_handler(CommandHandler(name, handler))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
