# main.py — TG Lookup API (FastAPI + Telethon)
# Deploy on Render as: Web Service → Python

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any, Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

# ───────────────────────── logging ─────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("tg-api")
logging.getLogger("telethon").setLevel(logging.WARNING)


# ───────────────────────── config ──────────────────────────
def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        log.critical("Missing required env var: %s", name)
        sys.exit(1)
    return v

API_ID       = int(_require("API_ID"))
API_HASH     = _require("API_HASH")
SESSION_STR  = _require("SESSION_STRING")
TARGET_BOT   = os.getenv("TARGET_BOT", "RAJFFLIVEBOT").lstrip("@")
API_KEY      = os.getenv("API_KEY", "").strip()
REQUEST_TIMEOUT    = float(os.getenv("REQUEST_TIMEOUT", "60"))
FIRST_REPLY_WAIT   = float(os.getenv("FIRST_REPLY_WAIT", "20"))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "4"))
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "30"))


# ─────────────────────── telethon client ───────────────────
client = TelegramClient(
    StringSession(SESSION_STR),
    API_ID,
    API_HASH,
    connection_retries=5,
    retry_delay=2,
    auto_reconnect=True,
    request_retries=5,
)

_client_lock = asyncio.Lock()
_semaphore   = asyncio.Semaphore(MAX_CONCURRENT)


async def ensure_client() -> None:
    async with _client_lock:
        if client.is_connected():
            return
        log.warning("Reconnecting Telethon client…")
        await client.connect()
        if not await client.is_user_authorized():
            log.critical("Session is not authorized. Regenerate SESSION_STRING.")
            raise RuntimeError("Telethon session not authorized")
        log.info("Telethon client reconnected.")


# ───────────────────────── lifespan ────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Telethon client…")
    await client.start()
    if not await client.is_user_authorized():
        log.critical("SESSION_STRING is invalid / not authorized.")
        sys.exit(1)
    me = await client.get_me()
    log.info("Logged in as %s (id=%s)", me.username or me.first_name, me.id)
    log.info("Target bot: @%s | concurrency=%d | rate=%d/min",
             TARGET_BOT, MAX_CONCURRENT, RATE_LIMIT_PER_MIN)
    try:
        yield
    finally:
        log.info("Shutting down Telethon client…")
        try:
            await client.disconnect()
        except Exception:
            log.exception("Error during disconnect")


app = FastAPI(title="TG Lookup API", version="1.0.1", lifespan=lifespan)


# ─────────────────────── rate limiter ──────────────────────
_rate_hits: dict[str, deque[float]] = defaultdict(deque)
_rate_lock = asyncio.Lock()


async def check_rate_limit(identifier: str) -> None:
    if RATE_LIMIT_PER_MIN <= 0:
        return
    now = time.monotonic()
    window = 60.0
    async with _rate_lock:
        hits = _rate_hits[identifier]
        while hits and now - hits[0] > window:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_PER_MIN:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({RATE_LIMIT_PER_MIN}/min). Try later.",
            )
        hits.append(now)


# ═══════════════════ parser / formatter ═══════════════════
# Converts the raw bot reply into a clean structured dict:
#
# {
#   "name": "N/A",
#   "username": "N/A",
#   "telegram_id": "7710199462",
#   "location": {
#     "country": "India",
#     "country_code": "+91",
#     "phone_number": "8278330781"
#   }
# }

# Lines to drop entirely (branding, requester, section headers, notices)
DROP_LINE_PATTERNS = [
    re.compile(r"🤖\s*ʙᴏᴛ\s*[—\-–]",        re.IGNORECASE),
    re.compile(r"@RAJFFLIVEBOT",             re.IGNORECASE),
    re.compile(r"ʀᴇǫᴜᴇsᴛᴇᴅ\s*ʙʏ",           re.IGNORECASE),
    re.compile(r"searching",                 re.IGNORECASE),
    re.compile(r"ᴘʟᴇᴀsᴇ\s*ᴡᴀɪᴛ",            re.IGNORECASE),
    re.compile(r"please\s+wait",             re.IGNORECASE),
    re.compile(r"🆔\s*user\s*info\s*🆔",     re.IGNORECASE),
    re.compile(r"🌐\s*location",             re.IGNORECASE),
]

# Map ASCII-only label → JSON key
LABEL_MAP = {
    "name":          "name",
    "username":      "username",
    "telegram id":   "telegram_id",
    "telegramid":    "telegram_id",
    "id":            "telegram_id",
    "country":       "country",
    "country code":  "country_code",
    "countrycode":   "country_code",
    "phone number":  "phone_number",
    "phonenumber":   "phone_number",
    "phone":         "phone_number",
}

# Any of these dash characters can separate label from value
DASH_CHARS = "—–-−"

# Emoji / flag stripper for values
FLAG_RE  = re.compile("[\U0001F1E6-\U0001F1FF]{2}")
EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F000-\U0001F0FF"
    "\U0000FE00-\U0000FE0F"
    "\U0001F900-\U0001F9FF"
    "]+",
    flags=re.UNICODE,
)


def _extract_label(raw_left: str) -> str:
    """Keep only ASCII letters + spaces from the left side of the dash."""
    kept = "".join(
        c for c in raw_left
        if (c.isascii() and c.isalpha()) or c.isspace()
    )
    return " ".join(kept.split()).lower()


def _clean_value(v: str) -> str:
    """Strip emoji/flags/markdown, collapse whitespace."""
    v = v.strip()
    v = FLAG_RE.sub("", v)
    v = EMOJI_RE.sub("", v)
    v = v.replace("`", "").replace("*", "").strip()
    v = re.sub(r"\s{2,}", " ", v)
    return v or "N/A"


def parse_bot_reply(raw: str) -> dict[str, Any]:
    """Turn the raw bot text into a structured dict."""
    flat: dict[str, str] = {}

    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # drop branding / header lines
        if any(p.search(line) for p in DROP_LINE_PATTERNS):
            continue

        # find first dash char
        idx = -1
        for i, ch in enumerate(line):
            if ch in DASH_CHARS:
                idx = i
                break
        if idx == -1:
            continue

        left  = line[:idx]
        right = line[idx + 1:].strip()
        if not right:
            continue

        label = _extract_label(left)
        if not label:
            continue

        key = LABEL_MAP.get(label)
        if not key:
            continue

        flat[key] = _clean_value(right)

    # ── build output ───────────────────────────────────────
    out: dict[str, Any] = {
        "name":        flat.get("name", "N/A"),
        "username":    flat.get("username", "N/A"),
        "telegram_id": flat.get("telegram_id", "N/A"),
    }

    location: dict[str, str] = {}
    if "country"      in flat: location["country"]      = flat["country"]
    if "country_code" in flat: location["country_code"] = flat["country_code"]
    if "phone_number" in flat: location["phone_number"] = flat["phone_number"]
    if location:
        out["location"] = location

    return out


def render_plain(data: dict[str, Any]) -> str:
    lines = [
        f"name          : {data.get('name', 'N/A')}",
        f"username      : {data.get('username', 'N/A')}",
        f"telegram_id   : {data.get('telegram_id', 'N/A')}",
    ]
    loc = data.get("location") or {}
    if loc:
        lines.append("")
        lines.append("location:")
        if "country"      in loc: lines.append(f"  country      : {loc['country']}")
        if "country_code" in loc: lines.append(f"  country_code : {loc['country_code']}")
        if "phone_number" in loc: lines.append(f"  phone_number : {loc['phone_number']}")
    return "\n".join(lines)


# ──────────────── result detection ─────────────────────────
RESULT_MARKERS = ("telegram id", "ᴛᴇʟᴇɢʀᴀᴍ ɪᴅ", "user info")
SEARCHING_MARKERS = ("searching", "ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ", "please wait")


def looks_like_result(text: str) -> bool:
    low = text.lower()
    if any(m in low for m in SEARCHING_MARKERS) and not any(
        m in low for m in RESULT_MARKERS
    ):
        return False
    return any(m in low for m in RESULT_MARKERS)


# ──────────────────── bot conversation ─────────────────────
PHONE_RE = re.compile(r"^\+?\d{7,15}$")


async def fetch_from_bot(number: str) -> str:
    await ensure_client()
    try:
        async with client.conversation(TARGET_BOT, timeout=REQUEST_TIMEOUT) as conv:
            await conv.send_message(f"/tg {number}")

            loop = asyncio.get_running_loop()
            deadline = loop.time() + REQUEST_TIMEOUT
            last_text: Optional[str] = None

            while loop.time() < deadline:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(
                        conv.get_response(),
                        timeout=min(FIRST_REPLY_WAIT, remaining),
                    )
                except asyncio.TimeoutError:
                    continue

                txt = (msg.text or msg.message or "").strip()
                if not txt:
                    continue
                last_text = txt

                if looks_like_result(txt):
                    return txt

            if last_text:
                return last_text
            raise HTTPException(504, "Bot did not reply within timeout.")

    except errors.UsernameNotOccupiedError:
        raise HTTPException(502, f"Bot @{TARGET_BOT} not found.")
    except errors.UsernameInvalidError:
        raise HTTPException(502, f"Bot @{TARGET_BOT} invalid.")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Telegram rate limit. Retry in {e.seconds}s.")
    except HTTPException:
        raise
    except asyncio.TimeoutError:
        raise HTTPException(504, "Bot lookup timed out.")
    except Exception as e:
        log.exception("fetch_from_bot failed")
        raise HTTPException(500, f"Lookup failed: {type(e).__name__}")


# ───────────────────── auth dependency ─────────────────────
def _client_id(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def auth_and_limit(request: Request, key: str = "") -> str:
    if API_KEY and key != API_KEY:
        raise HTTPException(401, "Unauthorized")
    ident = _client_id(request)
    await check_rate_limit(ident)
    return ident


# ───────────────────────── routes ──────────────────────────
@app.get("/", response_class=JSONResponse)
async def root():
    return {
        "name": "TG Lookup API",
        "version": "1.0.1",
        "endpoints": {
            "plain": "/tg?number=XXXXXXXXXX&key=YOUR_KEY",
            "json":  "/tg/json?number=XXXXXXXXXX&key=YOUR_KEY",
            "health": "/health",
        },
    }


@app.get("/health")
async def health():
    try:
        await ensure_client()
        connected = client.is_connected()
    except Exception:
        connected = False
    return {"ok": connected, "connected": connected}


@app.get("/tg", response_class=PlainTextResponse)
async def tg_lookup(
    request: Request,
    number: str = Query(..., description="Phone number, e.g. 8168784189"),
    key: str = Query("", description="API key if configured"),
):
    await auth_and_limit(request, key)
    number = re.sub(r"[\s\-()]", "", number or "")
    if not PHONE_RE.match(number):
        raise HTTPException(400, "Invalid phone number. Use 7–15 digits, optional leading +.")
    async with _semaphore:
        raw = await fetch_from_bot(number)
    return render_plain(parse_bot_reply(raw))


@app.get("/tg/json")
async def tg_lookup_json(
    request: Request,
    number: str = Query(...),
    key: str = Query(""),
):
    await auth_and_limit(request, key)
    number = re.sub(r"[\s\-()]", "", number or "")
    if not PHONE_RE.match(number):
        raise HTTPException(400, "Invalid phone number. Use 7–15 digits, optional leading +.")
    try:
        async with _semaphore:
            raw = await fetch_from_bot(number)
    except HTTPException:
        raise

    return JSONResponse({
        "success": True,
        "query": number,
        "result": parse_bot_reply(raw),
    })


# ───────────────────────── main ────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level=LOG_LEVEL.lower(),
        access_log=False,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
