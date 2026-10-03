# main.py — TG Lookup API (FastAPI + Telethon)
# Deploy on Render as: Web Service → Python
# Endpoints:
#   GET /health
#   GET /tg?number=XXXXXXXXXX&key=YOUR_KEY       -> plain text
#   GET /tg/json?number=XXXXXXXXXX&key=YOUR_KEY  -> JSON

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Optional

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


app = FastAPI(title="TG Lookup API", version="1.0.0", lifespan=lifespan)


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


# ─────────────── branding / header stripper ────────────────
BRAND_PATTERNS = [
    re.compile(r"^\s*🤖\s*ʙᴏᴛ\s*[—\-–]", re.IGNORECASE),
    re.compile(r"^\s*ʙᴏᴛ\s*[—\-–]", re.IGNORECASE),
    re.compile(r"@RAJFFLIVEBOT", re.IGNORECASE),
    re.compile(r"^\s*🆔\s*user\s*info\s*🆔\s*$", re.IGNORECASE),
    re.compile(r"^\s*ʀᴇǫᴜᴇsᴛᴇᴅ\s*ʙʏ\s*[—\-–]\s*.+$", re.IGNORECASE),
]


def strip_branding(text: str) -> str:
    out = []
    for line in text.splitlines():
        if any(p.search(line) for p in BRAND_PATTERNS):
            continue
        out.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(out)).strip()


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
        "version": "1.0.0",
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
    return strip_branding(raw)


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
        "result": strip_branding(raw),
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
