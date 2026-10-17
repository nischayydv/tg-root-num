# main.py — TG + Vehicle Lookup API (FastAPI + Telethon)
# Endpoints:
#   GET /health
#   GET /tg        ?number=<phone|@user|link>&key=...   -> plain text
#   GET /tg/json   ?number=<phone|@user|link>&key=...   -> JSON
#   GET /veh       ?vehicle=<reg_no>&key=...            -> pretty JSON
#   GET /veh/json  ?vehicle=<reg_no>&key=...            -> JSON (sponsors stripped)
#
# Concurrency model:
#   * One global NewMessage handler on TARGET_BOT routes every reply to the
#     matching in-flight request via the query echo ("/veh X" or `"query":"X"`).
#   * Duplicate in-flight queries share one send + one future.
#   * Fully safe for many simultaneous users.

from __future__ import annotations

import asyncio
import json as _json
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
from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession

# ───────────────────────── logging ─────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lookup-api")
logging.getLogger("telethon").setLevel(logging.WARNING)


# ───────────────────────── config ──────────────────────────
def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        log.critical("Missing required env var: %s", name)
        sys.exit(1)
    return v

API_ID             = int(_require("API_ID"))
API_HASH           = _require("API_HASH")
SESSION_STR        = _require("SESSION_STRING")
TARGET_BOT         = os.getenv("TARGET_BOT", "RAJFFLIVEBOT").lstrip("@")
TG_COMMAND         = os.getenv("TG_COMMAND", "/tg").strip() or "/tg"
VEH_COMMAND        = os.getenv("BOT_COMMAND", "/veh").strip() or "/veh"
API_KEY            = os.getenv("API_KEY", "").strip()
REQUEST_TIMEOUT    = float(os.getenv("REQUEST_TIMEOUT", "60"))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "8"))
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


# ══════════════════════ in-flight routing ═══════════════════
# _inflight[key] = asyncio.Future waiting for the bot reply
# key = f"{command}:{query}"
_inflight: dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_last_partial: dict[str, str] = {}

TG_RESULT_MARKERS  = ("telegram id", "ᴛᴇʟᴇɢʀᴀᴍ ɪᴅ", "user info")
SEARCHING_HINTS    = ("searching", "sᴇᴀʀᴄʜᴇᴅ", "ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ", "please wait")

CMD_ECHO_RE = re.compile(
    r"/(?:veh|rc|vehicle|vahan|tg)\s+(\S+)", re.IGNORECASE
)


def _find_query_in_reply(txt: str) -> Optional[str]:
    """Extract the query string the bot echoed in its reply."""
    payload = _extract_json_payload(txt)
    if payload and isinstance(payload.get("query"), str):
        return payload["query"].strip()
    m = CMD_ECHO_RE.search(txt)
    if m:
        return m.group(1).strip("✅").strip(",").strip()
    return None


def _is_final_reply(txt: str) -> bool:
    """True when this reply is the actual result (not a searching progress)."""
    if "{" in txt and _extract_json_payload(txt) is not None:
        return True
    low = txt.lower()
    if any(m in low for m in TG_RESULT_MARKERS):
        return True
    return False


async def _on_bot_message(event) -> None:
    """Route a bot reply to the correct waiting HTTP request."""
    try:
        txt = (event.message.text or event.message.message or "").strip()
    except Exception:
        return
    if not txt:
        return

    q = _find_query_in_reply(txt)
    final = _is_final_reply(txt)

    if q is not None:
        candidates = [
            f"{VEH_COMMAND}:{q}",
            f"{VEH_COMMAND}:{q.upper()}",
            f"{TG_COMMAND}:{q}",
            f"{TG_COMMAND}:{q.upper()}",
        ]
        if final:
            for key in candidates:
                async with _inflight_lock:
                    fut = _inflight.pop(key, None)
                if fut is not None and not fut.done():
                    fut.set_result(txt)
                    _last_partial.pop(key, None)
                    return
        else:
            for key in candidates:
                if key in _inflight:
                    _last_partial[key] = txt
            return

    # ── Fallback: reply without echo ─────────────────────
    # If exactly one /tg lookup is pending, give it the reply.
    if final:
        async with _inflight_lock:
            tg_keys = [k for k in _inflight if k.startswith(f"{TG_COMMAND}:")]
            if len(tg_keys) == 1:
                key = tg_keys[0]
                fut = _inflight.pop(key, None)
                if fut is not None and not fut.done():
                    fut.set_result(txt)
                    _last_partial.pop(key, None)


async def _dispatch(command: str, query: str, timeout: float) -> str:
    """Send `command query` (once per in-flight key) and await its routed reply."""
    key = f"{command}:{query}"

    async with _inflight_lock:
        existing = _inflight.get(key)
        if existing is not None and not existing.done():
            fut = existing
            should_send = False
        else:
            fut = asyncio.get_running_loop().create_future()
            _inflight[key] = fut
            should_send = True

    if should_send:
        await ensure_client()
        try:
            async with _semaphore:
                await client.send_message(TARGET_BOT, f"{command} {query}")
        except errors.FloodWaitError as e:
            async with _inflight_lock:
                if _inflight.get(key) is fut:
                    del _inflight[key]
            raise HTTPException(429, f"Telegram rate limit. Retry in {e.seconds}s.")
        except Exception as e:
            async with _inflight_lock:
                if _inflight.get(key) is fut:
                    del _inflight[key]
            log.exception("send_message failed")
            raise HTTPException(500, f"Send failed: {type(e).__name__}")

    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        async with _inflight_lock:
            if _inflight.get(key) is fut:
                del _inflight[key]
        partial = _last_partial.pop(key, None)
        if partial:
            return partial
        raise HTTPException(504, f"Bot did not reply for {query!r} within timeout.")


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

    client.add_event_handler(
        _on_bot_message,
        events.NewMessage(chats=TARGET_BOT, incoming=True),
    )

    log.info(
        "Target bot: @%s | tg=%s | veh=%s | concurrency=%d | rate=%d/min",
        TARGET_BOT, TG_COMMAND, VEH_COMMAND, MAX_CONCURRENT, RATE_LIMIT_PER_MIN,
    )
    try:
        yield
    finally:
        log.info("Shutting down Telethon client…")
        try:
            client.remove_event_handler(_on_bot_message)
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            log.exception("Error during disconnect")


app = FastAPI(title="Lookup API", version="3.1.0", lifespan=lifespan)


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


# ═══════════════════ /tg query classifier ═════════════════
PHONE_RE    = re.compile(r"^\+?\d{7,15}$")
USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
LINK_RE     = re.compile(
    r"^(?:https?://)?(?:t(?:elegram)?\.me|telegram\.dog)/(?P<u>[A-Za-z0-9_]{4,32})/?$",
    re.IGNORECASE,
)


def classify_tg_query(raw: str) -> tuple[str, str]:
    s = (raw or "").strip()
    if not s:
        return "invalid", ""
    m = LINK_RE.match(s)
    if m:
        return "username", m.group("u")
    if s.startswith("@"):
        u = s[1:]
        if USERNAME_RE.match(u):
            return "username", u
        return "invalid", s
    clean = re.sub(r"[\s\-()]", "", s)
    if PHONE_RE.match(clean):
        return "phone", clean
    if USERNAME_RE.match(s):
        return "username", s
    return "invalid", s


async def resolve_username(username: str) -> int:
    await ensure_client()
    try:
        entity = await client.get_entity(username)
        return int(entity.id)
    except errors.UsernameNotOccupiedError:
        raise HTTPException(404, f"Username @{username} does not exist.")
    except errors.UsernameInvalidError:
        raise HTTPException(400, f"Username @{username} is invalid.")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Telegram rate limit. Retry in {e.seconds}s.")
    except ValueError:
        raise HTTPException(404, f"Could not resolve @{username}.")
    except Exception as e:
        log.exception("resolve_username failed for %r", username)
        raise HTTPException(500, f"Resolution failed: {type(e).__name__}")


# ═══════════════════ /veh query classifier ═════════════════
STANDARD_VEH_RE = re.compile(r"^[A-Z]{2}[0-9]{1,2}[A-Z]{0,3}[0-9]{1,4}$")
BH_VEH_RE       = re.compile(r"^[0-9]{2}BH[0-9]{4}[A-Z]{0,2}$")
FALLBACK_VEH_RE = re.compile(r"^[A-Z][A-Z0-9]{5,14}$")


def normalize_vehicle(raw: str) -> Optional[str]:
    s = re.sub(r"[\s\-_]", "", raw or "").upper()
    if not s:
        return None
    if STANDARD_VEH_RE.match(s) or BH_VEH_RE.match(s) or FALLBACK_VEH_RE.match(s):
        return s
    return None


# ═══════════════════ JSON / sponsor handling ═══════════════
SPONSOR_KEY_RE = re.compile(
    r"👤|🕵|ᴅᴇᴠ|ʙᴏᴛ|sponsor|powered\s*by|^dev$|^bot$|^developer$|^channel$",
    re.IGNORECASE,
)

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


def _clean_text(v: str) -> str:
    v = FLAG_RE.sub("", v)
    v = EMOJI_RE.sub("", v)
    v = v.replace("`", "").replace("*", "").strip()
    return re.sub(r"\s{2,}", " ", v)


def strip_sponsor(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: strip_sponsor(v)
            for k, v in obj.items()
            if not SPONSOR_KEY_RE.search(str(k))
        }
    if isinstance(obj, list):
        return [strip_sponsor(x) for x in obj]
    return obj


def _extract_json_payload(raw: str) -> Optional[dict]:
    if not raw:
        return None
    start = raw.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    end = -1
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
    if end == -1:
        return None
    try:
        return _json.loads(raw[start:end])
    except _json.JSONDecodeError:
        log.warning("Bot reply had a JSON-like block that failed to parse")
        return None


# ═══════════════════ legacy /tg parser (unchanged) ═════════
DROP_LINE_PATTERNS = [
    re.compile(r"🤖\s*ʙᴏᴛ\s*[—\-–]",        re.IGNORECASE),
    re.compile(r"@" + re.escape(TARGET_BOT), re.IGNORECASE),
    re.compile(r"ʀᴇǫᴜᴇsᴛᴇᴅ\s*ʙʏ",           re.IGNORECASE),
    re.compile(r"searching",                 re.IGNORECASE),
    re.compile(r"sᴇᴀʀᴄʜᴇᴅ",                  re.IGNORECASE),
    re.compile(r"ᴘʟᴇᴀsᴇ\s*ᴡᴀɪᴛ",            re.IGNORECASE),
    re.compile(r"please\s+wait",             re.IGNORECASE),
    re.compile(r"🆔\s*user\s*info\s*🆔",     re.IGNORECASE),
    re.compile(r"🌐\s*location",             re.IGNORECASE),
    re.compile(r"👤\s*ᴅᴇᴠ",                    re.IGNORECASE),
    re.compile(r"🕵",                          re.IGNORECASE),
]

TG_LABEL_MAP = {
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

DASH_CHARS = "—–-−"


def _extract_label(raw_left: str) -> str:
    kept = "".join(
        c for c in raw_left
        if (c.isascii() and c.isalpha()) or c.isspace()
    )
    return " ".join(kept.split()).lower()


def parse_tg_reply(raw: str) -> dict[str, Any]:
    flat: dict[str, str] = {}
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if any(p.search(line) for p in DROP_LINE_PATTERNS):
            continue
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
        key = TG_LABEL_MAP.get(label)
        if not key:
            continue
        flat[key] = _clean_text(right) or "N/A"

    out: dict[str, Any] = {
        "name":        flat.get("name", "N/A"),
        "username":    flat.get("username", "N/A"),
        "telegram_id": flat.get("telegram_id", "N/A"),
    }
    loc: dict[str, str] = {}
    if "country"      in flat: loc["country"]      = flat["country"]
    if "country_code" in flat: loc["country_code"] = flat["country_code"]
    if "phone_number" in flat: loc["phone_number"] = flat["phone_number"]
    if loc:
        out["location"] = loc
    return out


def render_tg_plain(data: dict[str, Any]) -> str:
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


# ═══════════════════ resolvers ═════════════════════════════
async def tg_lookup_one(raw_query: str) -> dict[str, Any]:
    kind, value = classify_tg_query(raw_query)
    if kind == "invalid":
        raise HTTPException(
            400,
            "Invalid query. Use a phone number, @username, t.me link, or Telegram ID.",
        )

    resolved_from: Optional[str] = None
    resolved_to:   Optional[str] = None
    if kind == "username":
        uid = await resolve_username(value)
        resolved_from = f"@{value}"
        resolved_to   = str(uid)
        target = str(uid)
    else:
        target = value

    raw = await _dispatch(TG_COMMAND, target, REQUEST_TIMEOUT)
    return {
        "query":         raw_query,
        "resolved_from": resolved_from,
        "resolved_to":   resolved_to,
        "result":        parse_tg_reply(raw),
    }


async def veh_lookup_one(raw_query: str) -> dict[str, Any]:
    norm = normalize_vehicle(raw_query)
    if norm is None:
        raise HTTPException(
            400,
            f"Invalid vehicle number: {raw_query!r}. "
            "Examples: UP32QP0001, DL01AB1234, 22BH1234A.",
        )

    raw = await _dispatch(VEH_COMMAND, norm, REQUEST_TIMEOUT)
    payload = _extract_json_payload(raw)
    if payload is None:
        raise HTTPException(502, f"Bot reply for {norm} had no JSON payload.")
    body = payload.get("result", payload)
    return strip_sponsor(body)


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
        "name": "TG + Vehicle Lookup API",
        "version": "3.1.0",
        "endpoints": {
            "tg_plain":  "/tg?number=<phone|@user|link>&key=...",
            "tg_json":   "/tg/json?number=<phone|@user|link>&key=...",
            "veh_plain": "/veh?vehicle=<reg_no>&key=...",
            "veh_json":  "/veh/json?vehicle=<reg_no>&key=...",
            "health":    "/health",
        },
        "examples": {
            "tg":  "/tg/json?number=7710199462&key=YOUR_KEY",
            "veh": "/veh/json?vehicle=UP32QP0001&key=YOUR_KEY",
        },
        "concurrency": "Many requests at once are supported — replies are "
                       "routed to each caller by the bot's query echo.",
    }


@app.get("/health")
async def health():
    try:
        await ensure_client()
        connected = client.is_connected()
    except Exception:
        connected = False
    return {"ok": connected, "connected": connected, "inflight": len(_inflight)}


# ─── /tg (legacy behavior preserved) ──────────────────────
@app.get("/tg", response_class=PlainTextResponse)
async def tg_lookup(
    request: Request,
    number: str = Query(..., description="Phone number, @username, or t.me link"),
    key: str = Query("", description="API key if configured"),
):
    await auth_and_limit(request, key)
    data = await tg_lookup_one(number)
    return render_tg_plain(data["result"])


@app.get("/tg/json")
async def tg_lookup_json(
    request: Request,
    number: str = Query(..., description="Phone number, @username, or t.me link"),
    key: str = Query(""),
):
    await auth_and_limit(request, key)
    data = await tg_lookup_one(number)
    return JSONResponse({
        "success":       True,
        "query":         number,
        "resolved_from": data["resolved_from"],
        "resolved_to":   data["resolved_to"],
        "result":        data["result"],
    })


# ─── /veh (full JSON, sponsors stripped) ──────────────────
@app.get("/veh", response_class=PlainTextResponse)
async def veh_lookup(
    request: Request,
    vehicle: str = Query(..., description="Vehicle reg no"),
    key: str = Query("", description="API key if configured"),
):
    await auth_and_limit(request, key)
    body = await veh_lookup_one(vehicle)
    return _json.dumps(body, indent=2, ensure_ascii=False)


@app.get("/veh/json")
async def veh_lookup_json(
    request: Request,
    vehicle: str = Query(..., description="Vehicle reg no"),
    key: str = Query(""),
):
    await auth_and_limit(request, key)
    body = await veh_lookup_one(vehicle)
    return JSONResponse(body)   # full JSON only — sponsors removed


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
