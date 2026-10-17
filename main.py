# main.py — TG + Vehicle Lookup API (production, Render-ready)
#
# Endpoints:
#   GET /health
#   GET /tg        ?number=<phone|@user|link|id>&key=Nischay   -> plain text
#   GET /tg/json   ?number=<phone|@user|link|id>&key=Nischay   -> JSON
#   GET /veh       ?vehicle=<reg_no>&key=Nischay               -> pretty JSON
#   GET /veh/json  ?vehicle=<reg_no>&key=Nischay               -> JSON
#
# Behaviour:
#   /tg   → resolve @username → numeric ID, then send "/tg <id>" to bot
#   /veh  → SMC Vahan (all details) + bot (mobile) merged, run in parallel
#
# Render start command:
#   uvicorn main:app --host 0.0.0.0 --port $PORT \
#           --proxy-headers --forwarded-allow-ips="*"

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

import httpx
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import ResolveUsernameRequest


# ───────────────────────── logging ─────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lookup-api")
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


# ───────────────────────── config ──────────────────────────
def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        log.critical("Missing required env var: %s", name)
        sys.exit(1)
    return v


API_ID                = int(_require("API_ID"))
API_HASH              = _require("API_HASH")
SESSION_STR           = _require("SESSION_STRING")

TARGET_BOT            = os.getenv("TARGET_BOT", "RAJFFLIVEBOT").lstrip("@")
TG_COMMAND            = os.getenv("TG_COMMAND", "/tg").strip() or "/tg"
VEH_COMMAND           = os.getenv("BOT_COMMAND", "/veh").strip() or "/veh"
API_KEY               = os.getenv("API_KEY", "Nischay").strip() or "Nischay"
REQUEST_TIMEOUT       = float(os.getenv("REQUEST_TIMEOUT", "60"))
MAX_CONCURRENT        = int(os.getenv("MAX_CONCURRENT", "8"))
RATE_LIMIT_PER_MIN    = int(os.getenv("RATE_LIMIT_PER_MIN", "30"))

SMC_URL               = os.getenv(
    "SMC_URL",
    "https://www.smcinsurance.com/central/centralcall/CallReqWithHeader",
)
SMC_TIMEOUT           = float(os.getenv("SMC_TIMEOUT", "30"))
SMC_MAX_CONCURRENT    = int(os.getenv("SMC_MAX_CONCURRENT", "20"))
VEH_CACHE_TTL         = int(os.getenv("VEH_CACHE_TTL", "300"))
VEH_CACHE_TTL_PARTIAL = int(os.getenv("VEH_CACHE_TTL_PARTIAL", "60"))
VEH_CACHE_MAX_ENTRIES = int(os.getenv("VEH_CACHE_MAX_ENTRIES", "5000"))

SMC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 16; CPH2729 Build/BP2A.250605.015) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 "
        "Chrome/150.0.7871.181 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/json",
    "Origin": "https://www.smcinsurance.com",
    "Referer": "https://www.smcinsurance.com/",
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
    "sec-ch-ua-platform": '"Android"',
    "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Android WebView";v="150"',
    "sec-ch-ua-mobile": "?1",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}


# ───────────────────── telethon + concurrency ──────────────
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
_semaphore   = asyncio.Semaphore(MAX_CONCURRENT)   # limits outgoing bot sends
_smc_sem     = asyncio.Semaphore(SMC_MAX_CONCURRENT)


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


# ═══════════════════ shared helpers ════════════════════════
def _norm_key(s: str) -> str:
    """Canonical key for in-flight routing. Strips @, uppercases."""
    return s.strip().lstrip("@").upper()


SPONSOR_KEY_RE = re.compile(
    r"👤|🕵|ᴅᴇᴠ|ʙᴏᴛ|sponsor|powered\s*by|^dev$|^bot$|^developer$|^channel$",
    re.IGNORECASE,
)
FLAG_RE  = re.compile("[\U0001F1E6-\U0001F1FF]{2}")
EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F000-\U0001F0FF"
    "\U0000FE00-\U0000FE0F\U0001F900-\U0001F9FF]+",
    flags=re.UNICODE,
)


def _clean_text(v: str) -> str:
    v = FLAG_RE.sub("", v)
    v = EMOJI_RE.sub("", v)
    v = v.replace("`", "").replace("*", "").strip()
    return re.sub(r"\s{2,}", " ", v)


def strip_sponsor(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: strip_sponsor(v) for k, v in obj.items()
                if not SPONSOR_KEY_RE.search(str(k))}
    if isinstance(obj, list):
        return [strip_sponsor(x) for x in obj]
    return obj


def _extract_json_payload(raw: str) -> Optional[dict]:
    """Return the first valid JSON object embedded in `raw` (brace-matched, string-safe)."""
    if not raw:
        return None
    start = raw.find("{")
    if start == -1:
        return None
    depth = 0; in_str = False; esc = False; end = -1
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_str:
            if esc: esc = False
            elif ch == "\\": esc = True
            elif ch == '"': in_str = False
        else:
            if ch == '"': in_str = True
            elif ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0: end = i + 1; break
    if end == -1:
        return None
    try:
        return _json.loads(raw[start:end])
    except _json.JSONDecodeError:
        return None


def _extract_mobile_from_bot(raw: str) -> Optional[str]:
    payload = _extract_json_payload(raw)
    if not payload:
        return None
    body = payload.get("result", payload)
    if not isinstance(body, dict):
        return None
    owner = body.get("owner_information") or {}
    if not isinstance(owner, dict):
        return None
    m = owner.get("mobile")
    if isinstance(m, str) and m.strip() and m.strip().upper() != "NA":
        return m.strip()
    return None


# ═══════════════════ bot in-flight routing ═════════════════
_inflight: dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_last_partial: dict[str, str] = {}

TG_RESULT_MARKERS = ("telegram id", "ᴛᴇʟᴇɢʀᴀᴍ ɪᴅ", "user info")
CMD_ECHO_RE = re.compile(r"/(?:tg|veh|rc|vehicle|vahan)\s+(\S+)", re.IGNORECASE)


def _find_query_in_reply(txt: str) -> Optional[str]:
    """Return the query the bot echoed in its reply."""
    payload = _extract_json_payload(txt)
    if payload and isinstance(payload.get("query"), str):
        return payload["query"].strip()
    m = CMD_ECHO_RE.search(txt)
    if m:
        return m.group(1).strip("✅").strip(",").strip()
    return None


def _is_final_reply(txt: str) -> bool:
    if "{" in txt and _extract_json_payload(txt) is not None:
        return True
    return any(m in txt.lower() for m in TG_RESULT_MARKERS)


async def _on_bot_message(event) -> None:
    """Route a bot reply to the correct pending HTTP request."""
    try:
        txt = (event.message.text or event.message.message or "").strip()
    except Exception:
        return
    if not txt:
        return

    q = _find_query_in_reply(txt)
    final = _is_final_reply(txt)

    if q is not None:
        q_norm = _norm_key(q)
        keys = [f"{TG_COMMAND}:{q_norm}", f"{VEH_COMMAND}:{q_norm}"]
        if final:
            for key in keys:
                async with _inflight_lock:
                    fut = _inflight.pop(key, None)
                if fut is not None and not fut.done():
                    fut.set_result(txt)
                    _last_partial.pop(key, None)
                    return
        else:
            for key in keys:
                if key in _inflight:
                    _last_partial[key] = txt
            return

    # No echo → if exactly one pending, deliver to it (defensive)
    if final:
        async with _inflight_lock:
            if len(_inflight) == 1:
                key = next(iter(_inflight))
                fut = _inflight.pop(key, None)
                if fut is not None and not fut.done():
                    fut.set_result(txt)
                    _last_partial.pop(key, None)


async def _dispatch_bot(command: str, query: str, timeout: float) -> str:
    """Send '<command> <query>' to bot, await the routed reply."""
    key = f"{command}:{_norm_key(query)}"

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


# ═══════════════════ SMC client + cache ════════════════════
_smc_client: Optional[httpx.AsyncClient] = None
_smc_client_lock = asyncio.Lock()

_veh_cache: dict[str, tuple[float, dict, bool]] = {}
_veh_cache_lock = asyncio.Lock()
_veh_inflight: dict[str, asyncio.Future] = {}


async def _get_smc_client() -> httpx.AsyncClient:
    global _smc_client
    async with _smc_client_lock:
        if _smc_client is None or _smc_client.is_closed:
            _smc_client = httpx.AsyncClient(
                timeout=SMC_TIMEOUT,
                headers=SMC_HEADERS,
                follow_redirects=True,
                limits=httpx.Limits(max_connections=SMC_MAX_CONCURRENT * 2),
            )
        return _smc_client


async def _close_smc_client() -> None:
    global _smc_client
    if _smc_client is not None:
        try:
            await _smc_client.aclose()
        except Exception:
            pass
        _smc_client = None


def _veh_cache_put(norm: str, body: dict, has_mobile: bool) -> None:
    """Insert into cache with size bound."""
    if len(_veh_cache) >= VEH_CACHE_MAX_ENTRIES:
        # drop the oldest entry
        oldest = min(_veh_cache, key=lambda k: _veh_cache[k][0])
        _veh_cache.pop(oldest, None)
    _veh_cache[norm] = (time.monotonic(), body, has_mobile)


# ═══════════════════ /tg parser ════════════════════════════
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
    kept = "".join(c for c in raw_left if (c.isascii() and c.isalpha()) or c.isspace())
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
        left, right = line[:idx], line[idx + 1:].strip()
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
        lines.append(""); lines.append("location:")
        if "country"      in loc: lines.append(f"  country      : {loc['country']}")
        if "country_code" in loc: lines.append(f"  country_code : {loc['country_code']}")
        if "phone_number" in loc: lines.append(f"  phone_number : {loc['phone_number']}")
    return "\n".join(lines)


# ═══════════════════ /tg resolver ══════════════════════════
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
        if u.isdigit():
            return "phone_or_id", u
        return ("username", u) if USERNAME_RE.match(u) else ("invalid", s)
    clean = re.sub(r"[\s\-()]", "", s)
    if PHONE_RE.match(clean):
        return "phone_or_id", clean
    if USERNAME_RE.match(s):
        return "username", s
    return "invalid", s


async def resolve_username(username: str) -> int:
    """Resolve @username → numeric Telegram ID, with 3 fallback methods."""
    await ensure_client()
    username = username.lstrip("@").strip()
    attempts: list[str] = []

    # 1) high-level get_entity
    try:
        entity = await client.get_entity(username)
        return int(entity.id)
    except errors.UsernameNotOccupiedError:
        raise HTTPException(404, f"Username @{username} does not exist on Telegram.")
    except errors.UsernameInvalidError:
        raise HTTPException(400, f"Username @{username} is invalid.")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Telegram rate limit. Retry in {e.seconds}s.")
    except ValueError as e:
        attempts.append(f"get_entity: {e}")
    except Exception as e:
        attempts.append(f"get_entity: {type(e).__name__}: {e}")

    # 2) raw ResolveUsernameRequest
    try:
        res = await client(ResolveUsernameRequest(username))
        if res.users:
            return int(res.users[0].id)
        if res.chats:
            return int(res.chats[0].id)
        peer = getattr(res, "peer", None)
        if peer is not None:
            if getattr(peer, "user_id", None):
                return int(peer.user_id)
            if getattr(peer, "channel_id", None):
                return int(peer.channel_id)
        attempts.append("ResolveUsername: empty result")
    except errors.UsernameNotOccupiedError:
        raise HTTPException(404, f"Username @{username} does not exist on Telegram.")
    except errors.UsernameInvalidError:
        raise HTTPException(400, f"Username @{username} is invalid.")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Telegram rate limit. Retry in {e.seconds}s.")
    except Exception as e:
        attempts.append(f"ResolveUsername: {type(e).__name__}: {e}")

    # 3) get_entity with '@' prefix
    try:
        entity = await client.get_entity("@" + username)
        return int(entity.id)
    except Exception as e:
        attempts.append(f"get_entity('@'): {type(e).__name__}: {e}")

    log.warning("Could not resolve @%s — %s", username, " | ".join(attempts))
    raise HTTPException(
        404,
        f"Could not resolve @{username}. The username may not exist, or your "
        f"SESSION_STRING account cannot see it. Details: {' | '.join(attempts)}",
    )


async def tg_lookup_one(raw_query: str) -> dict[str, Any]:
    kind, value = classify_tg_query(raw_query)
    if kind == "invalid":
        raise HTTPException(400, "Invalid query. Use phone / @username / t.me link / numeric ID.")

    resolved_from: Optional[str] = None
    resolved_to:   Optional[str] = None

    if kind == "username":
        uid = await resolve_username(value)
        resolved_from = f"@{value}"
        resolved_to = str(uid)
        target = str(uid)
        log.info("Resolved %s → %s", resolved_from, resolved_to)
    else:  # phone_or_id
        target = value

    # Bot needs the numeric ID (or phone). Send that.
    raw = await _dispatch_bot(TG_COMMAND, target, REQUEST_TIMEOUT)
    return {
        "query":         raw_query,
        "resolved_from": resolved_from,
        "resolved_to":   resolved_to,
        "result":        parse_tg_reply(raw),
    }


# ═══════════════════ /veh resolver ═════════════════════════
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


async def _call_smc(vehicle: str) -> dict:
    c = await _get_smc_client()
    payload = {"URL": "GetVaahanDetailsByVehicleNo", "Props": [vehicle], "Token": ""}
    async with _smc_sem:
        try:
            r = await c.post(SMC_URL, json=payload)
        except httpx.TimeoutException:
            raise HTTPException(504, "Upstream Vahan API timed out.")
        except httpx.HTTPError as e:
            log.exception("SMC request error")
            raise HTTPException(502, f"Upstream error: {type(e).__name__}")

    if r.status_code != 200:
        raise HTTPException(502, f"Upstream HTTP {r.status_code}")
    try:
        data = r.json()
    except Exception:
        raise HTTPException(502, "Upstream returned non-JSON.")
    if data.get("statusCode") != 200:
        raise HTTPException(404, data.get("message") or "Vehicle not found.")
    body = data.get("response")
    if not body:
        raise HTTPException(404, f"No vehicle data for {vehicle}.")
    return strip_sponsor(body)


async def veh_lookup_one(raw_query: str) -> dict[str, Any]:
    norm = normalize_vehicle(raw_query)
    if norm is None:
        raise HTTPException(400, f"Invalid vehicle number: {raw_query!r}.")

    now = time.monotonic()
    async with _veh_cache_lock:
        hit = _veh_cache.get(norm)
        if hit:
            ts, cached_body, has_mobile = hit
            ttl = VEH_CACHE_TTL if has_mobile else VEH_CACHE_TTL_PARTIAL
            if now - ts < ttl:
                return cached_body

    async with _veh_cache_lock:
        existing = _veh_inflight.get(norm)
        if existing is not None and not existing.done():
            fut = existing
            owner = False
        else:
            fut = asyncio.get_running_loop().create_future()
            _veh_inflight[norm] = fut
            owner = True

    if not owner:
        return await fut

    try:
        smc_task = asyncio.create_task(_call_smc(norm))
        bot_task = asyncio.create_task(_dispatch_bot(VEH_COMMAND, norm, REQUEST_TIMEOUT))
        smc_res, bot_res = await asyncio.gather(smc_task, bot_task, return_exceptions=True)

        if isinstance(smc_res, Exception):
            if isinstance(smc_res, HTTPException):
                raise smc_res
            raise HTTPException(502, f"SMC failed: {type(smc_res).__name__}")

        body = dict(smc_res)
        mobile = _extract_mobile_from_bot(bot_res) if isinstance(bot_res, str) else None
        if mobile:
            body["mobile"] = mobile
        else:
            log.info("No mobile from bot for %s", norm)

        async with _veh_cache_lock:
            _veh_cache_put(norm, body, bool(mobile))
            _veh_inflight.pop(norm, None)
        if not fut.done():
            fut.set_result(body)
        return body

    except Exception as e:
        async with _veh_cache_lock:
            _veh_inflight.pop(norm, None)
        if not fut.done():
            fut.set_exception(e)
        raise


# ═══════════════════ auth + rate limit ═════════════════════
def _client_id(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


_rate_hits: dict[str, deque] = defaultdict(deque)
_rate_lock = asyncio.Lock()
_rate_cleanup_counter = 0


async def check_rate_limit(identifier: str) -> None:
    global _rate_cleanup_counter
    if RATE_LIMIT_PER_MIN <= 0:
        return
    now = time.monotonic()
    async with _rate_lock:
        hits = _rate_hits[identifier]
        while hits and now - hits[0] > 60.0:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_PER_MIN:
            raise HTTPException(429, f"Rate limit exceeded ({RATE_LIMIT_PER_MIN}/min).")
        hits.append(now)

        # periodic stale-entry cleanup
        _rate_cleanup_counter += 1
        if _rate_cleanup_counter >= 1000:
            _rate_cleanup_counter = 0
            for k in [k for k, v in _rate_hits.items() if not v]:
                del _rate_hits[k]


async def auth_and_limit(request: Request, key: str = "") -> str:
    if API_KEY and key != API_KEY:
        raise HTTPException(401, "Unauthorized")
    ident = _client_id(request)
    await check_rate_limit(ident)
    return ident


# ═══════════════════ lifespan ══════════════════════════════
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
        "Ready | bot=@%s tg=%s veh=%s | key=%s | smc=%s | cache=%ds/%ds",
        TARGET_BOT, TG_COMMAND, VEH_COMMAND,
        "set" if API_KEY else "open",
        SMC_URL, VEH_CACHE_TTL, VEH_CACHE_TTL_PARTIAL,
    )
    try:
        yield
    finally:
        log.info("Shutting down…")
        try:
            client.remove_event_handler(_on_bot_message)
        except Exception:
            pass
        await _close_smc_client()
        try:
            await client.disconnect()
        except Exception:
            log.exception("Error during disconnect")


app = FastAPI(title="Lookup API", version="4.2.0", lifespan=lifespan)


# ═══════════════════ routes ════════════════════════════════
@app.get("/", response_class=JSONResponse)
async def root():
    return {
        "name": "TG + Vehicle Lookup API",
        "version": "4.2.0",
        "endpoints": {
            "tg_plain":  "/tg?number=<phone|@user|link|id>&key=Nischay",
            "tg_json":   "/tg/json?number=<phone|@user|link|id>&key=Nischay",
            "veh_plain": "/veh?vehicle=<reg_no>&key=Nischay",
            "veh_json":  "/veh/json?vehicle=<reg_no>&key=Nischay",
            "health":    "/health",
        },
    }


@app.get("/health")
async def health():
    connected = client.is_connected()
    if not connected:
        try:
            await asyncio.wait_for(ensure_client(), timeout=5.0)
            connected = client.is_connected()
        except Exception:
            connected = False
    return {
        "ok": connected,
        "tg_connected": connected,
        "tg_inflight": len(_inflight),
        "veh_cached": len(_veh_cache),
        "veh_inflight": len(_veh_inflight),
    }


@app.get("/tg", response_class=PlainTextResponse)
async def tg_lookup(
    request: Request,
    number: str = Query(..., description="Phone / @username / t.me link / numeric ID"),
    key: str = Query("", description="API key (default: Nischay)"),
):
    await auth_and_limit(request, key)
    data = await tg_lookup_one(number)
    return render_tg_plain(data["result"])


@app.get("/tg/json")
async def tg_lookup_json(
    request: Request,
    number: str = Query(...),
    key: str = Query("", description="API key (default: Nischay)"),
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


@app.get("/veh", response_class=PlainTextResponse)
async def veh_lookup(
    request: Request,
    vehicle: str = Query(..., description="Indian vehicle registration number"),
    key: str = Query("", description="API key (default: Nischay)"),
):
    await auth_and_limit(request, key)
    body = await veh_lookup_one(vehicle)
    return _json.dumps(body, indent=2, ensure_ascii=False)


@app.get("/veh/json")
async def veh_lookup_json(
    request: Request,
    vehicle: str = Query(...),
    key: str = Query("", description="API key (default: Nischay)"),
):
    await auth_and_limit(request, key)
    body = await veh_lookup_one(vehicle)
    return JSONResponse(body)


# ═══════════════════ main ══════════════════════════════════
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
