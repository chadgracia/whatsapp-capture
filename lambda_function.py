"""WhatsApp Capture: TimelinesAI webhook -> S3 -> SES compliance forwarder.

Single-file Lambda (Python 3.12, stdlib + boto3). See CLAUDE.md for the
S3 layout, statuses and compliance rules.
"""

import base64
import hashlib
import hmac
import html
import json
import mimetypes
import os
import re
import traceback
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid

FUNCTION_URL = "https://scrqjg5mbppzctgagxbslh7yve0fbzyp.lambda-url.us-east-1.on.aws/"
REGION = "us-east-1"
DAILY_CAP = 400
MAX_EMAIL_BYTES = 9 * 1024 * 1024
MAX_DOWNLOAD_BYTES = 40 * 1024 * 1024
DOWNLOAD_TIMEOUT = 20
PREVIEW_LEN = 140
NOT_ATTACHED = "not attached (size/download)"
STATUSES = ("unsorted", "client", "personal")

_clients = {}


# ---------------------------------------------------------------------------
# AWS clients / env
# ---------------------------------------------------------------------------

def s3():
    if "s3" not in _clients:
        import boto3
        _clients["s3"] = boto3.client("s3", region_name=REGION)
    return _clients["s3"]


def ses():
    if "ses" not in _clients:
        import boto3
        _clients["ses"] = boto3.client("ses", region_name=REGION)
    return _clients["ses"]


def env(name, default=""):
    return (os.environ.get(name) or default).strip()


def forwarding_enabled():
    return env("FORWARDING_ENABLED").lower() == "true"


def key_ok(given, expected):
    if not given or not expected:
        return False
    return hmac.compare_digest(str(given).encode(), str(expected).encode())


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _bucket():
    return env("BUCKET")


def _is_missing(exc):
    code = (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")
    return code in ("NoSuchKey", "404", "NotFound")


def s3_get_bytes(key):
    try:
        return s3().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except Exception as e:
        if _is_missing(e):
            return None
        raise


def s3_get_json(key):
    data = s3_get_bytes(key)
    return None if data is None else json.loads(data)


def s3_put_bytes(key, data, content_type="application/octet-stream", metadata=None):
    kwargs = {"Bucket": _bucket(), "Key": key, "Body": data, "ContentType": content_type}
    if metadata:
        kwargs["Metadata"] = metadata
    s3().put_object(**kwargs)


def s3_put_json(key, obj):
    s3_put_bytes(key, json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8"),
                 "application/json")


def s3_list(prefix):
    token = None
    while True:
        kwargs = {"Bucket": _bucket(), "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3().list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            yield obj
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


def s3_delete_keys(keys):
    keys = list(keys)
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        s3().delete_objects(Bucket=_bucket(),
                            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True})


def safe(value, maxlen=150):
    s = re.sub(r"[^A-Za-z0-9._-]", "_", str(value)).strip(".")[:maxlen]
    return s or "x"


def contact_path(ck):
    return f"contacts/{ck}.json"


# ---------------------------------------------------------------------------
# Payload parsing (TimelinesAI format NOT verified -- defensive search)
# ---------------------------------------------------------------------------

CONTAINER_KEYS = ("data", "payload", "event_data", "body", "result")
MESSAGE_KEYS = ("message", "msg", "last_message", "whatsapp_message")
CHAT_KEYS = ("chat", "conversation", "thread")
CONTACT_KEYS = ("contact", "customer", "participant")
SENDER_KEYS = ("sender", "from", "author")
OUT_WORDS = {"outgoing", "outbound", "out", "sent", "send", "fromme"}
IN_WORDS = {"incoming", "inbound", "in", "received", "receive"}


def _scalar(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (str, int, float)):
        s = str(v).strip()
        return s or None
    return None


def _first(dicts, names):
    for d in dicts:
        for n in names:
            if n in d:
                s = _scalar(d[n])
                if s is not None:
                    return s
    return None


def _sub(dicts, names):
    out, ids = [], set()
    for d in dicts:
        for n in names:
            v = d.get(n)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                v = v[0]
            if isinstance(v, dict) and id(v) not in ids:
                ids.add(id(v))
                out.append(v)
    return out


def _digits(s):
    if not s:
        return ""
    s = str(s)
    if "@" in s:
        s = s.split("@", 1)[0]
    d = re.sub(r"\D", "", s)
    return d if 6 <= len(d) <= 16 else ""


def _jid_digits(v):
    """Phone digits from a WhatsApp JID like 380501234567@s.whatsapp.net (not bare ids)."""
    if not v or "@" not in str(v) or str(v).endswith("@g.us"):
        return ""
    return _digits(v)


def _direction(dicts):
    for d in dicts:
        for n in ("from_me", "fromMe", "is_from_me", "isFromMe", "outgoing", "is_outgoing",
                  "isOutgoing", "is_sent"):
            if isinstance(d.get(n), bool):
                return "OUT" if d[n] else "IN"
    for d in dicts:
        for n in ("direction", "message_direction", "messageDirection", "event_type",
                  "eventType", "event", "type", "kind"):
            v = d.get(n)
            if not isinstance(v, str):
                continue
            tokens = set(re.split(r"[^a-z]+", v.lower().replace("from_me", "fromme")))
            if tokens & OUT_WORDS:
                return "OUT"
            if tokens & IN_WORDS:
                return "IN"
    return "UNKNOWN"


def _is_group(dicts, chat_id):
    for d in dicts:
        for n in ("is_group", "isGroup", "group", "is_group_chat"):
            if isinstance(d.get(n), bool):
                return d[n]
        for n in ("chat_type", "chatType", "type"):
            v = d.get(n)
            if isinstance(v, str) and v.lower() in ("group", "group_chat", "groupchat"):
                return True
    return bool(chat_id and str(chat_id).endswith("@g.us"))


def _parse_ts(v):
    if v is None:
        return None
    try:
        s = str(v).strip()
        if re.fullmatch(r"\d+(\.\d+)?", s):
            f = float(s)
            if f > 1e12:
                f /= 1000.0
            return datetime.fromtimestamp(f, timezone.utc)
        s = s.replace("Z", "+00:00")
        s = re.sub(r"\s([+-]\d{2}):?(\d{2})$", r"\1:\2", s)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


URL_KEYS = ("url", "temporary_download_url", "download_url", "file_url", "media_url",
            "link", "src", "href", "attachment_url")
FILENAME_KEYS = ("filename", "file_name", "fileName", "name", "title", "original_name")


def _attachments(dicts):
    out, seen = [], set()

    def add(url, filename=None):
        if not isinstance(url, str) or not url.lower().startswith(("http://", "https://")):
            return
        if url in seen:
            return
        seen.add(url)
        if not filename:
            filename = urllib.parse.unquote(urllib.parse.urlparse(url).path.rsplit("/", 1)[-1])
        out.append({"url": url, "filename": filename or "attachment"})

    def add_item(item):
        if isinstance(item, str):
            add(item)
        elif isinstance(item, dict):
            add(_first([item], URL_KEYS), _first([item], FILENAME_KEYS))

    for d in dicts:
        for n in ("attachments", "files", "media", "attachment", "file", "documents",
                  "attachment_urls", "media_urls"):
            v = d.get(n)
            if isinstance(v, list):
                for item in v:
                    add_item(item)
            elif v is not None:
                add_item(v)
        for n in ("media_url", "file_url", "attachment_url", "document_url", "image_url"):
            add(d.get(n))
    return out


def parse_payload(body):
    """Return a normalized dict or None if no contact key / message found."""
    if not isinstance(body, dict):
        return None
    tops = [body] + _sub([body], CONTAINER_KEYS)
    tops += [d for d in _sub(tops[1:], CONTAINER_KEYS) if all(d is not t for t in tops)]
    msgs = _sub(tops, MESSAGE_KEYS + ("messages",))
    chats = _sub(msgs + tops, CHAT_KEYS)
    contacts = _sub(chats + msgs + tops, CONTACT_KEYS)
    senders = _sub(msgs + tops, SENDER_KEYS)
    recipients = _sub(msgs + tops, ("recipient", "to"))

    direction = _direction(msgs + tops)

    chat_id = (_first(chats, ("chat_id", "chatId", "id", "jid", "uid", "chat_jid"))
               or _first(msgs + tops, ("chat_id", "chatId", "conversation_id", "chat_jid",
                                       "chat_uid", "remote_jid", "remoteJid")))
    is_group = _is_group(chats + msgs + tops, chat_id)

    phone_names = ("phone", "phone_number", "phoneNumber", "msisdn", "wa_id", "number")
    phone = ""
    if not is_group:
        phone = (_digits(_first(chats, phone_names))
                 or _jid_digits(_first(chats, ("jid", "chat_jid", "id")))
                 or _digits(_first(contacts, phone_names))
                 or _jid_digits(_first(contacts, ("jid", "id")))
                 or _digits(_first(msgs + tops, ("chat_phone", "contact_phone")))
                 or _jid_digits(_first(msgs + tops, ("chat_jid", "remote_jid", "remoteJid")))
                 or _jid_digits(chat_id))
        if not phone and direction == "IN":
            phone = (_digits(_first(senders, phone_names + ("jid",)))
                     or _digits(_first(msgs + tops, ("sender_phone", "from_phone", "from"))))
        if not phone and direction == "OUT":
            phone = (_digits(_first(recipients, phone_names + ("jid",)))
                     or _digits(_first(msgs + tops, ("recipient_phone", "to_phone", "to"))))
        if not phone and direction != "OUT":
            phone = _digits(_first(msgs + tops, ("phone", "phone_number")))

    name = (_first(chats, ("full_name", "name", "title", "display_name", "chat_name", "subject"))
            or _first(contacts, ("full_name", "name", "display_name", "push_name", "pushname"))
            or _first(msgs + tops, ("chat_name", "chat_full_name", "contact_name", "group_name")))

    sender_name = (_first(senders, ("full_name", "name", "display_name", "push_name", "pushname"))
                   or _first(msgs + tops, ("sender_name", "from_name", "author_name",
                                           "sender_full_name")))
    if not sender_name:
        s = _first(msgs, ("sender", "author", "from"))
        if s and not _digits(s):
            sender_name = s
    if not name and not is_group and direction == "IN":
        name = sender_name
    if not sender_name:
        sender_name = "Me" if direction == "OUT" else (name or "")

    text = (_first(msgs, ("text", "body", "message_text", "content", "caption", "message"))
            or _first(tops, ("text", "message_text", "message", "caption", "content")))
    message_id = (_first(msgs, ("message_id", "messageId", "message_uid", "whatsapp_message_id",
                                "wamid", "uid", "id"))
                  or _first(tops, ("message_id", "messageId", "message_uid",
                                   "whatsapp_message_id")))
    ts = _parse_ts(_first(msgs + tops, ("timestamp", "created_at", "createdAt", "sent_at",
                                        "sentAt", "message_timestamp", "time", "date",
                                        "datetime")))
    attachments = _attachments(msgs + tops)

    if is_group:
        contact_key = safe(chat_id) if chat_id else ""
    else:
        contact_key = phone or (safe(chat_id) if chat_id else "")
    if not contact_key or (not text and not attachments):
        return None

    return {
        "contact_key": contact_key,
        "phone": phone,
        "name": name or (f"+{phone}" if phone else contact_key),
        "is_group": bool(is_group),
        "chat_name": name if is_group else "",
        "sender_name": sender_name,
        "direction": direction,
        "text": text or "",
        "message_id": message_id,
        "timestamp": ts,
        "attachments": attachments,
    }


# ---------------------------------------------------------------------------
# Time formatting
# ---------------------------------------------------------------------------

def to_kyiv(dt):
    try:
        from zoneinfo import ZoneInfo
        for n in ("Europe/Kyiv", "Europe/Kiev"):
            try:
                return dt.astimezone(ZoneInfo(n))
            except Exception:
                continue
    except Exception:
        pass
    # Fallback: EET/EEST with EU DST rules (last Sunday Mar/Oct, 01:00 UTC).
    def last_sunday(month):
        d = datetime(dt.year, month + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
        return d - timedelta(days=(d.weekday() - 6) % 7)
    summer = last_sunday(3) <= dt < last_sunday(10)
    return dt.astimezone(timezone(timedelta(hours=3 if summer else 2),
                                  "EEST" if summer else "EET"))


def fmt_times(ts_iso):
    dt = _parse_ts(ts_iso) or now_utc()
    k = to_kyiv(dt)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC"), k.strftime("%Y-%m-%d %H:%M:%S %Z")


# ---------------------------------------------------------------------------
# Daily cap
# ---------------------------------------------------------------------------

def _state_key(day=None):
    return f"state/sent-{(day or now_utc()).strftime('%Y-%m-%d')}.json"


def sent_today():
    st = s3_get_json(_state_key()) or {}
    return int(st.get("count", 0))


def cap_allows_send():
    """True if under the cap. When at/over cap, sends the one-per-day alert."""
    key = _state_key()
    st = s3_get_json(key) or {"count": 0, "alerted": False}
    if int(st.get("count", 0)) < DAILY_CAP:
        return True
    if not st.get("alerted"):
        st["alerted"] = True
        st["alerted_at"] = iso(now_utc())
        s3_put_json(key, st)
        send_email(
            f"WhatsApp Capture: daily forward cap ({DAILY_CAP}) reached",
            f"The daily cap of {DAILY_CAP} forward emails was reached on "
            f"{now_utc().strftime('%Y-%m-%d')} (UTC).\n\n"
            "Forwarding is paused until 00:00 UTC. Client messages received meanwhile are "
            "stored in S3 with forwarded:false. Use 'Forward held' on the admin page to send "
            "them once the cap resets.\n",
            [], cc=False)
    return False


def record_send():
    key = _state_key()
    st = s3_get_json(key) or {"count": 0, "alerted": False}
    st["count"] = int(st.get("count", 0)) + 1
    s3_put_json(key, st)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def _clean_header(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def send_email(subject, body, attachments, cc=True):
    from_addr, to_addr = env("FROM_ADDR"), env("TO_ADDR")
    cc_addrs = [a.strip() for a in env("CC_ADDR").split(",") if a.strip()] if cc else []
    msg = EmailMessage()
    msg["From"] = formataddr(("WhatsApp Capture", from_addr))
    msg["To"] = to_addr
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = _clean_header(subject)
    msg["Date"] = formatdate(usegmt=True)
    msg["Message-ID"] = make_msgid(domain=from_addr.split("@")[-1] if "@" in from_addr else None)
    msg.set_content(body)
    for filename, data, ctype in attachments:
        maintype, _, subtype = (ctype or "application/octet-stream").partition("/")
        msg.add_attachment(data, maintype=maintype or "application",
                           subtype=subtype or "octet-stream", filename=filename)
    ses().send_raw_email(Source=from_addr, Destinations=[to_addr] + cc_addrs,
                         RawMessage={"Data": msg.as_bytes()})


def _phone_label(rec):
    return f"+{rec['phone']}" if rec.get("phone") else "n/a"


def single_subject(rec):
    if rec.get("is_group"):
        return f"WhatsApp group | {rec.get('chat_name') or rec.get('contact_name')} | " \
               f"{rec.get('sender_name') or 'unknown'}"
    phone = f" (+{rec['phone']})" if rec.get("phone") else ""
    return f"WhatsApp | {rec.get('contact_name')}{phone} | {rec.get('direction')}"


def message_block(rec, att_status):
    utc, kyiv = fmt_times(rec.get("timestamp"))
    lines = [
        f"Contact: {rec.get('contact_name')}",
        f"Phone: {_phone_label(rec)}",
        f"Chat: {rec.get('chat_name') if rec.get('is_group') else 'Direct'}",
        f"Sender: {rec.get('sender_name')}",
        f"Direction: {rec.get('direction')}",
        f"Timestamp (UTC): {utc}",
        f"Timestamp (Europe/Kiev): {kyiv}",
        f"Message ID: {rec.get('message_id')}",
        "",
        rec.get("text") or "(no text)",
    ]
    atts = rec.get("attachments") or []
    if atts:
        lines += ["", "Attachments:"]
        for i, a in enumerate(atts):
            status = att_status.get((rec.get("message_id"), i), NOT_ATTACHED)
            if status == "attached":
                lines.append(f"- {a.get('filename')} (attached)")
            else:
                lines.append(f"- {a.get('filename')}: {a.get('url')} -- {status}")
    return "\n".join(lines)


def collect_attachments(recs, cache=None):
    """Load stored attachments from S3 while total email stays under the limit."""
    cache = cache or {}
    parts, status = [], {}
    budget = MAX_EMAIL_BYTES - 64 * 1024 - sum(len(r.get("text") or "") * 2 for r in recs)
    for rec in recs:
        for i, a in enumerate(rec.get("attachments") or []):
            k = a.get("s3_key")
            if not k:
                continue
            size = a.get("size") or 0
            encoded = (size * 4) // 3 + 2048
            if size and encoded > budget:
                continue
            data = cache.get(k)
            if data is None:
                try:
                    data = s3_get_bytes(k)
                except Exception:
                    data = None
            if data is None:
                continue
            encoded = (len(data) * 4) // 3 + 2048
            if encoded > budget:
                continue
            budget -= encoded
            ctype = a.get("content_type") or mimetypes.guess_type(a.get("filename") or "")[0]
            parts.append((a.get("filename") or "attachment", data,
                          ctype or "application/octet-stream"))
            status[(rec.get("message_id"), i)] = "attached"
    return parts, status


def forward_single(rec, msg_key, cache=None):
    if not forwarding_enabled() or not cap_allows_send():
        return False
    parts, status = collect_attachments([rec], cache)
    send_email(single_subject(rec), message_block(rec, status) + "\n", parts)
    record_send()
    rec["forwarded"] = True
    rec["forwarded_at"] = iso(now_utc())
    s3_put_json(msg_key, rec)
    return True


def load_messages(ck):
    out = []
    for obj in s3_list(f"messages/{ck}/"):
        rec = s3_get_json(obj["Key"])
        if rec is not None:
            out.append((obj["Key"], rec))
    out.sort(key=lambda kr: (kr[1].get("timestamp") or "", kr[0]))
    return out


def forward_backlog(ck, contact):
    """Forward every held (forwarded:false) message as ONE email. Returns count sent."""
    held = [(k, r) for k, r in load_messages(ck) if not r.get("forwarded")]
    if not held or not forwarding_enabled() or not cap_allows_send():
        return 0
    recs = [r for _, r in held]
    parts, status = collect_attachments(recs)
    phone = f" (+{contact['phone']})" if contact.get("phone") else ""
    subject = f"WhatsApp backlog | {contact.get('name')}{phone} | {len(recs)} messages"
    sep = "\n\n" + "-" * 60 + "\n\n"
    body = (f"{len(recs)} held messages for {contact.get('name')}{phone}, "
            f"chronological order.{sep}"
            + sep.join(message_block(r, status) for r in recs) + "\n")
    send_email(subject, body, parts)
    record_send()
    at = iso(now_utc())
    for k, r in held:
        r["forwarded"] = True
        r["forwarded_at"] = at
        s3_put_json(k, r)
    return len(recs)


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

def save_unparsed(raw, reason):
    d = now_utc()
    key = f"unparsed/{d.strftime('%Y/%m/%d')}/{uuid.uuid4()}.json"
    s3_put_bytes(key, raw if isinstance(raw, bytes) else str(raw).encode("utf-8"),
                 "application/json", metadata={"reason": _clean_header(reason)[:500]})
    return key


def download(url):
    """Return (bytes, content_type) or raise."""
    req = urllib.request.Request(url, headers={"User-Agent": "whatsapp-capture/1.0"})
    with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as r:
        data = r.read(MAX_DOWNLOAD_BYTES + 1)
        if len(data) > MAX_DOWNLOAD_BYTES:
            raise ValueError("attachment too large")
        ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip()
        return data, ctype


def store_attachments(ck, mid, atts, cache):
    used = set()
    out = []
    for i, a in enumerate(atts):
        fn = safe(a.get("filename") or f"attachment-{i}", 120)
        if fn in used:
            fn = f"{i}_{fn}"
        used.add(fn)
        entry = {"url": a["url"], "filename": a.get("filename") or fn,
                 "s3_key": None, "size": None, "content_type": None, "error": None}
        try:
            data, ctype = download(a["url"])
            ctype = ctype or mimetypes.guess_type(fn)[0] or "application/octet-stream"
            key = f"attachments/{ck}/{mid}/{fn}"
            s3_put_bytes(key, data, ctype)
            cache[key] = data
            entry.update(s3_key=key, size=len(data), content_type=ctype)
        except Exception as e:
            entry["error"] = f"{type(e).__name__}: {e}"[:300]
        out.append(entry)
    return out


def handle_webhook(raw):
    try:
        body = json.loads(raw)
    except Exception:
        save_unparsed(raw, "invalid json")
        return
    msg = parse_payload(body)
    if not msg:
        save_unparsed(raw, "no contact key or message")
        return

    ck = msg["contact_key"]
    mid = safe(msg["message_id"] or ("h" + hashlib.sha256(raw).hexdigest()[:32]))
    seen_key = f"seen/{mid}"
    if s3_get_bytes(seen_key) is not None:
        return

    now = iso(now_utc())
    contact = s3_get_json(contact_path(ck)) or {
        "key": ck, "name": msg["name"], "phone": msg["phone"], "is_group": msg["is_group"],
        "status": "unsorted", "first_seen": now, "last_seen": now,
        "last_preview": "", "msg_count": 0,
    }
    status = contact.get("status") if contact.get("status") in STATUSES else "unsorted"

    if status == "personal":
        contact["last_seen"] = now
        s3_put_json(contact_path(ck), contact)
        return

    ts = msg["timestamp"] or now_utc()
    preview = msg["text"] or ", ".join(a["filename"] for a in msg["attachments"])
    if msg["name"] and msg["name"] != ck:
        contact["name"] = msg["name"]
    if msg["phone"]:
        contact["phone"] = msg["phone"]
    contact["is_group"] = msg["is_group"]
    contact["last_seen"] = now
    contact["last_preview"] = preview[:PREVIEW_LEN]
    contact["msg_count"] = int(contact.get("msg_count", 0)) + 1

    s3_put_bytes(f"raw/{ck}/{mid}.json", raw, "application/json")
    cache = {}
    atts = store_attachments(ck, mid, msg["attachments"], cache)
    rec = {
        "message_id": mid,
        "contact_key": ck,
        "contact_name": contact["name"],
        "phone": msg["phone"] or contact.get("phone", ""),
        "is_group": msg["is_group"],
        "chat_name": msg["chat_name"] or (contact["name"] if msg["is_group"] else ""),
        "sender_name": msg["sender_name"],
        "direction": msg["direction"],
        "timestamp": iso(ts),
        "received_at": now,
        "text": msg["text"],
        "attachments": atts,
        "forwarded": False,
        "forwarded_at": None,
    }
    msg_key = f"messages/{ck}/{ts.strftime('%Y%m%dT%H%M%SZ')}_{mid}.json"
    s3_put_json(msg_key, rec)
    s3_put_json(contact_path(ck), contact)
    s3_put_json(seen_key, {"contact_key": ck, "at": now})

    if status == "client":
        forward_single(rec, msg_key, cache)


# ---------------------------------------------------------------------------
# Admin actions
# ---------------------------------------------------------------------------

def delete_unforwarded(ck):
    """Delete unforwarded messages + their raw/attachments/seen. Forwarded ones are kept."""
    kept, doomed = set(), []
    dead_mids = set()
    for k, r in load_messages(ck):
        mid = r.get("message_id")
        if r.get("forwarded"):
            kept.add(mid)
        else:
            doomed.append(k)
            if mid:
                dead_mids.add(mid)
    for obj in s3_list(f"raw/{ck}/"):
        mid = obj["Key"].rsplit("/", 1)[-1][:-len(".json")]
        if mid not in kept:
            doomed.append(obj["Key"])
            dead_mids.add(mid)
    for obj in s3_list(f"attachments/{ck}/"):
        mid = obj["Key"][len(f"attachments/{ck}/"):].split("/", 1)[0]
        if mid not in kept:
            doomed.append(obj["Key"])
    doomed += [f"seen/{m}" for m in dead_mids]
    s3_delete_keys(doomed)
    return len(kept)


def admin_action(ck, action):
    ck = safe(ck)
    contact = s3_get_json(contact_path(ck))
    if not contact:
        return "Contact not found."
    name = contact.get("name") or ck
    if action == "client":
        contact["status"] = "client"
        contact["sorted_at"] = iso(now_utc())
        s3_put_json(contact_path(ck), contact)
        n = forward_backlog(ck, contact)
        held = sum(1 for _, r in load_messages(ck) if not r.get("forwarded"))
        note = f" {n} held messages forwarded." if n else ""
        if held:
            note += f" {held} still held (forwarding off or daily cap)."
        return f"{name} marked Client.{note}"
    if action == "personal":
        kept = delete_unforwarded(ck)
        contact["status"] = "personal"
        contact["sorted_at"] = iso(now_utc())
        contact["last_preview"] = ""
        contact["msg_count"] = kept
        s3_put_json(contact_path(ck), contact)
        return f"{name} marked Personal. Unforwarded content deleted."
    if action == "forward_held":
        if contact.get("status") != "client":
            return "Only Client contacts can be forwarded."
        n = forward_backlog(ck, contact)
        return f"{n} held messages forwarded for {name}." if n else \
            f"Nothing forwarded for {name} (none held, forwarding off, or daily cap)."
    return "Unknown action."


# ---------------------------------------------------------------------------
# Daily nudge
# ---------------------------------------------------------------------------

def load_contacts():
    out = []
    for obj in s3_list("contacts/"):
        c = s3_get_json(obj["Key"])
        if c:
            out.append(c)
    return out


def daily_nudge():
    unsorted = [c for c in load_contacts() if c.get("status", "unsorted") == "unsorted"]
    if not unsorted:
        return 0
    unsorted.sort(key=lambda c: c.get("last_seen") or "", reverse=True)
    lines = [f"{len(unsorted)} WhatsApp contacts are waiting to be sorted "
             "(held messages are not forwarded until marked Client).", ""]
    for c in unsorted:
        phone = f"+{c['phone']}" if c.get("phone") else "(no phone)"
        group = " [group]" if c.get("is_group") else ""
        lines.append(f"- {c.get('name')}{group} {phone} -- {c.get('msg_count', 0)} msgs")
        if c.get("last_preview"):
            lines.append(f"    \"{c['last_preview']}\"")
    lines += ["", "Sort them here:",
              FUNCTION_URL + "?key=" + urllib.parse.quote(env("ADMIN_KEY"), safe="")]
    send_email(f"WhatsApp Capture: {len(unsorted)} contacts to sort", "\n".join(lines) + "\n",
               [], cc=False)
    return len(unsorted)


# ---------------------------------------------------------------------------
# Admin page
# ---------------------------------------------------------------------------

CSS = """
*{box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
sans-serif;margin:0;background:#f4f5f7;color:#1d2330}main{max-width:820px;margin:0 auto;
padding:16px}h1{font-size:22px;margin:8px 0}h2{font-size:17px;margin:24px 0 8px}
.status{font-size:13px;color:#555;background:#fff;border:1px solid #e1e4e8;border-radius:8px;
padding:8px 12px}.status a{color:#2463eb}.flash{background:#e8f5e9;border:1px solid #b7dfb9;
border-radius:8px;padding:8px 12px;margin:8px 0;font-size:14px}.card{background:#fff;
border:1px solid #e1e4e8;border-radius:8px;padding:10px 12px;margin:8px 0;display:flex;
gap:10px;align-items:flex-start;justify-content:space-between;flex-wrap:wrap}
.info{flex:1 1 300px;min-width:0}.name{font-weight:600}.meta{font-size:12px;color:#666}
.preview{font-size:13px;color:#333;margin-top:4px;overflow-wrap:anywhere}
.badge{display:inline-block;font-size:11px;background:#ede9fe;color:#5b21b6;border-radius:4px;
padding:1px 6px;margin-left:6px}.btns{display:flex;gap:6px;flex-wrap:wrap}
button{border:0;border-radius:6px;padding:8px 12px;font-size:14px;cursor:pointer}
.b-client{background:#2463eb;color:#fff}.b-personal{background:#e5e7eb;color:#111}
.b-held{background:#f59e0b;color:#111}.empty{color:#888;font-size:14px}
pre{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:10px;overflow-x:auto;
font-size:12px;white-space:pre-wrap;overflow-wrap:anywhere}
"""

JS = """
document.addEventListener('submit',function(e){var f=e.target;
if(!confirm(f.getAttribute('data-confirm'))){e.preventDefault();}});
"""


def _e(s):
    return html.escape(str(s if s is not None else ""), quote=True)


def _form(ck, action, label, cls, confirm_text):
    return (f'<form method="POST" action="/" data-confirm="{_e(confirm_text)}">'
            f'<input type="hidden" name="key" value="{_e(env("ADMIN_KEY"))}">'
            f'<input type="hidden" name="contact_key" value="{_e(ck)}">'
            f'<input type="hidden" name="action" value="{_e(action)}">'
            f'<button class="{cls}" type="submit">{_e(label)}</button></form>')


def _page(body):
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta name="robots" content="noindex,nofollow">'
            f'<title>WhatsApp Capture</title><style>{CSS}</style></head>'
            f'<body><main>{body}</main><script>{JS}</script></body></html>')


def _phone(c):
    return f"+{c['phone']}" if c.get("phone") else ""


def render_admin(flash=""):
    contacts = load_contacts()
    _annotate_held(contacts)
    unparsed = sum(1 for _ in s3_list("unparsed/"))
    admin_q = "?key=" + urllib.parse.quote(env("ADMIN_KEY"), safe="")
    by = {s: [] for s in STATUSES}
    for c in contacts:
        by[c.get("status") if c.get("status") in STATUSES else "unsorted"].append(c)
    for lst in by.values():
        lst.sort(key=lambda c: c.get("last_seen") or "", reverse=True)

    out = ["<h1>WhatsApp Capture</h1>",
           f'<div class="status">Forwarding: <b>{"ON" if forwarding_enabled() else "OFF"}</b>'
           f' &middot; Emails sent today: <b>{sent_today()}</b> / {DAILY_CAP}'
           f' &middot; Unparsed payloads: <b>{unparsed}</b>'
           f' (<a href="{_e(admin_q)}&amp;view=unparsed">view</a>)</div>']
    if flash:
        out.append(f'<div class="flash">{_e(flash)}</div>')

    out.append(f"<h2>To sort ({len(by['unsorted'])})</h2>")
    if not by["unsorted"]:
        out.append('<p class="empty">Nothing to sort.</p>')
    for c in by["unsorted"]:
        badge = '<span class="badge">group</span>' if c.get("is_group") else ""
        n = c.get("msg_count", 0)
        out.append(
            f'<div class="card"><div class="info"><div class="name">{_e(c.get("name"))}{badge}'
            f'</div><div class="meta">{_e(_phone(c))} &middot; {n} held &middot; last seen '
            f'{_e(c.get("last_seen"))}</div><div class="preview">{_e(c.get("last_preview"))}'
            f'</div></div><div class="btns">'
            + _form(c["key"], "client", "Client", "b-client",
                    f"Mark {c.get('name')} as CLIENT and forward {n} held message(s)?")
            + _form(c["key"], "personal", "Personal", "b-personal",
                    f"Mark {c.get('name')} as PERSONAL and permanently delete {n} held "
                    "message(s)?")
            + "</div></div>")

    out.append(f"<h2>Clients ({len(by['client'])})</h2>")
    if not by["client"]:
        out.append('<p class="empty">No clients yet.</p>')
    for c in by["client"]:
        held = ""
        if c.get("held_count"):
            held = _form(c["key"], "forward_held", f"Forward held ({c['held_count']})", "b-held",
                         f"Forward held messages for {c.get('name')} now?")
        out.append(
            f'<div class="card"><div class="info"><div class="name">{_e(c.get("name"))}'
            f'{"<span class=badge>group</span>" if c.get("is_group") else ""}</div>'
            f'<div class="meta">{_e(_phone(c))} &middot; last seen {_e(c.get("last_seen"))} '
            f'&middot; {c.get("msg_count", 0)} msgs</div></div><div class="btns">{held}'
            + _form(c["key"], "personal", "Move to Personal", "b-personal",
                    f"Move {c.get('name')} to PERSONAL? Future messages will not be stored. "
                    "Already-forwarded messages are kept; unforwarded ones are deleted.")
            + "</div></div>")

    out.append(f"<h2>Personal ({len(by['personal'])})</h2>")
    if not by["personal"]:
        out.append('<p class="empty">No personal contacts.</p>')
    for c in by["personal"]:
        out.append(
            f'<div class="card"><div class="info"><div class="name">{_e(c.get("name"))}'
            f'{"<span class=badge>group</span>" if c.get("is_group") else ""}</div>'
            f'<div class="meta">{_e(_phone(c))}</div></div><div class="btns">'
            + _form(c["key"], "client", "Move to Client", "b-client",
                    f"Move {c.get('name')} to CLIENT? Future messages will be forwarded.")
            + "</div></div>")
    return _page("".join(out))


def _annotate_held(contacts):
    for c in contacts:
        if c.get("status") == "client":
            c["held_count"] = sum(1 for _, r in load_messages(c["key"]) if not r.get("forwarded"))


def render_unparsed():
    objs = sorted(s3_list("unparsed/"), key=lambda o: str(o.get("LastModified") or ""),
                  reverse=True)
    admin_q = "?key=" + urllib.parse.quote(env("ADMIN_KEY"), safe="")
    out = ["<h1>WhatsApp Capture</h1>",
           f'<div class="status"><a href="{_e(admin_q)}">&larr; back</a> &middot; '
           f"{len(objs)} unparsed payloads (showing last 20)</div>"]
    for o in objs[:20]:
        raw = s3_get_bytes(o["Key"]) or b""
        try:
            txt = json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
        except Exception:
            txt = raw.decode("utf-8", "replace")
        out.append(f"<h2>{_e(o['Key'])}</h2><pre>{_e(txt)}</pre>")
    return _page("".join(out))


# ---------------------------------------------------------------------------
# Lambda entry
# ---------------------------------------------------------------------------

def resp(status, body="", ctype="text/plain; charset=utf-8", headers=None):
    h = {"Content-Type": ctype, "Cache-Control": "no-store"}
    h.update(headers or {})
    return {"statusCode": status, "headers": h, "body": body}


def html_resp(body):
    return resp(200, body, "text/html; charset=utf-8",
                {"Referrer-Policy": "no-referrer", "X-Robots-Tag": "noindex"})


def _query(event):
    q = event.get("queryStringParameters")
    if isinstance(q, dict):
        return q
    return dict(urllib.parse.parse_qsl(event.get("rawQueryString") or ""))


def _raw_body(event):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(body)
    return body.encode("utf-8") if isinstance(body, str) else bytes(body)


def lambda_handler(event, context):
    event = event or {}
    if event.get("source") == "aws.events" or event.get("detail-type") == "Scheduled Event":
        n = daily_nudge()
        return {"nudged": n}

    method = ((event.get("requestContext") or {}).get("http") or {}).get("method") \
        or event.get("httpMethod") or "GET"
    method = method.upper()
    q = _query(event)
    qkey = q.get("key", "")

    if method == "GET":
        if key_ok(qkey, env("ADMIN_KEY")):
            if q.get("view") == "unparsed":
                return html_resp(render_unparsed())
            return html_resp(render_admin(q.get("msg", "")))
        if key_ok(qkey, env("WEBHOOK_KEY")):
            return resp(200, "ok")
        return resp(403, "forbidden")

    if method != "POST":
        return resp(405, "method not allowed")

    raw = _raw_body(event)
    if key_ok(qkey, env("WEBHOOK_KEY")):
        try:
            handle_webhook(raw)
        except Exception as e:
            print("webhook error:", traceback.format_exc())
            try:
                save_unparsed(raw, f"exception: {type(e).__name__}: {e}")
            except Exception:
                print("failed to save unparsed:", traceback.format_exc())
        return resp(200, "ok")

    form = dict(urllib.parse.parse_qsl(raw.decode("utf-8", "replace")))
    if key_ok(form.get("key", ""), env("ADMIN_KEY")):
        try:
            flash = admin_action(form.get("contact_key", ""), form.get("action", ""))
        except Exception as e:
            print("admin error:", traceback.format_exc())
            flash = f"Error: {type(e).__name__}: {e}"
        loc = "/?" + urllib.parse.urlencode({"key": env("ADMIN_KEY"), "msg": flash})
        return resp(303, "", headers={"Location": loc})

    return resp(403, "forbidden")
