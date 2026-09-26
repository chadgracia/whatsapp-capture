"""WhatsApp Capture: TimelinesAI webhook -> S3 -> SES compliance forwarder.

Single-file Lambda (Python 3.12, stdlib + boto3). See CLAUDE.md for the
S3 layout, statuses and compliance rules.
"""

import base64
import gc
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
from collections import Counter
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
CRM_BUCKET = "full-pipeline-cache"
INDEX_KEY = "state/phone-index.json"
BATCH_KEY = "state/batch-last.json"
LOCK_KEY = "state/batch-lock.json"
LOCK_TTL = timedelta(minutes=15)
PIPELINE_PERSON_URL = "https://app.pipelinecrm.com/people/"
MATCH_DIGITS = 9
MIN_PHONE_DIGITS = 7
MAX_PHONE_DIGITS = 15

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


def s3_get_bytes(key, bucket=None):
    try:
        return s3().get_object(Bucket=bucket or _bucket(), Key=key)["Body"].read()
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
    sender_phone = (_digits(_first(senders, phone_names))
                    or _jid_digits(_first(senders, ("jid", "id")))
                    or _digits(_first(msgs + tops, ("sender_phone", "from_phone",
                                                    "author_phone"))))

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
        "sender_phone": sender_phone,
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
            "Forwarding is paused until 00:00 UTC. Unsent client messages stay in S3 with "
            "forwarded:false and go out in the next daily batch (or 'Send now' on the admin "
            "page once the cap resets).\n",
            [], cc=False)
    return False


def record_send():
    key = _state_key()
    st = s3_get_json(key) or {"count": 0, "alerted": False}
    st["count"] = int(st.get("count", 0)) + 1
    s3_put_json(key, st)


# ---------------------------------------------------------------------------
# CRM phone index (s3://full-pipeline-cache/people.json + companies.json)
# Phone fields verified against people.json: phone, mobile, home_phone (+ work_phone if present).
# ---------------------------------------------------------------------------

PHONE_FIELDS = ("phone", "mobile", "home_phone", "work_phone")


def person_phones(p):
    """Return ([(field, digits)], skipped) from the allowlisted top-level phone fields.
    Values containing 'http', '/' or '@', or with <7 or >15 digits, are skipped."""
    found, skipped = [], 0
    for field in PHONE_FIELDS:
        v = p.get(field)
        if isinstance(v, bool) or not isinstance(v, (str, int)):
            continue
        s = str(v).strip()
        if not s:
            continue
        d = re.sub(r"\D", "", s)
        low = s.lower()
        if ("http" in low or "/" in s or "@" in s
                or not MIN_PHONE_DIGITS <= len(d) <= MAX_PHONE_DIGITS):
            skipped += 1
            continue
        found.append((field, d))
    return found, skipped


def person_name(p):
    name = p.get("full_name") or p.get("name")
    if not name:
        name = " ".join(str(p.get(k) or "").strip() for k in ("first_name", "last_name")).strip()
    return str(name or "").strip()


def person_company(p, companies):
    c = p.get("company")
    if isinstance(c, dict) and c.get("name"):
        return str(c["name"]).strip()
    if p.get("company_name"):
        return str(p["company_name"]).strip()
    cid = p.get("company_id")
    if cid is not None:
        return str(companies.get(str(cid)) or "").strip()
    return ""


def _load_crm_list(key, field):
    raw = s3_get_bytes(key, bucket=CRM_BUCKET)
    if raw is None:
        raise RuntimeError(f"s3://{CRM_BUCKET}/{key} not found")
    data = json.loads(raw)
    del raw
    gc.collect()
    items = data.get(field) if isinstance(data, dict) else data
    del data
    return items or []


def build_phone_index():
    companies = {}
    for c in _load_crm_list("companies.json", "companies"):
        if isinstance(c, dict) and c.get("id") is not None:
            companies[str(c["id"])] = c.get("name") or ""
    gc.collect()

    people = _load_crm_list("people.json", "people")
    gc.collect()
    index, key_counts = {}, Counter()
    scanned = no_phone = skipped_values = 0
    for p in people:
        if not isinstance(p, dict):
            continue
        scanned += 1
        phones, skipped = person_phones(p)
        skipped_values += skipped
        if not phones:
            no_phone += 1
            continue
        pid = p.get("id")
        entry = {"person_id": pid, "full_name": person_name(p),
                 "company": person_company(p, companies)}
        for kname, digits in phones:
            key_counts[kname] += 1
            k = digits[-MATCH_DIGITS:]
            cur = index.get(k)
            if cur is None:
                index[k] = entry
            elif "ambiguous" in cur:
                if pid not in cur["ambiguous"]:
                    cur["ambiguous"].append(pid)
            elif cur["person_id"] != pid:
                index[k] = {"ambiguous": [cur["person_id"], pid]}
    del people, companies
    gc.collect()

    doc = {
        "built_at": iso(now_utc()),
        "people_scanned": scanned,
        "phones_indexed": len(index),
        "people_with_no_phone": no_phone,
        "top_phone_keys": key_counts.most_common(10),
        "skipped_non_phone_values": skipped_values,
        "index": index,
    }
    s3_put_bytes(INDEX_KEY, json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
                 .encode("utf-8"), "application/json")
    del index
    gc.collect()
    return {k: v for k, v in doc.items() if k != "index"}


def load_index():
    return s3_get_json(INDEX_KEY) or {}


def crm_lookup(index, phone):
    d = re.sub(r"\D", "", phone or "")
    none = {"crm_match": "none", "crm_person_id": None, "crm_full_name": None,
            "crm_company": None}
    if len(d) < MIN_PHONE_DIGITS:
        return none
    e = (index.get("index") or {}).get(d[-MATCH_DIGITS:])
    if not e:
        return none
    if "ambiguous" in e:
        return dict(none, crm_match="ambiguous", crm_ambiguous_ids=e["ambiguous"])
    return {"crm_match": "matched", "crm_person_id": e.get("person_id"),
            "crm_full_name": e.get("full_name"), "crm_company": e.get("company")}


def migrate_contact(c):
    """WhatsApp display name lives in wa_name (older records used 'name')."""
    if "name" in c:
        c.setdefault("wa_name", c["name"])
        c.pop("name")
    return c


def wa_name(c):
    return c.get("wa_name") or c.get("name") or (f"+{c['phone']}" if c.get("phone")
                                                  else c.get("key", ""))


def apply_crm(c, index):
    """Refresh crm_* fields from the index. Returns True if the record changed."""
    if not index.get("index"):
        return False
    fields = crm_lookup(index, "" if c.get("is_group") else c.get("phone"))
    changed = any(c.get(k) != v for k, v in fields.items())
    if "crm_ambiguous_ids" not in fields and "crm_ambiguous_ids" in c:
        c.pop("crm_ambiguous_ids")
        changed = True
    c.update(fields)
    return changed


def identity(c):
    """Manual override beats CRM; fallback '<WhatsApp name> [not in CRM]'."""
    if c.get("manual_full_name"):
        return {"full_name": c["manual_full_name"], "company": c.get("manual_company") or "",
                "known": True, "source": "manual"}
    if c.get("crm_match") == "matched" and c.get("crm_full_name"):
        return {"full_name": c["crm_full_name"], "company": c.get("crm_company") or "",
                "known": True, "source": "crm"}
    return {"full_name": f"{wa_name(c)} [not in CRM]", "company": "", "known": False,
            "source": "none"}


def company_label(idn):
    if idn["company"]:
        return idn["company"]
    return "Individual" if idn["known"] else "unknown (not in CRM)"


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


def batch_subject(c, n, date):
    if c.get("is_group"):
        return f"WhatsApp group | {wa_name(c)} | {n} messages | {date}"
    idn = identity(c)
    company = f" ({company_label(idn)})" if idn["known"] else ""
    phone = f"+{c['phone']}" if c.get("phone") else "no phone"
    return f"WhatsApp | {idn['full_name']}{company} | {phone} | {n} messages | {date}"


def batch_header(c, recs):
    first, _ = fmt_times(recs[0].get("timestamp"))
    last, _ = fmt_times(recs[-1].get("timestamp"))
    if c.get("is_group"):
        lines = [f"Contact: {wa_name(c)} (group chat)", "Company: n/a (group)",
                 "Phone: n/a", "Pipeline person ID: n/a (group)",
                 f"WhatsApp name: {wa_name(c)}"]
    else:
        idn = identity(c)
        pid = c.get("crm_person_id") if c.get("crm_match") == "matched" else None
        lines = [f"Contact: {idn['full_name']}", f"Company: {company_label(idn)}",
                 f"Phone: {'+' + c['phone'] if c.get('phone') else 'n/a'}",
                 f"Pipeline person ID: {pid if pid else 'not in CRM'}",
                 f"WhatsApp name: {wa_name(c)}"]
    lines.append(f"Period: {first} - {last}")
    lines.append(f"Messages: {len(recs)}")
    return "\n".join(lines)


def sender_label(c, rec, index):
    if rec.get("direction") == "OUT":
        return rec.get("sender_name") or "Me"
    if c.get("is_group"):
        sp = rec.get("sender_phone") or ""
        m = crm_lookup(index, sp)
        if m["crm_match"] == "matched" and m["crm_full_name"]:
            return f"{m['crm_full_name']} ({m['crm_company'] or 'Individual'})"
        return f"{rec.get('sender_name') or 'unknown'}{' (+' + sp + ')' if sp else ''}"
    return identity(c)["full_name"]


def message_block(c, rec, att_status, index):
    utc, kyiv = fmt_times(rec.get("timestamp"))
    lines = [
        f"Timestamp (UTC): {utc}",
        f"Timestamp (Europe/Kiev): {kyiv}",
        f"Direction: {rec.get('direction')}",
        f"Sender: {sender_label(c, rec, index)}",
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


def _encoded(n):
    return (n * 4) // 3 + 2048


def pack_parts(held):
    """Yield (items, attachment_parts, att_status) chunks, each under MAX_EMAIL_BYTES.
    A message is never dropped: if its attachments don't fit, they are listed instead."""
    budget = MAX_EMAIL_BYTES - 64 * 1024
    cur, parts, status, used = [], [], {}, 0
    for k, r in held:
        text_cost = len((r.get("text") or "").encode("utf-8")) * 2 + 2048
        datas = []
        for i, a in enumerate(r.get("attachments") or []):
            if not a.get("s3_key"):
                continue
            try:
                data = s3_get_bytes(a["s3_key"])
            except Exception:
                data = None
            if data is not None:
                datas.append((i, a, data))
        cost = text_cost + sum(_encoded(len(d)) for _, _, d in datas)
        if cur and used + cost > budget:
            yield cur, parts, status
            cur, parts, status, used = [], [], {}, 0
        cur.append((k, r))
        used += text_cost
        for i, a, data in datas:
            e = _encoded(len(data))
            if used + e > budget:
                continue
            used += e
            ctype = a.get("content_type") or mimetypes.guess_type(a.get("filename") or "")[0]
            parts.append((a.get("filename") or "attachment", data,
                          ctype or "application/octet-stream"))
            status[(r.get("message_id"), i)] = "attached"
    if cur:
        yield cur, parts, status


def load_messages(ck):
    out = []
    for obj in s3_list(f"messages/{ck}/"):
        rec = s3_get_json(obj["Key"])
        if rec is not None:
            out.append((obj["Key"], rec))
    out.sort(key=lambda kr: (kr[1].get("timestamp") or "", kr[0]))
    return out


def forward_contact(c, held, index, date, stats):
    """Send held messages for one client contact; marks each part forwarded after it sends."""
    sep = "\n\n" + "-" * 60 + "\n\n"
    for n, (items, parts, status) in enumerate(pack_parts(held), 1):
        if not cap_allows_send():
            stats["cap_hit"] = True
            return
        recs = [r for _, r in items]
        subject = batch_subject(c, len(recs), date) + (f" (part {n})" if n > 1 else "")
        body = batch_header(c, recs) + sep + sep.join(
            message_block(c, r, status, index) for r in recs) + "\n"
        try:
            send_email(subject, body, parts)
        except Exception as e:
            print("send failed:", c.get("key"), traceback.format_exc())
            stats["errors"].append(f"{c.get('key')}: {type(e).__name__}: {e}"[:300])
            return
        record_send()
        stats["emails_sent"] += 1
        at = iso(now_utc())
        for k, r in items:
            r["forwarded"] = True
            r["forwarded_at"] = at
            s3_put_json(k, r)
        stats["messages_forwarded"] += len(recs)


# ---------------------------------------------------------------------------
# Daily batch
# ---------------------------------------------------------------------------

def acquire_lock():
    lk = s3_get_json(LOCK_KEY)
    if lk:
        at = _parse_ts(lk.get("at"))
        if at and now_utc() - at < LOCK_TTL:
            return False
    s3_put_json(LOCK_KEY, {"at": iso(now_utc())})
    return True


def release_lock():
    s3_delete_keys([LOCK_KEY])


def refresh_contacts(index):
    """Persist CRM lookup + wa_name migration on every contact. Returns contacts."""
    contacts = []
    for c in load_contacts():
        before = json.dumps(c, sort_keys=True)
        migrate_contact(c)
        apply_crm(c, index)
        if json.dumps(c, sort_keys=True) != before:
            s3_put_json(contact_path(c["key"]), c)
        contacts.append(c)
    return contacts


def run_batch(trigger):
    if not acquire_lock():
        return {"skipped": "another batch run is in progress"}
    try:
        stats = {"run_at": iso(now_utc()), "trigger": trigger, "emails_sent": 0,
                 "messages_forwarded": 0, "messages_waiting": 0, "forwarding_enabled":
                 forwarding_enabled(), "errors": []}
        index = load_index()  # rebuilt only via the admin "Rebuild index" button
        date = now_utc().strftime("%Y-%m-%d")
        for c in refresh_contacts(index):
            if c.get("status") != "client":
                continue
            held = [(k, r) for k, r in load_messages(c["key"]) if not r.get("forwarded")]
            if held and forwarding_enabled() and not stats.get("cap_hit"):
                forward_contact(c, held, index, date, stats)
            stats["messages_waiting"] += sum(
                1 for _, r in load_messages(c["key"]) if not r.get("forwarded"))
        s3_put_json(BATCH_KEY, stats)
        return stats
    finally:
        release_lock()


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


def store_attachments(ck, mid, atts):
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
            entry.update(s3_key=key, size=len(data), content_type=ctype)
        except Exception as e:
            entry["error"] = f"{type(e).__name__}: {e}"[:300]
        out.append(entry)
    return out


def handle_webhook(raw):
    """Store every non-Personal message. Never sends email (forwarding is the daily batch)."""
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
        "key": ck, "wa_name": msg["name"], "phone": msg["phone"], "is_group": msg["is_group"],
        "status": "unsorted", "first_seen": now, "last_seen": now,
        "last_preview": "", "msg_count": 0,
    }
    migrate_contact(contact)
    status = contact.get("status") if contact.get("status") in STATUSES else "unsorted"

    if status == "personal":
        contact["last_seen"] = now
        s3_put_json(contact_path(ck), contact)
        return

    ts = msg["timestamp"] or now_utc()
    preview = msg["text"] or ", ".join(a["filename"] for a in msg["attachments"])
    if msg["name"] and msg["name"] != ck:
        contact["wa_name"] = msg["name"]
    if msg["phone"]:
        contact["phone"] = msg["phone"]
    contact["is_group"] = msg["is_group"]
    contact["last_seen"] = now
    contact["last_preview"] = preview[:PREVIEW_LEN]
    contact["msg_count"] = int(contact.get("msg_count", 0)) + 1

    s3_put_bytes(f"raw/{ck}/{mid}.json", raw, "application/json")
    atts = store_attachments(ck, mid, msg["attachments"])
    rec = {
        "message_id": mid,
        "contact_key": ck,
        "contact_name": wa_name(contact),
        "phone": msg["phone"] or contact.get("phone", ""),
        "is_group": msg["is_group"],
        "chat_name": msg["chat_name"] or (wa_name(contact) if msg["is_group"] else ""),
        "sender_name": msg["sender_name"],
        "sender_phone": msg["sender_phone"],
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


def manual_identity(form):
    """Validate the Full name / Company form. Returns (fields, error)."""
    full = _clean_header(form.get("full_name"))[:200]
    company = _clean_header(form.get("company"))[:200]
    individual = form.get("individual") in ("1", "on", "true")
    if not full:
        return None, "Full name is required."
    if not company and not individual:
        return None, "Company is required unless 'Individual — no company' is ticked."
    return {"manual_full_name": full, "manual_company": "" if individual else company,
            "manual_individual": individual}, None


def _batch_flash(st):
    if st.get("skipped"):
        return f"Send now skipped: {st['skipped']}."
    msg = (f"Batch done: {st['emails_sent']} emails, {st['messages_forwarded']} messages "
           f"forwarded, {st['messages_waiting']} waiting.")
    if not st.get("forwarding_enabled"):
        msg += " Forwarding is OFF."
    if st.get("cap_hit"):
        msg += " Daily cap reached."
    if st.get("errors"):
        msg += f" {len(st['errors'])} send error(s)."
    return msg


def admin_action(form):
    action = form.get("action", "")
    if action == "send_now":
        return _batch_flash(run_batch("admin"))
    if action == "rebuild_index":
        st = build_phone_index()
        refresh_contacts(load_index())
        return (f"Index rebuilt: {st['people_scanned']} people, {st['phones_indexed']} phones, "
                f"{st['people_with_no_phone']} without phone, "
                f"{st['skipped_non_phone_values']} non-phone values skipped.")

    ck = safe(form.get("contact_key", ""))
    contact = s3_get_json(contact_path(ck))
    if not contact:
        return "Contact not found."
    migrate_contact(contact)
    name = wa_name(contact)
    if action == "client":
        apply_crm(contact, load_index())
        if not contact.get("is_group") and not identity(contact)["known"]:
            fields, err = manual_identity(form)
            if err:
                return f"{name} not changed: {err}"
            contact.update(fields)
        contact["status"] = "client"
        contact["sorted_at"] = iso(now_utc())
        s3_put_json(contact_path(ck), contact)
        return f"{name} marked Client. Held messages go out in the next daily batch."
    if action == "set_identity":
        fields, err = manual_identity(form)
        if err:
            return f"{name} not changed: {err}"
        contact.update(fields)
        s3_put_json(contact_path(ck), contact)
        return f"{name} identity set to {fields['manual_full_name']}."
    if action == "personal":
        kept = delete_unforwarded(ck)
        contact["status"] = "personal"
        contact["sorted_at"] = iso(now_utc())
        contact["last_preview"] = ""
        contact["msg_count"] = kept
        s3_put_json(contact_path(ck), contact)
        return f"{name} marked Personal. Unforwarded content deleted."
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
        lines.append(f"- {wa_name(c)}{group} {phone} -- {c.get('msg_count', 0)} msgs")
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
padding:8px 12px;margin:6px 0;display:flex;gap:8px;align-items:center;flex-wrap:wrap;
justify-content:space-between}.status a{color:#2463eb}.flash{background:#e8f5e9;
border:1px solid #b7dfb9;border-radius:8px;padding:8px 12px;margin:8px 0;font-size:14px}
.card{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:10px 12px;
margin:8px 0;display:flex;gap:10px;align-items:flex-start;justify-content:space-between;
flex-wrap:wrap}.info{flex:1 1 300px;min-width:0}.name{font-weight:600}
.meta{font-size:12px;color:#666}.preview{font-size:13px;color:#333;margin-top:4px;
overflow-wrap:anywhere}.badge{display:inline-block;font-size:11px;background:#ede9fe;
color:#5b21b6;border-radius:4px;padding:1px 6px;margin-left:6px;text-decoration:none}
.crm{background:#dcfce7;color:#166534}.warn{background:#fef3c7;color:#92400e}
.btns{display:flex;gap:6px;flex-wrap:wrap;align-items:flex-start}
button{border:0;border-radius:6px;padding:8px 12px;font-size:14px;cursor:pointer}
.b-client{background:#2463eb;color:#fff}.b-personal{background:#e5e7eb;color:#111}
.b-held{background:#f59e0b;color:#111}.empty{color:#888;font-size:14px}
.idform{display:flex;flex-direction:column;gap:6px;min-width:220px}
.idform input[type=text]{padding:7px 8px;border:1px solid #d1d5db;border-radius:6px;
font-size:14px}.idform label{font-size:12px;color:#444}
details summary{cursor:pointer;font-size:14px;color:#2463eb;padding:8px 4px}
pre{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:10px;overflow-x:auto;
font-size:12px;white-space:pre-wrap;overflow-wrap:anywhere}
"""

JS = """
document.addEventListener('submit',function(e){var f=e.target;
if(!confirm(f.getAttribute('data-confirm'))){e.preventDefault();}});
"""


def _e(s):
    return html.escape(str(s if s is not None else ""), quote=True)


def _form(ck, action, label, cls, confirm_text, fields=""):
    ck_input = (f'<input type="hidden" name="contact_key" value="{_e(ck)}">' if ck else "")
    return (f'<form method="POST" action="/" data-confirm="{_e(confirm_text)}"'
            f'{" class=idform" if fields else ""}>'
            f'<input type="hidden" name="key" value="{_e(env("ADMIN_KEY"))}">{ck_input}'
            f'<input type="hidden" name="action" value="{_e(action)}">{fields}'
            f'<button class="{cls}" type="submit">{_e(label)}</button></form>')


def _identity_fields(c):
    ind = c.get("manual_individual")
    return (f'<input type="text" name="full_name" required placeholder="Full name" '
            f'value="{_e(c.get("manual_full_name") or "")}">'
            f'<input type="text" name="company" placeholder="Company" '
            f'value="{_e(c.get("manual_company") or "")}">'
            f'<label><input type="checkbox" name="individual" value="1"'
            f'{" checked" if ind else ""}> Individual — no company</label>')


def _client_button(c, label):
    """One click if identity is known (or group); otherwise inline Full name/Company form."""
    who = wa_name(c)
    if c.get("is_group") or identity(c)["known"]:
        return _form(c["key"], "client", label, "b-client",
                     f"Mark {who} as CLIENT? Held messages go out in the next daily batch.")
    return _form(c["key"], "client", label, "b-client",
                 f"Mark {who} as CLIENT with this name/company?", _identity_fields(c))


def _page(body):
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta name="robots" content="noindex,nofollow">'
            f'<title>WhatsApp Capture</title><style>{CSS}</style></head>'
            f'<body><main>{body}</main><script>{JS}</script></body></html>')


def _phone(c):
    return f"+{c['phone']}" if c.get("phone") else ""


def _name_html(c):
    """'<CRM full name> — <company>' + Pipeline badge when identified; WA name + phone below."""
    group = '<span class="badge">group</span>' if c.get("is_group") else ""
    idn = identity(c)
    if c.get("is_group") or not idn["known"]:
        warn = ""
        if not c.get("is_group"):
            label = "CRM: ambiguous" if c.get("crm_match") == "ambiguous" else "not in CRM"
            warn = f'<span class="badge warn">{label}</span>'
        return (f'<div class="name">{_e(wa_name(c))}{group}{warn}</div>'
                f'<div class="meta">{_e(_phone(c))}')
    badge = ""
    if c.get("crm_match") == "matched" and c.get("crm_person_id") is not None:
        badge = (f'<a class="badge crm" target="_blank" rel="noopener noreferrer" '
                 f'href="{_e(PIPELINE_PERSON_URL + str(c["crm_person_id"]))}">Pipeline</a>')
    manual = '<span class="badge">manual</span>' if idn["source"] == "manual" else ""
    return (f'<div class="name">{_e(idn["full_name"])} — {_e(company_label(idn))}'
            f'{badge}{manual}</div>'
            f'<div class="meta">WhatsApp: {_e(wa_name(c))} {_e(_phone(c))}')


def _held_count(c):
    return sum(1 for _, r in load_messages(c["key"]) if not r.get("forwarded"))


def render_admin(flash=""):
    index = load_index()
    contacts = []
    for c in load_contacts():  # CRM lookup in memory only: GET never writes
        migrate_contact(c)
        apply_crm(c, index)
        contacts.append(c)
    unparsed = sum(1 for _ in s3_list("unparsed/"))
    admin_q = "?key=" + urllib.parse.quote(env("ADMIN_KEY"), safe="")
    by = {s: [] for s in STATUSES}
    for c in contacts:
        by[c.get("status") if c.get("status") in STATUSES else "unsorted"].append(c)
    for lst in by.values():
        lst.sort(key=lambda c: c.get("last_seen") or "", reverse=True)
    waiting = 0
    for c in by["client"]:
        c["held_count"] = _held_count(c)
        waiting += c["held_count"]
    last = s3_get_json(BATCH_KEY) or {}
    not_sent = ""
    if waiting and not str(last.get("run_at") or "").startswith(now_utc().strftime("%Y-%m-%d")):
        not_sent = ' <span class="badge warn">not sent today</span>'

    out = ["<h1>WhatsApp Capture</h1>",
           f'<div class="status"><span>Forwarding: <b>'
           f'{"ON" if forwarding_enabled() else "OFF"}</b>'
           f' &middot; Emails sent today: <b>{sent_today()}</b> / {DAILY_CAP}'
           f' &middot; Unparsed payloads: <b>{unparsed}</b>'
           f' (<a href="{_e(admin_q)}&amp;view=unparsed">view</a>)</span></div>',
           f'<div class="status"><span>Last batch: <b>{_e(last.get("run_at") or "never")}</b>'
           f' &middot; emails sent: <b>{last.get("emails_sent", 0)}</b>'
           f' &middot; messages forwarded: <b>{last.get("messages_forwarded", 0)}</b>'
           f' &middot; messages waiting: <b>{waiting}</b>{not_sent}</span>'
           + _form("", "send_now", "Send now", "b-held",
                   f"Run the daily batch now and email {waiting} waiting client message(s)?")
           + "</div>"]
    top_keys = ", ".join(f"{k} ({n})" for k, n in index.get("top_phone_keys") or [])
    out.append(
        f'<div class="status"><span>CRM index: <b>{_e(index.get("built_at") or "never built")}'
        f'</b> &middot; people: {index.get("people_scanned", 0)}'
        f' &middot; phones indexed: {index.get("phones_indexed", 0)}'
        f' &middot; people with no phone: {index.get("people_with_no_phone", 0)}'
        f' &middot; skipped non-phone values: {index.get("skipped_non_phone_values", 0)}'
        f'<br>Top phone keys: {_e(top_keys or "n/a")}</span>'
        + _form("", "rebuild_index", "Rebuild index", "b-personal",
                "Rebuild the CRM phone index from people.json now?")
        + "</div>")
    if flash:
        out.append(f'<div class="flash">{_e(flash)}</div>')

    out.append(f"<h2>To sort ({len(by['unsorted'])})</h2>")
    if not by["unsorted"]:
        out.append('<p class="empty">Nothing to sort.</p>')
    for c in by["unsorted"]:
        n = c.get("msg_count", 0)
        out.append(
            f'<div class="card"><div class="info">{_name_html(c)} &middot; {n} held &middot; '
            f'last seen {_e(c.get("last_seen"))}</div><div class="preview">'
            f'{_e(c.get("last_preview"))}</div></div><div class="btns">'
            + _client_button(c, "Client")
            + _form(c["key"], "personal", "Personal", "b-personal",
                    f"Mark {wa_name(c)} as PERSONAL and permanently delete {n} held "
                    "message(s)?")
            + "</div></div>")

    out.append(f"<h2>Clients ({len(by['client'])})</h2>")
    if not by["client"]:
        out.append('<p class="empty">No clients yet.</p>')
    for c in by["client"]:
        held = f" &middot; {c['held_count']} waiting" if c.get("held_count") else ""
        edit = ""
        if not c.get("is_group"):
            edit = ("<details><summary>Edit</summary>"
                    + _form(c["key"], "set_identity", "Save", "b-client",
                            f"Override name/company for {wa_name(c)}?", _identity_fields(c))
                    + "</details>")
        out.append(
            f'<div class="card"><div class="info">{_name_html(c)} &middot; last seen '
            f'{_e(c.get("last_seen"))} &middot; {c.get("msg_count", 0)} msgs{held}</div></div>'
            f'<div class="btns">{edit}'
            + _form(c["key"], "personal", "Move to Personal", "b-personal",
                    f"Move {wa_name(c)} to PERSONAL? Future messages will not be stored. "
                    "Already-forwarded messages are kept; unforwarded ones are deleted.")
            + "</div></div>")

    out.append(f"<h2>Personal ({len(by['personal'])})</h2>")
    if not by["personal"]:
        out.append('<p class="empty">No personal contacts.</p>')
    for c in by["personal"]:
        out.append(
            f'<div class="card"><div class="info">{_name_html(c)}</div></div>'
            f'<div class="btns">' + _client_button(c, "Move to Client") + "</div></div>")
    return _page("".join(out))


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
    task = event.get("task") or (event.get("detail") if isinstance(event.get("detail"), dict)
                                 else {}).get("task")
    if task == "daily_forward" and "requestContext" not in event:
        return run_batch("schedule")
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
            flash = admin_action(form)
        except Exception as e:
            print("admin error:", traceback.format_exc())
            flash = f"Error: {type(e).__name__}: {e}"
        loc = "/?" + urllib.parse.urlencode({"key": env("ADMIN_KEY"), "msg": flash})
        return resp(303, "", headers={"Location": loc})

    return resp(403, "forbidden")
