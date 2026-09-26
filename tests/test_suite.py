"""Offline tests: boto3 is mocked (in-memory fake S3 + MagicMock SES), no network."""

import copy
import email
import email.policy
import json
import os
import sys
import unittest
import urllib.parse
from datetime import datetime, timezone
from unittest import mock

sys.modules.setdefault("boto3", mock.MagicMock())
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import lambda_function as lf  # noqa: E402

ENV = {
    "WEBHOOK_KEY": "hook-secret",
    "ADMIN_KEY": "admin-secret",
    "BUCKET": "test-bucket",
    "FROM_ADDR": "capture@example.com",
    "TO_ADDR": "archive@example.com",
    "FORWARDING_ENABLED": "true",
}

MAIN = "test-bucket"

INCOMING = {
    "event_type": "message:received:new",
    "chat": {"chat_id": 111, "full_name": "Ivan Petrenko", "phone": "+380 50 123 4567",
             "is_group": False},
    "message": {
        "message_id": "m-in-1", "text": "Hello, about the deal", "direction": "received",
        "timestamp": "2026-09-26T08:00:00Z",
        "sender": {"full_name": "Ivan Petrenko", "phone": "+380501234567"},
        "attachments": [{"filename": "terms.pdf",
                         "temporary_download_url": "https://files.example/terms.pdf"}],
    },
    "whatsapp_account": {"phone": "+15550001111", "full_name": "Chad"},
}

OUTGOING = {
    "event": "message.sent",
    "data": {
        "chat": {"id": "380501234567@s.whatsapp.net", "name": "Ivan Petrenko"},
        "message": {"id": "m-out-1", "body": "Sure, sending now", "from_me": True,
                    "created_at": 1790000000},
    },
}

GROUP = {
    "event_type": "message:received:new",
    "chat": {"chat_id": "120363000000000000@g.us", "full_name": "Deal Team", "is_group": True},
    "message": {"message_id": "m-grp-1", "text": "Group hello", "direction": "incoming",
                "timestamp": 1790000100000,
                "sender": {"full_name": "Olena K", "phone": "+380671112233"}},
}

CK = "380501234567"


PEOPLE = {"people": [
    {"id": 101, "full_name": "Ivan Petrenko", "company": {"id": 1, "name": "Acme Capital"},
     "phone": "+380 (50) 123-4567"},
    {"id": 102, "first_name": "Olena", "last_name": "Kovalenko", "company_id": 2,
     "custom_fields": {"Mobile Phone": "067 111 2233"}},
    {"id": 103, "name": "Petro Solo", "company_name": "Solo LLC",
     "phones": [{"number": "+44 20 7946 0001", "type": "work"}]},
    {"id": 104, "full_name": "Twin A", "work_phone": "+1 555 000 9999"},
    {"id": 105, "full_name": "Twin B", "mobile": "001-555-000-9999"},
    {"id": 106, "full_name": "No Phone Person", "phone": "123"},
    {"id": 107, "full_name": "Freelancer", "custom_fields": [
        {"name": "Cell phone", "value": "+971 50 000 1111"}]},
]}
COMPANIES = {"companies": [{"id": 1, "name": "Acme Capital"}, {"id": 2, "name": "Kyiv Partners"}]}


class FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Body:
    def __init__(self, data):
        self._d = data

    def read(self):
        return self._d


class FakeS3:
    def __init__(self):
        self.buckets = {MAIN: {}}
        self.objects = self.buckets[MAIN]
        self.writes = 0

    def get_object(self, Bucket, Key):
        b = self.buckets.get(Bucket, {})
        if Key not in b:
            raise FakeClientError("NoSuchKey")
        return {"Body": _Body(b[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        assert Bucket == MAIN, "writes only go to the capture bucket"
        self.writes += 1
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.buckets[Bucket] if k.startswith(Prefix))
        return {"Contents": [{"Key": k, "Size": len(self.objects[k]),
                              "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)}
                             for k in keys], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        self.writes += 1
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)

    def keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def json(self, key):
        return json.loads(self.objects[key])


def webhook_event(payload, key="hook-secret"):
    body = payload if isinstance(payload, str) else json.dumps(payload)
    ev = {"requestContext": {"http": {"method": "POST"}}, "body": body,
          "isBase64Encoded": False, "queryStringParameters": {}}
    if key is not None:
        ev["queryStringParameters"] = {"key": key}
    return ev


def admin_post(ck="", action="", key="admin-secret", **fields):
    data = {"key": key, "contact_key": ck, "action": action}
    data.update(fields)
    return {"requestContext": {"http": {"method": "POST"}},
            "body": urllib.parse.urlencode(data), "isBase64Encoded": False}


def get_event(**q):
    return {"requestContext": {"http": {"method": "GET"}}, "queryStringParameters": q}


BATCH_EVENT = {"task": "daily_forward"}


def with_id(payload, mid, text=None, ts=None):
    p = copy.deepcopy(payload)
    m = p["message"] if "message" in p else p["data"]["message"]
    m["message_id" if "message_id" in m else "id"] = mid
    if text is not None:
        m["text" if "text" in m else "body"] = text
    if ts is not None:
        m["timestamp"] = ts
    return p


class Base(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, ENV, clear=False)
        self.env.start()
        os.environ.pop("CC_ADDR", None)
        self.s3 = FakeS3()
        self.s3.buckets["full-pipeline-cache"] = {
            "people.json": json.dumps(PEOPLE).encode(),
            "companies.json": json.dumps(COMPANIES).encode()}
        self.ses = mock.MagicMock()
        lf._clients.clear()
        lf._clients.update(s3=self.s3, ses=self.ses)
        self.dl = mock.patch.object(lf, "download",
                                    return_value=(b"%PDF-1.4 fake", "application/pdf"))
        self.dl.start()

    def tearDown(self):
        self.dl.stop()
        self.env.stop()
        lf._clients.clear()

    def post(self, payload, key="hook-secret"):
        return lf.lambda_handler(webhook_event(payload, key), None)

    def batch(self):
        return lf.lambda_handler(dict(BATCH_EVENT), None)

    def set_status(self, ck, status, **extra):
        rec = {"key": ck, "wa_name": "X", "phone": ck, "is_group": False, "status": status,
               "first_seen": "2026-01-01T00:00:00Z", "last_seen": "2026-01-01T00:00:00Z",
               "last_preview": "old", "msg_count": 0}
        rec.update(extra)
        self.s3.objects[f"contacts/{ck}.json"] = json.dumps(rec).encode()

    def contact(self, ck=CK):
        return self.s3.json(f"contacts/{ck}.json")

    def sent(self):
        return [email.message_from_bytes(c.kwargs["RawMessage"]["Data"],
                                         policy=email.policy.default)
                for c in self.ses.send_raw_email.call_args_list]

    @staticmethod
    def text_of(m):
        part = m.get_body(preferencelist=("plain",))
        return part.get_content()


class TestParser(unittest.TestCase):
    def test_incoming(self):
        m = lf.parse_payload(INCOMING)
        self.assertEqual(m["contact_key"], CK)
        self.assertEqual(m["direction"], "IN")
        self.assertEqual(m["name"], "Ivan Petrenko")
        self.assertEqual(m["message_id"], "m-in-1")
        self.assertEqual(m["attachments"][0]["filename"], "terms.pdf")

    def test_outgoing_same_contact(self):
        m = lf.parse_payload(OUTGOING)
        self.assertEqual(m["contact_key"], CK)
        self.assertEqual(m["direction"], "OUT")
        self.assertEqual(m["text"], "Sure, sending now")
        self.assertEqual(m["sender_name"], "Me")

    def test_group(self):
        m = lf.parse_payload(GROUP)
        self.assertTrue(m["is_group"])
        self.assertEqual(m["contact_key"], "120363000000000000_g.us")
        self.assertEqual(m["chat_name"], "Deal Team")
        self.assertEqual(m["sender_name"], "Olena K")
        self.assertEqual(m["sender_phone"], "380671112233")
        self.assertEqual(m["timestamp"].year, 2026)

    def test_unparseable(self):
        self.assertIsNone(lf.parse_payload({"foo": "bar"}))
        self.assertIsNone(lf.parse_payload([1, 2]))


class TestPhoneIndex(Base):
    def test_phone_normalization(self):
        phones = lf.person_phones({"id": 1, "phone": "+380 (50) 123-4567", "fax": "999999999",
                                   "Mobile": 380671112233, "home_phone": "12-34",
                                   "phones": [{"number": "+44 20 7946 0001", "id": 55}]})
        self.assertEqual(sorted(d for _, d in phones),
                         ["380501234567", "380671112233", "442079460001"])

    def test_build_stats_and_lookup(self):
        st = lf.build_phone_index()
        self.assertEqual(st["people_scanned"], 7)
        self.assertEqual(st["people_with_no_phone"], 1)
        self.assertEqual(st["phones_indexed"], 5)
        self.assertIn(["phone", 1], [list(x) for x in st["top_phone_keys"]])
        idx = lf.load_index()
        self.assertIn("built_at", idx)
        # last-9 match across different prefixes / formatting
        m = lf.crm_lookup(idx, "0501234567")
        self.assertEqual((m["crm_match"], m["crm_person_id"], m["crm_full_name"],
                          m["crm_company"]), ("matched", 101, "Ivan Petrenko", "Acme Capital"))
        # company via company_id, name via first+last, phone via custom_fields dict
        m = lf.crm_lookup(idx, "+380671112233")
        self.assertEqual((m["crm_full_name"], m["crm_company"]),
                         ("Olena Kovalenko", "Kyiv Partners"))
        # company_name fallback + list of phone dicts
        self.assertEqual(lf.crm_lookup(idx, "442079460001")["crm_company"], "Solo LLC")
        # custom_fields list form
        self.assertEqual(lf.crm_lookup(idx, "971500001111")["crm_full_name"], "Freelancer")
        # ambiguous
        m = lf.crm_lookup(idx, "15550009999")
        self.assertEqual(m["crm_match"], "ambiguous")
        self.assertEqual(sorted(m["crm_ambiguous_ids"]), [104, 105])
        self.assertEqual(lf.crm_lookup(idx, "380999999999")["crm_match"], "none")
        self.assertEqual(lf.crm_lookup(idx, "12345")["crm_match"], "none")


class TestWebhook(Base):
    def test_bad_key_403(self):
        for key in (None, "", "wrong", "admin-secret"):
            r = self.post(INCOMING, key)
            self.assertEqual(r["statusCode"], 403, key)
        self.assertEqual(self.s3.writes, 0)
        self.ses.send_raw_email.assert_not_called()

    def test_unsorted_stored_not_emailed(self):
        r = self.post(INCOMING)
        self.assertEqual(r["statusCode"], 200)
        msgs = self.s3.keys(f"messages/{CK}/")
        self.assertEqual(len(msgs), 1)
        rec = self.s3.json(msgs[0])
        self.assertFalse(rec["forwarded"])
        self.assertIn(f"raw/{CK}/m-in-1.json", self.s3.objects)
        self.assertIn(f"attachments/{CK}/m-in-1/terms.pdf", self.s3.objects)
        c = self.contact()
        self.assertEqual((c["status"], c["msg_count"], c["wa_name"]),
                         ("unsorted", 1, "Ivan Petrenko"))
        self.assertNotIn("name", c)
        self.ses.send_raw_email.assert_not_called()

    def test_webhook_never_sends_for_client(self):
        self.set_status(CK, "client", wa_name="Ivan Petrenko")
        self.post(INCOMING)
        self.post(OUTGOING)
        self.ses.send_raw_email.assert_not_called()
        for k in self.s3.keys(f"messages/{CK}/"):
            self.assertFalse(self.s3.json(k)["forwarded"])

    def test_migrates_name_to_wa_name(self):
        self.set_status(CK, "unsorted")
        rec = self.contact()
        rec["name"] = rec.pop("wa_name")
        self.s3.objects[f"contacts/{CK}.json"] = json.dumps(rec).encode()
        self.post(INCOMING)
        c = self.contact()
        self.assertNotIn("name", c)
        self.assertEqual(c["wa_name"], "Ivan Petrenko")

    def test_personal_stores_nothing(self):
        self.set_status(CK, "personal", last_preview="")
        before = self.contact()
        self.post(INCOMING)
        for p in ("messages/", "raw/", "attachments/", "seen/", "unparsed/"):
            self.assertEqual(self.s3.keys(p), [], p)
        after = self.contact()
        self.assertNotEqual(after["last_seen"], before["last_seen"])
        self.assertEqual(after["last_preview"], "")
        self.ses.send_raw_email.assert_not_called()

    def test_dedup(self):
        self.post(INCOMING)
        self.post(INCOMING)
        self.assertEqual(len(self.s3.keys(f"messages/{CK}/")), 1)
        self.assertEqual(self.contact()["msg_count"], 1)

    def test_unparseable_saved(self):
        for payload in ({"foo": "bar"}, "not json {"):
            r = self.post(payload)
            self.assertEqual(r["statusCode"], 200)
        self.assertEqual(len(self.s3.keys("unparsed/")), 2)
        self.assertEqual(self.s3.keys("messages/"), [])
        self.assertEqual(self.s3.keys("contacts/"), [])

    def test_base64_body(self):
        import base64
        ev = webhook_event(INCOMING)
        ev["body"] = base64.b64encode(ev["body"].encode()).decode()
        ev["isBase64Encoded"] = True
        lf.lambda_handler(ev, None)
        self.assertEqual(len(self.s3.keys(f"messages/{CK}/")), 1)


class TestBatch(Base):
    def test_one_email_per_client_in_order(self):
        lf.build_phone_index()
        self.set_status(CK, "client", wa_name="Ivan")
        self.set_status("447700900123", "client", wa_name="Bob")
        self.post(INCOMING)
        self.post(OUTGOING)
        bob = {"chat": {"phone": "+447700900123", "full_name": "Bob"},
               "message": {"message_id": "b1", "text": "hi from bob", "direction": "received",
                           "timestamp": "2026-09-26T07:00:00Z"}}
        self.post(bob)
        st = self.batch()
        self.assertEqual((st["emails_sent"], st["messages_forwarded"], st["messages_waiting"]),
                         (2, 3, 0))
        mails = {m["Subject"]: m for m in self.sent()}
        date = lf.now_utc().strftime("%Y-%m-%d")
        subj = f"WhatsApp | Ivan Petrenko (Acme Capital) | +380501234567 | 2 messages | {date}"
        self.assertIn(subj, mails)
        self.assertIn(f"WhatsApp | Bob [not in CRM] | +447700900123 | 1 messages | {date}",
                      mails)
        m = mails[subj]
        text = self.text_of(m)
        self.assertLess(text.index("Sure, sending now"), text.index("Hello, about the deal"))
        for line in ("Contact: Ivan Petrenko", "Company: Acme Capital",
                     "Phone: +380501234567", "Pipeline person ID: 101",
                     "WhatsApp name: Ivan Petrenko", "Period: ", "Direction: OUT",
                     "Direction: IN", "Sender: Ivan Petrenko",
                     "Timestamp (Europe/Kiev): 2026-09-26 11:00:00 EEST", "terms.pdf (attached)"):
            self.assertIn(line, text)
        self.assertEqual([p.get_filename() for p in m.iter_attachments()], ["terms.pdf"])
        for k in self.s3.keys("messages/"):
            rec = self.s3.json(k)
            self.assertTrue(rec["forwarded"])
            self.assertTrue(rec["forwarded_at"])
        c = self.contact()
        self.assertEqual((c["crm_match"], c["crm_person_id"], c["crm_full_name"],
                          c["crm_company"]), ("matched", 101, "Ivan Petrenko", "Acme Capital"))
        self.assertEqual(self.s3.json(lf._state_key())["count"], 2)
        last = self.s3.json(lf.BATCH_KEY)
        self.assertEqual(last["emails_sent"], 2)
        self.assertNotIn(lf.LOCK_KEY, self.s3.objects)

    def test_bob_body_fallback(self):
        self.set_status("447700900123", "client", wa_name="Bob")
        self.post({"chat": {"phone": "+447700900123", "full_name": "Bob"},
                   "message": {"message_id": "b1", "text": "hi", "direction": "received"}})
        self.batch()
        text = self.text_of(self.sent()[0])
        self.assertIn("Contact: Bob [not in CRM]", text)
        self.assertIn("Pipeline person ID: not in CRM", text)

    def test_group_sender_identity(self):
        gk = "120363000000000000_g.us"
        lf.build_phone_index()
        self.set_status(gk, "client", wa_name="Deal Team", is_group=True, phone="")
        self.post(GROUP)
        unknown = with_id(GROUP, "m-grp-2", "who am I", 1790000200000)
        unknown["message"]["sender"] = {"full_name": "Stranger", "phone": "+49 151 0000000"}
        self.post(unknown)
        self.batch()
        m = self.sent()[0]
        date = lf.now_utc().strftime("%Y-%m-%d")
        self.assertEqual(m["Subject"], f"WhatsApp group | Deal Team | 2 messages | {date}")
        text = self.text_of(m)
        self.assertIn("Sender: Olena Kovalenko (Kyiv Partners)", text)
        self.assertIn("Sender: Stranger (+491510000000)", text)

    def test_second_run_sends_nothing(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        self.batch()
        self.batch()
        self.assertEqual(self.ses.send_raw_email.call_count, 1)

    def test_failed_send_retried_next_run(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        self.ses.send_raw_email.side_effect = RuntimeError("SES down")
        st = self.batch()
        self.assertEqual(st["emails_sent"], 0)
        self.assertEqual(len(st["errors"]), 1)
        self.assertFalse(self.s3.json(self.s3.keys(f"messages/{CK}/")[0])["forwarded"])
        self.ses.send_raw_email.side_effect = None
        st = self.batch()
        self.assertEqual((st["emails_sent"], st["messages_forwarded"]), (1, 1))
        self.assertTrue(self.s3.json(self.s3.keys(f"messages/{CK}/")[0])["forwarded"])

    def test_split_into_parts(self):
        self.set_status(CK, "client", wa_name="Ivan")
        big = b"x" * (4 * 1024 * 1024)
        self.dl.stop()
        with mock.patch.object(lf, "download", return_value=(big, "application/pdf")):
            for i in range(3):
                self.post(with_id(INCOMING, f"big-{i}", f"msg {i}",
                                  f"2026-09-26T0{i}:00:00Z"))
        self.dl.start()
        st = self.batch()
        mails = self.sent()
        self.assertEqual(len(mails), 3)
        self.assertEqual(st["messages_forwarded"], 3)
        self.assertNotIn("(part", mails[0]["Subject"])
        self.assertTrue(mails[1]["Subject"].endswith("(part 2)"))
        self.assertTrue(mails[2]["Subject"].endswith("(part 3)"))
        for i, m in enumerate(mails):
            self.assertLess(len(m.as_bytes()), lf.MAX_EMAIL_BYTES)
            self.assertIn(f"msg {i}", self.text_of(m))
            self.assertEqual(len(list(m.iter_attachments())), 1)

    def test_oversized_attachment_listed_not_dropped(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.dl.stop()
        with mock.patch.object(lf, "download",
                               return_value=(b"x" * (10 * 1024 * 1024), "application/pdf")):
            self.post(INCOMING)
        self.dl.start()
        self.batch()
        m = self.sent()[0]
        self.assertIn("https://files.example/terms.pdf -- not attached (size/download)",
                      self.text_of(m))
        self.assertEqual(list(m.iter_attachments()), [])

    def test_failed_download_listed(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.dl.stop()
        with mock.patch.object(lf, "download", side_effect=OSError("boom")):
            self.post(INCOMING)
        self.dl.start()
        self.batch()
        self.assertIn("https://files.example/terms.pdf -- not attached (size/download)",
                      self.text_of(self.sent()[0]))

    def test_personal_and_unsorted_never_sent(self):
        self.post(INCOMING)                         # unsorted
        self.set_status("447700900123", "personal")
        self.post({"chat": {"phone": "+447700900123"},
                   "message": {"message_id": "p1", "text": "private", "direction": "received"}})
        st = self.batch()
        self.ses.send_raw_email.assert_not_called()
        self.assertEqual(st["emails_sent"], 0)

    def test_forwarding_disabled(self):
        os.environ["FORWARDING_ENABLED"] = "false"
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        st = self.batch()
        self.ses.send_raw_email.assert_not_called()
        self.assertEqual(st["messages_waiting"], 1)

    def test_daily_cap(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.set_status("447700900123", "client", wa_name="Bob")
        self.post(INCOMING)
        self.post({"chat": {"phone": "+447700900123"},
                   "message": {"message_id": "b1", "text": "hi", "direction": "received"}})
        self.s3.objects[lf._state_key()] = json.dumps({"count": 400}).encode()
        st = self.batch()
        self.batch()
        subjects = [m["Subject"] for m in self.sent()]
        self.assertEqual(len(subjects), 1)
        self.assertIn("cap", subjects[0])
        self.assertEqual(st["messages_waiting"], 2)
        for k in self.s3.keys("messages/"):
            self.assertFalse(self.s3.json(k)["forwarded"])

    def test_batch_does_not_rebuild_index(self):
        self.s3.buckets["full-pipeline-cache"] = {}   # any CRM read would fail
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        st = self.batch()
        self.assertNotIn(lf.INDEX_KEY, self.s3.objects)
        self.assertEqual(st["emails_sent"], 1)
        self.assertIn("Ivan Petrenko [not in CRM]", self.sent()[0]["Subject"])

    def test_lock_prevents_concurrent_run(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        self.s3.objects[lf.LOCK_KEY] = json.dumps({"at": lf.iso(lf.now_utc())}).encode()
        st = self.batch()
        self.assertIn("skipped", st)
        self.ses.send_raw_email.assert_not_called()


class TestIdentity(Base):
    def test_manual_beats_crm(self):
        c = {"wa_name": "Ivan", "crm_match": "matched", "crm_full_name": "Ivan Petrenko",
             "crm_company": "Acme Capital", "manual_full_name": "Ivan P. Petrenko",
             "manual_company": "Acme Holdings"}
        idn = lf.identity(c)
        self.assertEqual((idn["full_name"], idn["company"]), ("Ivan P. Petrenko", "Acme Holdings"))
        c["manual_company"] = ""
        self.assertEqual(lf.company_label(lf.identity(c)), "Individual")

    def test_fallback_not_in_crm(self):
        idn = lf.identity({"wa_name": "Ivan", "crm_match": "ambiguous"})
        self.assertEqual(idn["full_name"], "Ivan [not in CRM]")
        self.assertFalse(idn["known"])

    def test_manual_override_used_in_batch(self):
        self.set_status(CK, "client", wa_name="Ivan", manual_full_name="Ivan Override",
                        manual_company="Override Co")
        self.post(INCOMING)
        self.batch()
        self.assertIn("WhatsApp | Ivan Override (Override Co) | +380501234567",
                      self.sent()[0]["Subject"])


class TestAdmin(Base):
    def test_mark_client_matched_one_click_no_email(self):
        lf.build_phone_index()
        self.post(INCOMING)
        self.post(OUTGOING)
        r = lf.lambda_handler(admin_post(CK, "client"), None)
        self.assertEqual(r["statusCode"], 303)
        self.assertTrue(r["headers"]["Location"].startswith("/?key=admin-secret"))
        self.ses.send_raw_email.assert_not_called()
        c = self.contact()
        self.assertEqual((c["status"], c["crm_match"]), ("client", "matched"))
        self.batch()
        self.assertEqual(self.ses.send_raw_email.call_count, 1)
        self.assertIn("2 messages", self.sent()[0]["Subject"])

    def test_mark_client_unmatched_requires_identity(self):
        lf.build_phone_index()
        ck = "447700900123"
        self.post({"chat": {"phone": "+447700900123", "full_name": "Bob"},
                   "message": {"message_id": "b1", "text": "hi", "direction": "received"}})
        lf.lambda_handler(admin_post(ck, "client"), None)
        self.assertEqual(self.contact(ck)["status"], "unsorted")
        lf.lambda_handler(admin_post(ck, "client", full_name="Bob Smith"), None)
        self.assertEqual(self.contact(ck)["status"], "unsorted")
        lf.lambda_handler(admin_post(ck, "client", full_name="Bob Smith", individual="1"), None)
        c = self.contact(ck)
        self.assertEqual((c["status"], c["manual_full_name"], c["manual_company"]),
                         ("client", "Bob Smith", ""))
        lf.lambda_handler(admin_post(ck, "set_identity", full_name="Robert Smith",
                                     company="Smith & Co"), None)
        c = self.contact(ck)
        self.assertEqual((c["manual_full_name"], c["manual_company"]),
                         ("Robert Smith", "Smith & Co"))

    def test_mark_personal_deletes(self):
        self.post(INCOMING)
        self.post(OUTGOING)
        lf.lambda_handler(admin_post(CK, "personal"), None)
        for p in (f"messages/{CK}/", f"raw/{CK}/", f"attachments/{CK}/", "seen/"):
            self.assertEqual(self.s3.keys(p), [], p)
        c = self.contact()
        self.assertEqual((c["status"], c["last_preview"], c["wa_name"]),
                         ("personal", "", "Ivan Petrenko"))
        self.ses.send_raw_email.assert_not_called()

    def test_client_to_personal_keeps_forwarded(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        self.batch()                                # forwarded
        self.post(OUTGOING)                         # held
        lf.lambda_handler(admin_post(CK, "personal"), None)
        msgs = self.s3.keys(f"messages/{CK}/")
        self.assertEqual(len(msgs), 1)
        self.assertEqual(self.s3.json(msgs[0])["message_id"], "m-in-1")
        self.assertEqual(self.s3.keys(f"raw/{CK}/"), [f"raw/{CK}/m-in-1.json"])
        self.assertEqual(self.s3.keys(f"attachments/{CK}/"),
                         [f"attachments/{CK}/m-in-1/terms.pdf"])
        self.assertEqual(self.s3.keys("seen/"), ["seen/m-in-1"])

    def test_send_now_and_rebuild_require_post_and_key(self):
        self.set_status(CK, "client", wa_name="Ivan")
        self.post(INCOMING)
        w = self.s3.writes
        for action in ("send_now", "rebuild_index"):
            r = lf.lambda_handler(get_event(key="admin-secret", action=action), None)
            self.assertEqual(r["statusCode"], 200)
            r = lf.lambda_handler(admin_post(action=action, key="wrong"), None)
            self.assertEqual(r["statusCode"], 403)
        self.assertEqual(self.s3.writes, w)
        self.ses.send_raw_email.assert_not_called()
        self.assertNotIn(lf.INDEX_KEY, self.s3.objects)
        r = lf.lambda_handler(admin_post(action="rebuild_index"), None)
        self.assertEqual(r["statusCode"], 303)
        self.assertIn(lf.INDEX_KEY, self.s3.objects)
        self.assertEqual(self.contact()["crm_match"], "matched")
        page = lf.lambda_handler(get_event(key="admin-secret"), None)["body"]
        self.assertIn("not sent today", page)
        lf.lambda_handler(admin_post(action="send_now"), None)
        self.assertEqual(self.ses.send_raw_email.call_count, 1)
        page = lf.lambda_handler(get_event(key="admin-secret"), None)["body"]
        self.assertNotIn("not sent today", page)

    def test_admin_bad_key(self):
        self.post(INCOMING)
        w = self.s3.writes
        r = lf.lambda_handler(admin_post(CK, "personal", key="nope"), None)
        self.assertEqual(r["statusCode"], 403)
        self.assertEqual(self.s3.writes, w)

    def test_get_never_mutates(self):
        lf.build_phone_index()
        self.post(INCOMING)
        rec = self.contact()                       # legacy record: 'name', no crm fields
        rec["name"] = rec.pop("wa_name")
        self.s3.objects[f"contacts/{CK}.json"] = json.dumps(rec).encode()
        self.s3.objects["unparsed/2026/09/26/x.json"] = b'{"a":1}'
        w = self.s3.writes
        snapshot = dict(self.s3.objects)
        events = [get_event(key="admin-secret"),
                  get_event(key="admin-secret", view="unparsed"),
                  get_event(key="admin-secret", contact_key=CK, action="personal"),
                  get_event(key="admin-secret", action="send_now"),
                  get_event(key="hook-secret"),
                  get_event(key="bad")]
        codes = [lf.lambda_handler(e, None)["statusCode"] for e in events]
        self.assertEqual(codes, [200, 200, 200, 200, 200, 403])
        self.assertEqual(self.s3.writes, w)
        self.assertEqual(self.s3.objects, snapshot)
        self.ses.send_raw_email.assert_not_called()

    def test_admin_page_content(self):
        lf.build_phone_index()
        self.post(INCOMING)
        self.post(GROUP)
        self.post({"chat": {"phone": "+447700900123", "full_name": "Bob"},
                   "message": {"message_id": "b1", "text": "hi", "direction": "received"}})
        self.post({"foo": "bar"})
        page = lf.lambda_handler(get_event(key="admin-secret"), None)["body"]
        self.assertIn("<title>WhatsApp Capture</title>", page)
        self.assertIn("To sort (3)", page)
        self.assertIn("Ivan Petrenko — Acme Capital", page)
        self.assertIn('href="https://app.pipelinecrm.com/people/101"', page)
        self.assertIn("WhatsApp: Ivan Petrenko +380501234567", page)
        self.assertIn('name="full_name" required', page)          # Bob's inline form
        self.assertIn("Individual — no company", page)
        self.assertIn('class="badge">group', page)
        self.assertIn('value="send_now"', page)
        self.assertIn('value="rebuild_index"', page)
        self.assertIn("people with no phone: 1", page)
        self.assertIn("Last batch: <b>never</b>", page)
        self.assertNotIn("not sent today", page)   # nothing waiting (no clients)
        self.assertIn("confirm(", page)
        page = lf.lambda_handler(get_event(key="admin-secret", view="unparsed"), None)["body"]
        self.assertIn("<pre>", page)


class TestNudge(Base):
    def test_nudge(self):
        ev = {"source": "aws.events", "detail-type": "Scheduled Event"}
        lf.lambda_handler(ev, None)
        self.ses.send_raw_email.assert_not_called()
        self.post(INCOMING)
        lf.lambda_handler(ev, None)
        m = self.sent()[0]
        self.assertEqual(m["Subject"], "WhatsApp Capture: 1 contacts to sort")
        body = self.text_of(m)
        self.assertIn(lf.FUNCTION_URL + "?key=admin-secret", body)
        self.assertIn("Hello, about the deal", body)


if __name__ == "__main__":
    unittest.main()
