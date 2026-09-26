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
        self.objects = {}
        self.writes = 0

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FakeClientError("NoSuchKey")
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        self.writes += 1
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
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


def admin_post(ck, action, key="admin-secret"):
    body = urllib.parse.urlencode({"key": key, "contact_key": ck, "action": action})
    return {"requestContext": {"http": {"method": "POST"}}, "body": body,
            "isBase64Encoded": False}


def get_event(**q):
    return {"requestContext": {"http": {"method": "GET"}}, "queryStringParameters": q}


class Base(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, ENV, clear=False)
        self.env.start()
        os.environ.pop("CC_ADDR", None)
        self.s3 = FakeS3()
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

    def set_status(self, ck, status, **extra):
        rec = {"key": ck, "name": "X", "phone": ck, "is_group": False, "status": status,
               "first_seen": "2026-01-01T00:00:00Z", "last_seen": "2026-01-01T00:00:00Z",
               "last_preview": "old", "msg_count": 0}
        rec.update(extra)
        self.s3.objects[f"contacts/{ck}.json"] = json.dumps(rec).encode()

    def sent(self):
        return [email.message_from_bytes(c.kwargs["RawMessage"]["Data"],
                                          policy=email.policy.default)
                for c in self.ses.send_raw_email.call_args_list]


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
        self.assertEqual(m["timestamp"].year, 2026)

    def test_unparseable(self):
        self.assertIsNone(lf.parse_payload({"foo": "bar"}))
        self.assertIsNone(lf.parse_payload([1, 2]))


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
        self.assertEqual(rec["direction"], "IN")
        self.assertIn(f"raw/{CK}/m-in-1.json", self.s3.objects)
        self.assertIn(f"attachments/{CK}/m-in-1/terms.pdf", self.s3.objects)
        c = self.s3.json(f"contacts/{CK}.json")
        self.assertEqual(c["status"], "unsorted")
        self.assertEqual(c["msg_count"], 1)
        self.assertEqual(c["last_preview"], "Hello, about the deal")
        self.ses.send_raw_email.assert_not_called()

    def test_client_message_emailed(self):
        self.set_status(CK, "client", name="Ivan Petrenko")
        os.environ["CC_ADDR"] = "cc@example.com"
        self.post(INCOMING)
        self.assertEqual(self.ses.send_raw_email.call_count, 1)
        kwargs = self.ses.send_raw_email.call_args.kwargs
        self.assertEqual(kwargs["Destinations"], ["archive@example.com", "cc@example.com"])
        m = self.sent()[0]
        self.assertEqual(m["Subject"], "WhatsApp | Ivan Petrenko (+380501234567) | IN")
        self.assertEqual(m["From"], "WhatsApp Capture <capture@example.com>")
        self.assertEqual(m["Cc"], "cc@example.com")
        text = m.get_payload()[0].get_payload(decode=True).decode()
        self.assertIn("Hello, about the deal", text)
        self.assertIn("Timestamp (Europe/Kiev): 2026-09-26 11:00:00 EEST", text)
        self.assertIn("terms.pdf (attached)", text)
        self.assertEqual([p.get_filename() for p in m.iter_attachments()], ["terms.pdf"])
        rec = self.s3.json(self.s3.keys(f"messages/{CK}/")[0])
        self.assertTrue(rec["forwarded"])
        self.assertTrue(rec["forwarded_at"])
        self.assertEqual(self.s3.json(lf._state_key())["count"], 1)

    def test_failed_download_listed(self):
        self.set_status(CK, "client", name="Ivan Petrenko")
        self.dl.stop()
        with mock.patch.object(lf, "download", side_effect=OSError("boom")):
            self.post(INCOMING)
        self.dl.start()
        text = self.sent()[0].get_payload(decode=True).decode()
        self.assertIn("https://files.example/terms.pdf -- not attached (size/download)", text)

    def test_group_subject(self):
        self.set_status("120363000000000000_g.us", "client", name="Deal Team", is_group=True)
        self.post(GROUP)
        self.assertEqual(self.sent()[0]["Subject"], "WhatsApp group | Deal Team | Olena K")

    def test_personal_stores_nothing(self):
        self.set_status(CK, "personal", last_preview="")
        before = self.s3.json(f"contacts/{CK}.json")
        self.post(INCOMING)
        for p in ("messages/", "raw/", "attachments/", "seen/", "unparsed/"):
            self.assertEqual(self.s3.keys(p), [], p)
        after = self.s3.json(f"contacts/{CK}.json")
        self.assertNotEqual(after["last_seen"], before["last_seen"])
        self.assertEqual(after["last_preview"], "")
        self.assertEqual(after["msg_count"], 0)
        self.ses.send_raw_email.assert_not_called()

    def test_dedup(self):
        self.set_status(CK, "client", name="Ivan Petrenko")
        self.post(INCOMING)
        self.post(INCOMING)
        self.assertEqual(len(self.s3.keys(f"messages/{CK}/")), 1)
        self.assertEqual(self.s3.json(f"contacts/{CK}.json")["msg_count"], 1)
        self.assertEqual(self.ses.send_raw_email.call_count, 1)

    def test_forwarding_disabled(self):
        os.environ["FORWARDING_ENABLED"] = "false"
        self.set_status(CK, "client", name="Ivan Petrenko")
        self.post(INCOMING)
        self.ses.send_raw_email.assert_not_called()
        self.assertFalse(self.s3.json(self.s3.keys(f"messages/{CK}/")[0])["forwarded"])

    def test_daily_cap(self):
        self.set_status(CK, "client", name="Ivan Petrenko")
        self.s3.objects[lf._state_key()] = json.dumps({"count": 400}).encode()
        self.post(INCOMING)
        self.post(OUTGOING)
        subjects = [m["Subject"] for m in self.sent()]
        self.assertEqual(len(subjects), 1)
        self.assertIn("cap", subjects[0])
        for k in self.s3.keys(f"messages/{CK}/"):
            self.assertFalse(self.s3.json(k)["forwarded"])
        st = self.s3.json(lf._state_key())
        self.assertEqual(st["count"], 400)
        self.assertTrue(st["alerted"])

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


class TestAdmin(Base):
    def test_mark_client_sends_one_backlog(self):
        self.post(INCOMING)
        self.post(OUTGOING)
        self.ses.send_raw_email.assert_not_called()
        r = lf.lambda_handler(admin_post(CK, "client"), None)
        self.assertEqual(r["statusCode"], 303)
        self.assertTrue(r["headers"]["Location"].startswith("/?key=admin-secret"))
        self.assertEqual(self.ses.send_raw_email.call_count, 1)
        m = self.sent()[0]
        self.assertEqual(m["Subject"],
                         "WhatsApp backlog | Ivan Petrenko (+380501234567) | 2 messages")
        text = m.get_payload()[0].get_payload(decode=True).decode()
        self.assertLess(text.index("Sure, sending now"), text.index("Hello, about the deal"))
        self.assertEqual([p.get_filename() for p in m.iter_attachments()], ["terms.pdf"])
        for k in self.s3.keys(f"messages/{CK}/"):
            self.assertTrue(self.s3.json(k)["forwarded"])
        self.assertEqual(self.s3.json(f"contacts/{CK}.json")["status"], "client")
        # Subsequent messages go out individually.
        self.post(dict(copy.deepcopy(INCOMING), message=dict(INCOMING["message"],
                                                             message_id="m-in-2")))
        self.assertEqual(self.ses.send_raw_email.call_count, 2)

    def test_mark_personal_deletes(self):
        self.post(INCOMING)
        self.post(OUTGOING)
        lf.lambda_handler(admin_post(CK, "personal"), None)
        for p in (f"messages/{CK}/", f"raw/{CK}/", f"attachments/{CK}/", "seen/"):
            self.assertEqual(self.s3.keys(p), [], p)
        c = self.s3.json(f"contacts/{CK}.json")
        self.assertEqual(c["status"], "personal")
        self.assertEqual(c["last_preview"], "")
        self.assertEqual(c["name"], "Ivan Petrenko")
        self.ses.send_raw_email.assert_not_called()

    def test_client_to_personal_keeps_forwarded(self):
        self.set_status(CK, "client", name="Ivan Petrenko")
        self.post(INCOMING)                      # forwarded
        os.environ["FORWARDING_ENABLED"] = "false"
        self.post(OUTGOING)                      # held
        lf.lambda_handler(admin_post(CK, "personal"), None)
        msgs = self.s3.keys(f"messages/{CK}/")
        self.assertEqual(len(msgs), 1)
        self.assertEqual(self.s3.json(msgs[0])["message_id"], "m-in-1")
        self.assertEqual(self.s3.keys(f"raw/{CK}/"), [f"raw/{CK}/m-in-1.json"])
        self.assertEqual(self.s3.keys(f"attachments/{CK}/"), [f"attachments/{CK}/m-in-1/terms.pdf"])
        self.assertEqual(self.s3.keys("seen/"), ["seen/m-in-1"])

    def test_mark_client_respects_cap(self):
        self.post(INCOMING)
        self.s3.objects[lf._state_key()] = json.dumps({"count": 400}).encode()
        lf.lambda_handler(admin_post(CK, "client"), None)
        self.assertEqual(len(self.sent()), 1)  # only the cap alert
        self.assertFalse(self.s3.json(self.s3.keys(f"messages/{CK}/")[0])["forwarded"])

    def test_admin_bad_key(self):
        self.post(INCOMING)
        w = self.s3.writes
        r = lf.lambda_handler(admin_post(CK, "personal", key="nope"), None)
        self.assertEqual(r["statusCode"], 403)
        self.assertEqual(self.s3.writes, w)

    def test_get_never_mutates(self):
        self.post(INCOMING)
        self.s3.objects["unparsed/2026/09/26/x.json"] = b'{"a":1}'
        w = self.s3.writes
        snapshot = dict(self.s3.objects)
        events = [get_event(key="admin-secret"),
                  get_event(key="admin-secret", view="unparsed"),
                  get_event(key="admin-secret", contact_key=CK, action="personal"),
                  get_event(key="hook-secret"),
                  get_event(key="bad")]
        codes = [lf.lambda_handler(e, None)["statusCode"] for e in events]
        self.assertEqual(codes, [200, 200, 200, 200, 403])
        self.assertEqual(self.s3.writes, w)
        self.assertEqual(self.s3.objects, snapshot)
        self.ses.send_raw_email.assert_not_called()

    def test_admin_page_content(self):
        self.post(INCOMING)
        self.post(GROUP)
        self.post({"foo": "bar"})
        page = lf.lambda_handler(get_event(key="admin-secret"), None)["body"]
        self.assertIn("<title>WhatsApp Capture</title>", page)
        self.assertIn("To sort (2)", page)
        self.assertIn("Ivan Petrenko", page)
        self.assertIn('class="badge">group', page)
        self.assertIn("confirm(", page)
        self.assertIn('method="POST"', page)
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
        body = m.get_payload(decode=True).decode()
        self.assertIn(lf.FUNCTION_URL + "?key=admin-secret", body)
        self.assertIn("Hello, about the deal", body)


if __name__ == "__main__":
    unittest.main()
