# whatsapp-capture

Single-file AWS Lambda (`lambda_function.py`, Python 3.12, stdlib + boto3 only) that receives
WhatsApp messages from TimelinesAI webhooks, lets Chad sort each contact as Client or Personal
on an admin page, and emails every Client message to the archived Rainmaker inbox (compliance).

- Lambda: `whatsapp-capture` (account 271378210266, us-east-1), invoked via its Function URL.
- Deploy: `.github/workflows/deploy.yml` runs tests, then zips ONLY `lambda_function.py` and
  runs `update-function-code --function-name whatsapp-capture` on push to `main`.
  Keep the package single-file; never change the function name.
- Tests: `python3 -m py_compile lambda_function.py && python3 -m unittest tests/test_suite.py`
  (boto3 mocked, no network, no boto3 install needed).

## Routes (one Function URL)
- `POST /?key=<WEBHOOK_KEY>` — TimelinesAI webhook. Always 200 on valid key; 403 otherwise.
- `GET /?key=<ADMIN_KEY>` — admin page. `&view=unparsed` shows the last 20 unparsed payloads.
- `POST /` form (`key`, `contact_key`, `action` = `client` | `personal` | `forward_held`) —
  admin actions, then 303 back to the page. GET never changes state.
- EventBridge scheduled event (`source: aws.events` / `detail-type: Scheduled Event`) —
  daily nudge email listing unsorted contacts (nothing sent if none).

## Env vars (names only)
`WEBHOOK_KEY`, `ADMIN_KEY`, `BUCKET`, `FROM_ADDR`, `TO_ADDR`, `FORWARDING_ENABLED` ("true"/"false"),
optional `CC_ADDR` (comma-separated; added as CC on forward/backlog emails).

## S3 layout (bucket = `BUCKET`)
- `contacts/<contact_key>.json` — key, name, phone, is_group, status, first_seen, last_seen,
  last_preview (140 chars), msg_count. Contact key = phone digits, or sanitized chat id
  (always chat id for groups).
- `messages/<contact_key>/<YYYYMMDDTHHMMSSZ>_<message_id>.json` — normalized message,
  `forwarded` / `forwarded_at`, attachments with their S3 keys.
- `raw/<contact_key>/<message_id>.json` — raw webhook body.
- `attachments/<contact_key>/<message_id>/<filename>` — downloaded at receipt (TimelinesAI URLs
  may expire), so backlog emails can attach them.
- `seen/<message_id>` — dedup marker.
- `unparsed/YYYY/MM/DD/<uuid>.json` — payloads the parser could not handle (or that raised).
- `state/sent-YYYY-MM-DD.json` — daily forward counter `{count, alerted}`.

## Statuses
- `unsorted` (default for new contacts): message + raw + attachments stored, not emailed.
- `client`: stored and forwarded immediately (one email per message).
- `personal`: nothing about the message is stored; only `last_seen` is bumped.

## Compliance rules
- Marking Personal deletes that contact's unforwarded messages, raw payloads, attachments and
  seen markers. Only the contact record (name, phone, status) remains.
- Forwarded messages are compliance records and are NEVER deleted (Client -> Personal deletes
  only unforwarded ones).
- Marking Client sends all held (forwarded:false) messages as ONE backlog email.
- Max 400 forward emails per UTC day; above that nothing is sent, messages stay
  forwarded:false, and one alert email goes to TO_ADDR. Held client messages (cap or
  FORWARDING_ENABLED off) can be sent later with the "Forward held" button on the admin page.
- Emails over 9 MB: attachments that don't fit are listed by URL as "not attached (size/download)".

## Payload parser is UNVERIFIED
The TimelinesAI Webhooks v2 payload shape has not been confirmed. `parse_payload` searches
several plausible key names (top level and under data/payload/message/chat/...). Until real
payloads are inspected via `?key=<ADMIN_KEY>&view=unparsed` (and raw/ objects), treat
direction, group detection, names and attachment URLs as best-effort. Update the parser and the
sample payloads in `tests/test_suite.py` once the real format is known.
