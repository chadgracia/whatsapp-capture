# whatsapp-capture

Single-file AWS Lambda (`lambda_function.py`, Python 3.12, stdlib + boto3 only) that receives
WhatsApp messages from TimelinesAI webhooks, lets Chad sort each contact as Client or Personal
on an admin page, and emails every Client message to the archived Rainmaker inbox (compliance).
Forwarding is a **once-a-day batch** (one email per Client contact), started by Chad clicking
"Send now" on the admin page daily; the webhook never sends email.
Each forwarded contact is identified by full name + company from the Pipeline CRM cache.

- Lambda: `whatsapp-capture` (account 271378210266, us-east-1), invoked via its Function URL.
- Deploy: `.github/workflows/deploy.yml` runs tests, then zips ONLY `lambda_function.py` and
  runs `update-function-code --function-name whatsapp-capture` on push to `main`.
  Keep the package single-file; never change the function name.
- Tests: `python3 -m py_compile lambda_function.py && python3 -m unittest tests/test_suite.py`
  (boto3 mocked, no network, no boto3 install needed).

## Routes (one Function URL)
- `POST /?key=<WEBHOOK_KEY>` — TimelinesAI webhook. Always 200 on valid key; 403 otherwise.
- `GET /?key=<ADMIN_KEY>` — admin page. `&view=unparsed` shows the last 20 unparsed payloads.
- `POST /` form (`key`, `action`, optional `contact_key`) — admin actions, then 303 back:
  `client` | `personal` | `set_identity` (per contact; `full_name`, `company`, `individual`),
  `bulk_personal` (`contact_keys` comma-separated, max 100; only `unsorted` contacts change),
  `send_now` (run the daily batch), `rebuild_index`. GET never changes state (the admin page
  computes CRM identity in memory only).
- Scheduled event with input `{"task": "daily_forward"}` — same batch as "Send now" (optional;
  no schedule is configured — Chad sends manually). Any other EventBridge scheduled event (`source: aws.events` /
  `detail-type: Scheduled Event`) — daily nudge email listing unsorted contacts.

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
- `state/sent-YYYY-MM-DD.json` — daily email counter `{count, alerted}`.
- `state/phone-index.json` — CRM phone index `{built_at, people_scanned, phones_indexed,
  people_with_no_phone, top_phone_keys, skipped_non_phone_values, index: {last9: {person_id, full_name, company} |
  {ambiguous: [ids]}}}`.
- `state/batch-last.json` — last batch stats; `state/batch-lock.json` — 15-min run lock.

Contact records also carry `wa_name` (WhatsApp display name; older records used `name` and are
migrated on write), `crm_match` (`matched`|`ambiguous`|`none`), `crm_person_id`,
`crm_full_name`, `crm_company`, and optional `manual_full_name` / `manual_company` /
`manual_individual`. Messages carry `sender_phone` (used to identify group senders).

## Statuses
- `unsorted` (default for new contacts): message + raw + attachments stored, not emailed.
- `client`: stored with forwarded:false; sent by the next daily batch.
- `personal`: nothing about the message is stored; only `last_seen` is bumped.

## Compliance rules
- Marking Personal deletes that contact's unforwarded messages, raw payloads, attachments and
  seen markers. Only the contact record (name, phone, status) remains.
- Forwarded messages are compliance records and are NEVER deleted (Client -> Personal deletes
  only unforwarded ones).
- Daily batch: for each Client contact with forwarded:false messages, ONE email with all of
  them in chronological order (split into "(part 2)", ... to stay under 9 MB — a message is
  never dropped; an attachment that can't fit is listed as "not attached (size/download)").
  Each part is marked forwarded only after it sends, so failures retry next run and re-runs
  never double-send. Unsorted and Personal contacts are never sent.
- Max 400 emails per UTC day; above that nothing is sent, messages stay forwarded:false, and
  one alert email goes to TO_ADDR. FORWARDING_ENABLED != "true" → batch sends nothing.

## CRM identity
- Source: `s3://full-pipeline-cache/people.json` (`{"people": [...]}`, ~113 MB) and
  `companies.json` (`{"companies": [...]}`); the role can only GetObject these two keys.
  The index is rebuilt ONLY by the admin "Rebuild index" button (the batch just reads
  `state/phone-index.json`); that button needs Lambda memory ~3 GB and a multi-minute timeout.
  Contacts added to the CRM after the last rebuild show "[not in CRM]" until the next rebuild.
- Match on the last 9 digits of the phone. Two different people on one key → ambiguous.
- Identity precedence: manual override > CRM match > fallback `<WhatsApp name> [not in CRM]`.
  Missing identity never blocks forwarding.
- Phone fields are **verified** against people.json: `phone`, `mobile`, `home_phone`
  (`work_phone` also read if present). Top-level allowlist only — never scan by substring
  (e.g. `image_mobile_url` is a photo URL). Values containing "http", "/" or "@", or with
  fewer than 7 / more than 15 digits, are skipped and counted in `skipped_non_phone_values`.

## Payload parser is UNVERIFIED
The TimelinesAI Webhooks v2 payload shape has not been confirmed. `parse_payload` searches
several plausible key names (top level and under data/payload/message/chat/...). Until real
payloads are inspected via `?key=<ADMIN_KEY>&view=unparsed` (and raw/ objects), treat
direction, group detection, names and attachment URLs as best-effort. Update the parser and the
sample payloads in `tests/test_suite.py` once the real format is known.

## SECURITY RULES — MANDATORY (read before any change)

This repo is PUBLIC, and so are its GitHub Actions logs. Secrets have leaked from Chad's repos 5-6 times. Never again.

1. No secret value ever goes in code, tests, docs, comments, commit messages, PR text or logs. That includes API keys, admin keys, HMAC/signing secrets, tokens, passwords, and any URL containing `key=`, `token=` or `sso=`. Secrets come from `os.environ[...]` (Lambda environment variables) or `s3://pipeline-token/` only. Tests use obvious fakes like `"test-key"`.
2. Every `aws lambda ...` command in a workflow must end with `> /dev/null`. Lambda API responses include every environment variable, and the logs are public. Never print the environment (`printenv`, `env`, `set -x`, or `print(os.environ)`).
3. Never log or print a secret, even partially, or a request URL that carries one.
4. `.github/scripts/secret_guard.py` runs automatically before every `git commit` and `git push` (via `.claude/settings.json`), and again in CI. If it blocks you, remove the secret and move the value to an environment variable. NEVER bypass, weaken, delete or skip the guard, and never use `--no-verify` or `secret-guard: allow` to get past it without Chad's explicit approval.
5. A new secret needs Chad to add a Lambda environment variable first. Tell him the exact variable name. Code that reads it ships after that.
6. Before finishing, run `python3 .github/scripts/secret_guard.py --tree` and report the result.
7. NEVER build anything that asks for, collects, uploads, stores, emails or processes client identity data — tax IDs, SSNs, passports, driver's licenses, other government IDs, or dates of birth. Chad does not want or need it; identity verification is handled by Rainmaker's own forms. If a task seems to need it, stop and ask Chad.
