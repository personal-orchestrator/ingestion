# ingestion
Lightweight, non-blocking API gateway routing Edge audio payloads to the async processing queue.

## Life Dashboard webhook

`POST /life-dashboard` receives health data from the
[Life Dashboard Companion](https://github.com/owen282000/life-dashboard-companion-app) app. It is
capture only: nothing reads these files yet. In the app, set the URL on the Health tab and add
`Authorization: Bearer <LIFE_DASHBOARD_TOKEN>` under Webhook Headers. An unset token rejects everything.

Each payload is written unparsed to `{HEALTH_DIR}/YYYY-MM-DD/<sha256 of the body>.json`, one payload per
file, and answered with 204. A body of 1 KiB or less with a truthy `test` field is a ping and stores nothing.

Anyone reading the files must apply these rules:

- The folder is the UTC day the payload arrived, not the day of its records. `daily_totals` and backfills
  carry other days, so find records by the times inside them.
- A re-send after midnight makes a second file. Apply the app's *Deduplication and ordering* rules: the same
  `uuid` recurs when a source edits a record, `daily_totals` is latest-`sequence`-wins per date, and
  `sequence` is per install.
- Nothing is validated, because a refusal costs the app that payload. Skip `*.tmp` files left by a killed
  write, and expect the odd body that is not JSON from whoever holds the token.
- `deleted_records` is stored as data. A record removed in Health Connect stays here until removed by hand.

Rejected alternatives:

- SQLite: a table of blobs gives up `grep`, and an indexed row per record models a payload with no reader yet.
- `USAGE_TOKEN`: its own token lets the two phone apps be rotated independently.
- `X-Signature` HMAC: adds little over TLS on a write-only endpoint.
- A daily JSONL: a multi-megabyte append needs a lock, and a killed write can tear a whole day.
- The payload's `timestamp` for the folder: no client string reaches the path, and a bad one cannot fail.
- Parsing every body: it adds a way to OOM the pod, and a 422 loses the payload.
- A size limit below the app-wide 100 MiB: every 413 is a payload dropped after a week.
- `fsync`: a power cut can lose a payload, which a backfill can resend.
- A NATS event: nothing would consume it.
