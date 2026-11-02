# ingestion
Lightweight, non-blocking API gateway routing Edge audio payloads to the async processing queue.

## Life Dashboard webhook

`POST /life-dashboard` receives health data from the
[Life Dashboard Companion](https://github.com/owen282000/life-dashboard-companion-app) app. It is
capture only: nothing reads these files yet. In the app, set the URL on the Health tab and add
`Authorization: Bearer <LIFE_DASHBOARD_TOKEN>` under Webhook Headers. An unset token rejects everything.

Each payload is written unparsed to `{HEALTH_DIR}/YYYY-MM-DD/<sha256 of the body>.json`, one payload per
file, and answered with 204. A body of 1 KiB or less with `"test": true` is a ping and stores nothing.

The app's Screen Time tab stays off, so the folder holds health data only. The endpoint does not branch on
`source`: if that tab is ever pointed here, its payloads land in the same folder. `transcription-worker` and
`ai-worker` can read the files through their `/data` mount. On the host they are 0644 in 0755 folders, which
is fine while the owner is the node's only login.

Anyone reading the files must apply these rules:

- The folder is the UTC day the payload arrived, not the day of its records. `daily_totals`, `screen_time`
  (seven days in every sync) and backfills carry other days, so find records by the times inside them.
- A re-send after midnight makes a second file. Apply the app's *Deduplication and ordering* rules: the same
  `uuid` recurs when a source edits a record, `daily_totals` and `screen_time` are latest-`sequence`-wins
  per date, and `sequence` is per install.
- Nothing is validated, because a refusal costs the app that payload. Skip `*.tmp` files left by a killed
  write, and expect an empty file or the odd body that is not JSON from whoever holds the token.
- `deleted_records` is stored as data. A record removed in Health Connect stays here until removed by hand.

Rejected alternatives:

- Waiting for `O5`'s spike: Backfill could fetch the history later, but the owner asked for capture now.
- A hostPath of its own, or `subPath` on the workers: more manifest lines, and a `mv` can still move the data.
- 0600 files: the owner is the only login, and it would put `sudo` in front of `grep`.
- A body parameter: FastAPI would parse it for anonymous callers, so the body is read after auth instead.
- `<source>-<sequence>-<hash>` filenames: a reader must open every file regardless.
- SQLite: a table of blobs gives up `grep`, and an indexed row per record models a payload with no reader yet.
- `USAGE_TOKEN`: its own token lets the two phone apps be rotated independently.
- `X-Signature` HMAC: adds little over TLS on a write-only endpoint.
- A daily JSONL: a multi-megabyte append needs a lock, and a killed write can tear a whole day.
- The payload's `timestamp` for the folder: no client string reaches the path, and a bad one cannot fail.
- Parsing every body: it adds a way to OOM the pod, and a 422 loses the payload.
- A size limit below the app-wide 100 MiB: every 413 is a payload dropped after a week.
- `fsync`: a power cut can lose a payload, which a backfill can resend.
- A NATS event: nothing would consume it.
