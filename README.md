PigFlow is a local HTTP control service for long-distance pipeline pigging operations.

## Recovery

Durable recovery restores journal, runs, routes, permits and pressure state in
that fixed order. A read-only dry run lets an operator see, before a real
restart, what a given snapshot would recover and exactly which stage would
block it:

```
GET /api/recovery/dry-run                  # canonical snapshot.json
GET /api/recovery/dry-run?snapshot=snapshot-2026-09-26.json
```

The endpoint never mutates runtime state. It re-reads the snapshot and the full
event journal from disk and returns, per stage, the recoverable runs, route
leases and permits together with blocking reasons (missing journal tail,
invalid runs, leases fenced by missing or terminal runs, permits without a
durable passage event, residual pressure during depressurization). Blocked
evidence returns `200` with `ready:false`; only access/decoding failures return
error statuses (`503` persistence not configured, `404` snapshot missing,
`422` corrupt snapshot/journal). Snapshot references must stay inside the
configured journal directory.

Formal recovery (`PIGFLOW_RECOVER=1`) re-reads the same evidence itself — a
dry-run verdict is never reused — applies the identical checks, and refuses to
publish anything if a stage is blocked. On success the new state is assembled
first, swapped in as a whole, and verified afterwards; a failed post-check rolls
the previous state back in, so a half-recovered state is never observable.
