# Event schema (version 1)

The SDK sends batches to `POST /v1/events`:

```json
{
  "schema_version": 1,
  "sent_at": "2026-11-11T10:30:01.000000Z",
  "sdk": { "name": "pulsetrace.python", "version": "0.1.0" },
  "events": [ ... ]
}
```

Headers: `Content-Type: application/json`, `Content-Encoding: gzip`,
`Authorization: Bearer <api_key>` (when configured),
`User-Agent: pulsetrace-python/<version>`.

## Common fields

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | `1`; the collector rejects versions it does not know |
| `event_id` | string | 32 lowercase hex (UUID4); the deduplication key |
| `timestamp` | string | RFC 3339 UTC with microseconds, always `Z` |
| `type` | `error` \| `message` \| `transaction` | |
| `level` | `debug` \| `info` \| `warning` \| `error` \| `fatal` | |
| `platform` | string | `python` |
| `service`, `environment`, `release` | string | release may be `null` |
| `server_name` | string | hostname, optional |
| `sdk`, `runtime` | object | name/version |
| `trace` | object | `trace_id` (32 hex), `span_id` (16 hex), `parent_span_id`, `sampled` |
| `request` | object | `method`, `route` (template), `status_code`, `duration_ms`, `headers` (allowlisted); `path`, `query`, `client_ip` only with `send_default_pii` |
| `breadcrumbs` | array | `{timestamp, type, category, level, message?, data?}`, oldest first |
| `tags` | object | string → string |
| `user`, `extra` | object | JSON values, normalised and size-limited |
| `fingerprint` | array of strings | optional explicit grouping override |

Unknown fields are allowed and preserved by the collector, so newer SDKs keep
working against older collectors.

## `error` events

```json
"exception": {
  "mechanism": { "type": "asgi", "handled": false },
  "values": [
    {
      "id": 0, "parent_id": null, "relation": null,
      "type": "PaymentProcessingError", "module": "examples.payment_service.main",
      "message": "Payment processing failed",
      "stacktrace": {
        "frames": [
          {
            "filename": "examples/payment_service/main.py",
            "abs_path": "/app/examples/payment_service/main.py",
            "module": "examples.payment_service.main",
            "function": "checkout", "lineno": 129, "in_app": true,
            "context_line": "            raise PaymentProcessingError(\"Payment processing failed\") from exc",
            "pre_context": ["..."], "post_context": ["..."]
          }
        ]
      }
    },
    { "id": 1, "parent_id": 0, "relation": "cause", "type": "HTTPStatusError", "module": "httpx", "message": "...", "stacktrace": { "frames": [] } }
  ]
}
```

* `values[0]` is the captured exception. Others link to their parent through
  `parent_id`, with `relation` set to `cause` (`raise ... from`), `context`
  (raised while handling) or `group` (member of an `ExceptionGroup`, with
  `group_index`).
* Frames are ordered oldest call first, crash site last. Over-long traces keep
  the first and last frames and report `frames_omitted`.
* `notes` carries `BaseException.__notes__`; `truncated: true` marks chains that
  exceeded `max_exceptions`.

## `message` events

`message` (string) instead of `exception`. Messages logged with arguments carry
the unformatted template as their `fingerprint`.

## `transaction` events

```json
"transaction": {
  "name": "POST /checkout", "op": "http.server",
  "start_timestamp": "2026-11-11T10:30:00.000000Z",
  "duration_ms": 51.7, "status": "ok",
  "spans": [
    {
      "span_id": "a1b2c3d4e5f60718", "parent_span_id": "0f1e2d3c4b5a6978",
      "op": "http.client", "name": "POST http://fraud-service:8001/score",
      "start_timestamp": "2026-11-11T10:30:00.004000Z", "duration_ms": 43.3,
      "status": "ok", "data": { "http.status_code": 200 }
    }
  ],
  "dropped_spans": 0
}
```

`trace.span_id` is the transaction's own span id; its `parent_span_id` is the
caller's span when the request carried a `traceparent`. Span `status` values
follow a small vocabulary (`ok`, `internal_error`, `cancelled`, `not_found`,
`unavailable`, `resource_exhausted`, ...) derived from HTTP status codes or
exceptions.

## Collector response

```json
{ "accepted": 3, "duplicates": 1, "rejected": 1,
  "errors": [ { "index": 4, "event_id": "...", "error": "timestamp: Input should have timezone info" } ] }
```

| Status | Meaning | SDK behaviour |
|---|---|---|
| 200 | batch processed (possibly partially rejected) | delivered |
| 400 | malformed envelope | drop, no retry |
| 401 | missing/invalid API key | drop, configuration error |
| 413 | body, decompressed body or batch too large | split batch |
| 415 | unsupported `Content-Encoding` | drop |
| 429 | rate limited, with `Retry-After` | pause, retry |
| 503 | storage unavailable, with `Retry-After` | pause, retry |
