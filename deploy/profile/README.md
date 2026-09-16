# Gateway request profiles v1

Data path: gateway /metrics -> vmagent -> VictoriaMetrics -> independent query API -> code-eval Web. Only expanded detailed analysis reads the gateway. No profile tables, report database, collection control or model inference in the Web layer.

Enable `profile` in gateway config; defaults: listen 127.0.0.1:18082, API key env AIGATE_PROFILE_KEY, directory relative to config, 72h/10GiB event retention, 100000 index entries, 2048 queued events, 4MiB maximum profiled payload. Profiling limits do not reject business requests. New profile records contain only structural sizes, keyed cumulative message-prefix hashes and usage. Existing audit question_prefix is a separate log.

Use a private reachable bind at deployment and distribute a separate observation credential to vmagent and the platform server. HMAC key is generated as a mode-0600 file in the profile directory; keep it stable across restarts. No credential, request ID, hash, task ID or timestamp label is exported to VM. A maximum of 64 backend/model metric groups is admitted; excess goes to overflow.

Semantics: first increment is gateway arrival to first nonempty content, reasoning or tool delta, not backend TTFT. Missing usage remains absent. Streaming cumulative usage is replaced, never summed. Cached/input totals share a denominator cohort. Prefix repeat counters compare supported fixed message levels with the preceding one hour of observations, across query boundaries. A cold/evicted index is explicitly not ready for a full hour. Concurrent input matching follows observation order; detailed analysis sorts requests by arrival. Both remain approximate message-level observations, never exact tokenizer/KV claims.

Detailed API: authenticated GET /profile/v1/status, POST /profile/v1/analyses {start,end,backend?,model?}, GET /profile/v1/analyses/{id}. Range uses UTC epoch seconds [start,end), <=24h within retained events. One worker, four queued jobs, 16 cached jobs, 10min cache TTL, 30s execution deadline, 100000 requests / 256MiB scan budget. No event/hash export. Results include quality, pending and unknown outcomes. VM queries support the existing 30-day retention and remain available when events expire.

Files are rotated each minute or at 4MiB, synchronized every second. Abrupt process termination can lose a tail; detailed results never claim durable exactly-once capture. After restart, old incomplete requests are unknown and prefix history warms anew. No schema migration back to retired profiles.

SSE profile outcomes validate error events and stream completion independently of management mode, without changing forwarded bytes. Detailed `pending` counts only requests in the live set captured at `pending_as_of`; missing end records for other requests are `unknown`, including queue/write losses. The event cutoff remains `as_of`. Shared-prefix statistics include only requests with observed prefix marks; quality reports `shared_prefix_observed` and `shared_prefix_missing`. A valid observed prefix with no repeat remains zero; unobserved prefixes do not enter the distribution.

The 10 GiB limit is the current code/configuration default. Existing explicit `max_disk_bytes` settings remain effective until updated and the gateway restarted. The deployment report below describes the earlier 2 GiB deployment, not acceptance of these review fixes.

Segment sidecars record min/max arrival time and covered bytes. Analysis skips a segment only when the sidecar covers its entire file; an active/crashed tail is scanned conservatively. The as_of boundary also filters lifecycle events. Query-time scan limits therefore apply to intersecting segments, not the entire 72h store.

After restart, the analysis cutoff includes the retained events that predate startup, even when filesystem mtime precision rounds the last event's file timestamp down. Live writes still advance the cutoff with their completed-write time.

VM cumulative counters include process/group birth gauges. Queries use increase_pure only when a zero origin inside the requested window is established; otherwise increase is used. This avoids dropping startup token totals while retaining correct historical/reset behavior. Recent queries use an explicit 1ms latency offset and mark the latest samples as potentially incomplete; ingestion is still asynchronous.

A single <=16MiB counters-metric-v2.json checkpoint is written on graceful shutdown and consumed on startup. It preserves cumulative counters and their zero-origin time across normal restarts, including resets that would otherwise be invisible between two scrapes. It is operational metadata, not metric history. Unclean exits can still lose an unsampled tail and reset counters; such boundary uncertainty remains part of capture quality.

Current events use events-v2-*.jsonl. General request metrics include streaming and non-streaming requests with request_scope="all"; first-increment and live stream observations use request_scope="streaming". Non-streaming arrivals also have their own request_scope="nonstreaming" counter. Status and analysis responses declare scope_policy="metric-v2". Legacy counters/events remain untouched for rollback and are not restored, read, or counted toward the new generation's retention limit; account for their disk space separately. See the [deployment checks](../README.md).
