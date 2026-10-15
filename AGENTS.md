# AGENTS.md

Orientation for AI agents (and humans) working in this repository. For end-user
usage docs see `README.md`; this file is about how the codebase is built and the
conventions to keep when changing it.

## What this package is

`rails-on-cloud` is a **gem that brings Laravel Cloud features to a Rails app**.
It is the Rails sibling of `laravel/symfony-on-cloud` and deliberately mirrors
its design and wire protocols: a thin host owning a few shared platform
primitives, with each capability self-contained beside it.

The **only feature today is Managed Queues** — Laravel Cloud's managed SQS
queues as an Active Job adapter (`config.active_job.queue_adapter = :cloud`)
plus the `laravel-cloud` executable Laravel Cloud runs: `queue:work` (the
worker). Failed jobs are retried by Laravel Cloud itself, which pushes them
straight to SQS (see "Dashboard retries").

- Ruby `>= 3.2`, Active Job `>= 7.1`, `aws-sdk-sqs` `~> 1.80`.
- Module `LaravelCloud` under `lib/laravel_cloud/`; the adapter must live at
  `ActiveJob::QueueAdapters::CloudAdapter` for Active Job's `:cloud` lookup.

## Layout

```
LaravelCloud                       ← thin host (lib/laravel_cloud.rb)
  ├─ Events                        log socket (LARAVEL_CLOUD_LOG_SOCKET) — the one observability channel
  ├─ CLI                           `laravel-cloud` (exe/) — boots the app from config/environment.rb
  └─ Queue                         Managed Queues
       ├─ Runtime                  lazily-built collaborators (config, SQS, agent, observer, dispatcher, settings)
       ├─ Settings                 optional app settings: allowed_job_classes, on_failure
       ├─ ManagedQueueConfig       parses LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG; queue URLs (FIFO-aware)
       ├─ AgentClient              in-container cloud-agent over its unix socket (GET /next, POST /result)
       ├─ Dispatcher               push path behind the adapter (FIFO / fair queues / delays / retry interception / batches)
       ├─ Worker                   receive → execute → report one outcome per message; tries, cap, shutdown
       ├─ JobClass                 validates the message's job class before constantizing it
       ├─ RetryAttempts            wraps `retry_on` to record its `attempts:` for the delivery cap
       ├─ Delivery                 one received message + the retry verdict recorded while it runs
       ├─ Message                  payload codec + attempt-counter bridge
       ├─ Observer                 queue lifecycle + failed_job events
       └─ MessageAttributes        job concern for message group / deduplication IDs
ActiveJob::QueueAdapters::CloudAdapter   thin; delegates to the Dispatcher
```

**To add a feature:** give it its own namespace beside `Queue`, build on
`LaravelCloud.events` for metrics, and don't add host-level plumbing for one
feature.

## Managed Queues — how it works

### The two receive paths

Pushing **always** goes straight to SQS. Receiving depends on environment:

- **On Laravel Cloud**: the cloud-agent sidecar long-polls SQS and hands the
  worker messages over its unix socket; the worker reports the outcome back and
  the agent performs the SQS delete / visibility change. Selected by the
  injected config's `agent.enabled` flag (`agent_available?`), not by probing
  the socket.
- **Locally / no agent**: the worker receives from SQS directly and deletes /
  changes visibility itself.

Agent outcomes are `processed` (delete) and `released` (visibility reset).
There is no `failed` outcome: terminal failures are reported `processed` and
the failure is emitted separately as an observability event.

### Payload

`{ uuid: job_id, displayName: job class, data: job.serialize }`. `uuid` /
`displayName` mirror Laravel's payload so the dashboard can name a failed job;
decoding reads only `data`.

### Retries (the core design)

Retries are **Active Job-native (`retry_on`), executed as SQS visibility
resets** — no re-push, no duplicate:

1. **Interception.** The worker makes the `Delivery` current while the job runs
   (`ActiveSupport::IsolatedExecutionState`). `retry_job` re-enqueues a `dup`
   with the **same job_id**, so the Dispatcher recognises it and records a
   release (backoff ceil'd to whole seconds, clamped to 12h) instead of pushing.
   The worker performs it after the job returns.
2. **Counter bridge.** A released message comes back with its original job
   data, so `Message.decode` sets `executions` from SQS's
   `ApproximateReceiveCount` alone (receive N → N-1) and **nils
   `exception_executions`** — Active Job then falls back to the overall
   `executions` count in `retry_on`. Without this, retries never end.
   Attempts are therefore **per message**: any count stored in the payload is
   ignored, so a freshly pushed message (a dashboard retry, a queue move)
   starts again at 1, and attempts are shared across all `retry_on` rules,
   Laravel-style. Both are documented in the README.
3. **Moving queues.** `retry_on ..., queue:` changes the job's `queue_name`
   from the one it arrived with; that can't be a release, so it's pushed to the
   new queue and the original deleted. The release is recorded first so a
   failed push still retries in place.

An exception escaping `perform` is re-attempted (released after `--backoff`)
while the delivery count is under `--tries` (default 1 — i.e. terminal, Active
Job semantics); then it's terminal: delete, emit `failed` + `failed_job`, call
`on_failure`. An unrunnable job class (bad name, not allowed, not an Active
Job) and MaxAttemptsExceeded are terminal regardless; an *undefined* class is
retryable (rolling deploys).

### Delivery cap

A message whose receive count exceeds `max(--tries, job's retry_on attempts)`
fails as `MaxAttemptsExceeded` without running (Laravel's guard against jobs
that crash/hang the worker and never report). Because retries release the
same message, every `retry_on` retry is a delivery — hence `RetryAttempts`
recording the class's highest `attempts:` (`:unlimited` → no cap). Don't drop
that wrapper without replacing how the cap learns a job's retry policy.

### Shutdown

`SIGTERM`/`SIGINT` set a flag (signal handlers can't lock or wait). An idle
long-poll is interrupted immediately; a running job gets `--shutdown-timeout`,
enforced by a watchdog thread that raises `Worker::Shutdown` (an `Exception`,
so `retry_on`/`rescue` don't catch it) into the job; its message is released
with 0 delay. The abandoned delivery counts as an attempt.

### Dashboard retries (Laravel Cloud's side)

The app runs nothing to retry a failed job: Laravel Cloud pushes the stored
`failed_job` payload straight onto SQS. There's no Active Job hook to honour
(no `JobRetryRequested` / `retryUntil` equivalent), so nothing is lost by
skipping the app. For it to work, Cloud's push must:

- go to `{prefix}/{queue}{suffix}`, with the suffix *before* `.fifo` on FIFO
  queues — `ManagedQueueConfig#queue_url_for`, Laravel's `SqsQueue` rule;
- send the stored `payload` **verbatim** — the worker reads `data` and ignores
  any stored attempt count (see the counter bridge), so no editing is needed;
- on a `.fifo` queue, set `MessageGroupId` (the queue name) and a unique
  `MessageDeduplicationId` — the job's own group ID isn't kept.

The stored payload is the message body exactly as it sat on SQS: the worker
emits it whole in `failed_job`, never trimmed.

### Failed-job reporting

The `failed_job` event the worker emits matches `FailedJobProvider::log`
(laravel/framework 13.x): exactly its fields, including `job_name` and
`exception_preview`; a UUIDv7 `id`; and it's emitted *before* the `failed`
lifecycle event. Exception text is coerced to valid UTF-8 (invalid bytes →
U+FFFD, PHP's `JSON_INVALID_UTF8_SUBSTITUTE`) before anything is built from it,
because a raise there happens after the message is deleted and would lose the
failed job. `cloud_socket_test.rb` checks the real bytes on a real socket.

Not ported: Laravel's optional `exception_id` on `failed_job`, which links to
the separate Cloud Exceptions feature (`LARAVEL_CLOUD_EXCEPTIONS`,
`Cloud\ExceptionReporter`). That belongs with an exceptions feature, if one is
added.

### Observability

`Observer` emits `queued` · `started` · `processed` · `released` · `failed`
(`_cloud_event: "queue"`, `"Y-m-d H:i:s.u"` UTC timestamps) and `failed_job`
(attempts = receive count). `failed_job` carries the full payload and
exception, untrimmed, as Laravel's `FailedJobProvider::log` does. If a large
event ever runs into the log pipeline's line limits (containerd splits lines
over 16 KiB), fix it there or deliberately here, not by quietly trimming the
payload, which Cloud needs whole to retry the job.

### Dispatch rules

Mirroring the Symfony transport's `send()`: FIFO queues get default group
(queue name) and dedupe (fresh UUID) IDs and reject any delay; a delay over
900s raises; dedupe IDs on a standard queue raise; a message group on a
standard queue is a fair-queue tenant key. SQS errors on push become
`ActiveJob::EnqueueError` (Active Job's convention); misuse raises
`InvalidDispatch` / `NotConfigured` loudly.

## Testing

```bash
bundle install
bundle exec rake test
```

**Real fakes, not mocks of our own code:**

- **SQS**: `test/support/fake_sqs.rb` — a stateful queue behind the real AWS
  SDK client (`stub_responses`), so parameter validation is real and receive
  counts, deletes and visibility resets behave like SQS.
- **Agent**: `test/support/agent_stub.rb` — a real HTTP server on a unix socket.
- **Events**: a real `UNIXServer` (`events_test.rb`); worker tests use a
  recording sink.
- **Retries**: `worker_test.rb` / `agent_worker_test.rb` drive real `retry_on`
  / `discard_on` end to end; `worker_tries_test.rb` covers tries, the cap and
  shutdown; `cloud_socket_test.rb` checks the failed-job event on a real
  socket. Prefer adding there over asserting internals.

## Conventions to keep

- **Heavy explanatory comments** on non-obvious code: explain *why* (SQS
  limits, Laravel/Symfony parity, ordering constraints), not the mechanics.
- **Mirror Laravel's SQS semantics** and the Symfony package's wire formats
  where there's a choice — the dashboard and agent expect them.
- **Fail loudly** on an unsatisfiable dispatch rather than silently degrading.
- **One observability channel** (`LaravelCloud.events`).
