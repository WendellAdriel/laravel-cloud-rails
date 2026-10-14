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
worker) and `queue:retry <url>` (retrying a failed job from the dashboard).

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
       ├─ FailedJobRetrier         `queue:retry`: fetch, decrypt (LaravelEncrypter, APP_KEY), re-push
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
   data, so `Message.decode` rebuilds `executions` from SQS's
   `ApproximateReceiveCount` and **nils `exception_executions`** — Active Job
   then falls back to the overall `executions` count in `retry_on`. Without
   this, retries never end. Consequence (documented in the README): attempts
   are counted across all `retry_on` rules, Laravel-style.
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

### Failed-job retry

Cloud hands `queue:retry` an https URL whose body is the `failed_job` event,
encrypted with `APP_KEY` (Laravel's Encrypter format; CBC+HMAC or GCM, read
from the payload). The payload is pushed back verbatim (fresh receive count)
and a `failed_job` event with `retried_at` is emitted. Payloads trimmed by the
collector workaround (`data_truncated`) are refused, not pushed.

### Observability

`Observer` emits `queued` · `started` · `processed` · `released` · `failed`
(`_cloud_event: "queue"`, `"Y-m-d H:i:s.u"` UTC timestamps) and `failed_job`
(attempts = receive count). The `MAX_*_BYTES` trimming of `failed_job` is a
**temporary workaround** for the log collector's 16 KiB CRI line limit,
documented in-file — not permanent.

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
  shutdown; `failed_job_retry_test.rb` round-trips a real failed job through
  the encrypter and back onto the queue. Prefer adding there over asserting
  internals.

## Conventions to keep

- **Heavy explanatory comments** on non-obvious code: explain *why* (SQS
  limits, Laravel/Symfony parity, ordering constraints), not the mechanics.
- **Mirror Laravel's SQS semantics** and the Symfony package's wire formats
  where there's a choice — the dashboard and agent expect them.
- **Fail loudly** on an unsatisfiable dispatch rather than silently degrading.
- **One observability channel** (`LaravelCloud.events`).
