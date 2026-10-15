# AGENTS.md

How this codebase works and what to keep in mind when changing it. User docs
are in `README.md`.

## What this is

A gem bringing Laravel Cloud features to Rails, the sibling of
`laravel/symfony-on-cloud`. The only feature so far is **Managed Queues**: an
Active Job adapter (`:cloud`) plus the worker Laravel Cloud runs
(`laravel-cloud queue:work`).

- `lib/laravel_cloud.rb` — shared pieces: the dashboard event socket (`Events`).
- `lib/laravel_cloud/queue/` — Managed Queues. Start with `dispatcher.rb`
  (sending) and `worker.rb` (receiving).
- `lib/active_job/queue_adapters/cloud_adapter.rb` — where Active Job looks up
  `:cloud`; it only delegates.
- `lib/laravel_cloud/cli.rb` — the `laravel-cloud` command.

New features get their own namespace beside `Queue` and report through
`LaravelCloud.events`.

## Things that are easy to break

**Retries reuse the message.** The worker makes the `Delivery` current while a
job runs. `retry_on` re-enqueues a dup with the same `job_id`, which the
`Dispatcher` recognises and turns into a release (visibility reset) of the
original message rather than a new one.

**Attempts come from SQS.** Since a released message keeps its original
payload, `Message.decode` sets `executions` from `ApproximateReceiveCount` and
clears `exception_executions`. Without that, `retry_on` would retry forever.
Attempts are per message: a freshly pushed message starts again at 1.

**The delivery cap needs `RetryAttempts`.** The worker fails a message
delivered more than `max(--tries, the job's retry_on attempts)` times.
`RetryAttempts` wraps `retry_on` to record those attempts; without it,
`--tries 1` would cut every `retry_on` short.

**The `failed_job` event is Cloud's contract.** It must match Laravel's
`Cloud\FailedJobProvider::log` (laravel/framework 13.x):

- exactly its fields, emitted before the `failed` queue event;
- a UUIDv7 `id` — Cloud's ClickHouse table derives the failure time from it;
- the full, untrimmed payload — Cloud pushes it back verbatim to retry the job;
- exception text forced to valid UTF-8 first, since an error there would lose
  the failed job.

`test/cloud_socket_test.rb` checks the bytes on a real socket.

**Dashboard retries happen on Cloud's side.** Cloud pushes the stored payload
straight to SQS; nothing runs in the app. That push must use
`{prefix}/{queue}{suffix}` (suffix before `.fifo`), send the payload verbatim,
and on FIFO queues set a `MessageGroupId` (the queue name) and a unique
`MessageDeduplicationId`.

**Without the agent, the worker reads SQS directly.** That isn't only for local
development: Cloud enables the agent only for some queues.

## Testing

```bash
bundle exec rake test
```

Tests use real fakes, not mocks of our own code: a stateful SQS behind the
real AWS SDK client (`test/support/fake_sqs.rb`), a real unix-socket agent
(`test/support/agent_stub.rb`), and real `retry_on` / `discard_on`. Prefer
end-to-end worker tests over asserting internals.

## Releasing

Publish a GitHub release tagged with the version (e.g. `v1.2.0`). The release
workflow sets `LaravelCloud::VERSION` from the tag and pushes to RubyGems via
trusted publishing. `version.rb` holds a `0.0.0` placeholder, so never publish
locally.

## Conventions

- Short comments that explain *why*, not what.
- Match Laravel's behaviour and formats wherever Cloud consumes them.
- Raise on a dispatch the queue can't honour rather than silently degrading.
