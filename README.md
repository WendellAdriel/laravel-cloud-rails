# rails-on-cloud

Laravel Cloud features for Ruby on Rails applications.

## Requirements

- Ruby 3.2+
- Rails / Active Job 7.1+

## Installation

```ruby
# Gemfile
gem "rails-on-cloud"
```

## Features

| Feature | Summary |
| --- | --- |
| [Managed Queues](#managed-queues) | Laravel Cloud managed queues as an Active Job backend. |

---

## Managed Queues

An Active Job adapter (`:cloud`) and worker for Laravel Cloud managed queues,
which are backed by Amazon SQS. Jobs report to the Laravel Cloud dashboard
(throughput, duration, failures, failed-job retry) the same way a Laravel app's
do.

- [Setup](#setup)
- [Dispatching jobs](#dispatching-jobs)
- [FIFO queues](#fifo-queues)
- [Fair queues](#fair-queues)
- [Retries and failures](#retries-and-failures)
- [Running workers](#running-workers)
- [Configuration](#configuration)
- [Local development](#local-development)
- [Reference](#reference)

### Setup

1. Use the adapter in production:

   ```ruby
   # config/environments/production.rb
   config.active_job.queue_adapter = :cloud
   ```

2. Run the worker as your queue process, from the application root:

   ```bash
   bundle exec laravel-cloud queue:work
   ```

Nothing else is required on Cloud. Queue locations, region, credentials and the
in-container agent all come from the environment Laravel Cloud provides (see
[Environment variables](#environment-variables)).

### Dispatching jobs

Use the standard Active Job API:

```ruby
ProcessReportJob.perform_later(report)
ProcessReportJob.set(wait: 5.minutes).perform_later(report)
ActiveJob.perform_all_later(jobs)
```

**Queues.** A job's Active Job queue name is the managed queue it's sent to.
Jobs on Active Job's default queue go to the environment's default managed
queue.

```ruby
class ProcessReportJob < ApplicationJob
  queue_as :critical                                         # → "critical"
end

ProcessReportJob.set(queue: :reports).perform_later(report)  # → "reports"
```

**Delays.** `wait:` / `wait_until:` become an SQS delivery delay, which SQS
caps at 15 minutes. A longer delay raises `LaravelCloud::Queue::InvalidDispatch`
instead of running the job early. FIFO queues reject any delay. Retry backoff
isn't subject to this limit; see [Retries and failures](#retries-and-failures).

**Batches.** `ActiveJob.perform_all_later` uses `SendMessageBatch` (10 messages
per request, grouped by queue). All jobs are validated before any are sent. A
job SQS rejects is left with `successfully_enqueued? == false` and an
`enqueue_error`.

**Priority** is not supported by SQS and is ignored.

### FIFO queues

A managed queue whose name ends in `.fifo` is an
[SQS FIFO queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html):
strict ordering within a message group, plus deduplication.

```ruby
class ProcessOrderJob < ApplicationJob
  queue_as "orders.fifo"
end
```

By default, the message group ID is the queue name and the deduplication ID is
unique per message. To set them yourself:

```ruby
class ProcessOrderJob < ApplicationJob
  include LaravelCloud::Queue::MessageAttributes

  queue_as "orders.fifo"

  message_group_id { "customer-#{arguments.first.customer_id}" }  # ordering scope
  message_deduplication_id { "order-#{arguments.first.id}" }      # dedupe key
end
```

Both accept a block (evaluated on the job) or a static value.

### Fair queues

On a standard queue, a message group ID turns on
[SQS fair queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues.html):
the group is a tenant key that SQS uses to stop one tenant's backlog from
starving others. Jobs in the same group still run in parallel; there's no
ordering or deduplication.

```ruby
class SyncAccountJob < ApplicationJob
  include LaravelCloud::Queue::MessageAttributes

  message_group_id { "account-#{arguments.first.id}" }
end
```

A `message_deduplication_id` on a standard queue raises `InvalidDispatch`.

### Retries and failures

**`retry_on`** works as usual:

```ruby
class ProcessReportJob < ApplicationJob
  retry_on Net::OpenTimeout, wait: :polynomially_longer, attempts: 5
end
```

A retry doesn't enqueue a copy of the job. The original SQS message is returned
to the queue and delivered again after the wait. As a result:

- no duplicate messages
- waits up to 12 hours (longer waits are capped at 12 hours)
- works on FIFO queues

Attempts are counted per message, by SQS deliveries. So a job with several
`retry_on` rules shares one attempt count across them rather than counting per
exception, and a `retry_on ..., queue:` that moves a job to another queue
starts its count again there.

**`discard_on`** works as usual. A discarded job counts as processed.

**Unhandled exceptions.** An exception no `retry_on` handles (or one that has
used up its attempts) is retried by the worker while `--tries` allows, after
`--backoff` seconds. After that the failure is terminal: the message is deleted,
the job appears as failed in the Laravel Cloud dashboard, and the
[`on_failure`](#configuration) hook is called. With the default `--tries 1`,
an unhandled exception is terminal straight away, which is Active Job's normal
behaviour.

**Attempt cap.** A message delivered more times than its job is allowed is
failed without running (`MaxAttemptsExceeded`). This catches jobs that crash or
hang the worker before it can record an outcome. A job's limit is the larger of
`--tries` and its highest `retry_on` `attempts:` value. `attempts: :unlimited`
means no cap.

**Never retried:** messages naming a job class that isn't a valid Active Job,
or one not in [`allowed_job_classes`](#configuration). A job class that isn't
defined in the worker's code is retried as usual, since a rolling deploy may
not have reached that worker yet. Messages that can't be decoded at all are
deleted and logged.

**Retrying from the dashboard.** Retrying a failed job in Laravel Cloud puts its
original message back on its queue, where it starts again at attempt 1. Nothing
runs in your app to do this. Active Job's enqueue callbacks don't fire for a
retried job, since it isn't enqueued again from Ruby.

### Running workers

```bash
bundle exec laravel-cloud queue:work [options]
```

The command boots the app from `config/environment.rb`, eager-loading it if
`config.eager_load` is on. Each worker process runs one job at a time; scale by
running more processes.

| Option | Default | Description |
| --- | --- | --- |
| `--tries N` | `1` | Total attempts for a job that raises an unhandled exception. Also sets the attempt cap. |
| `--backoff SECONDS` | `0` | Wait before the worker re-attempts a job that raised. |
| `--max-jobs N` | none | Exit after processing N jobs. |
| `--max-time SECONDS` | none | Exit after running this long. |
| `--shutdown-timeout SECONDS` | `15` | After a stop signal, how long the running job gets to finish. |
| `--queue NAME` | default queue | Queues to poll, in order, when receiving from SQS directly. Repeat or comma-separate. |
| `--stop-when-empty` | off | Exit when there's nothing left to process. |

**Receiving.** On Laravel Cloud the worker gets jobs from the in-container
Cloud agent, which is tied to one queue, so `--queue` has no effect there.
Without the agent (locally), the worker reads straight from SQS, polling each
`--queue` in turn.

**Shutdown.** On `SIGTERM`/`SIGINT` the worker finishes the current job and
exits. A job still running when `--shutdown-timeout` runs out is interrupted
and its message is returned for immediate redelivery; that delivery counts as an
attempt. An idle worker exits immediately.

**Exit status.** `0` on a normal stop. `1` if the worker can't continue: the
Cloud agent is unreachable or returns errors, or no managed queue is
configured. On Cloud, a non-zero exit restarts the container.

### Configuration

None is required. Optional settings, in an initializer:

```ruby
# config/initializers/laravel_cloud.rb
LaravelCloud::Queue.configure do |queue|
  # Only run these job classes. Names are checked before any class is loaded.
  # Default: any ActiveJob::Base subclass.
  queue.allowed_job_classes = %w[ProcessReportJob SendInvoiceJob]

  # Called on terminal failures with the error and the job's serialized data.
  # Exceptions raised by the handler are logged and ignored.
  queue.on_failure = ->(error, job_data) { Rails.error.report(error, context: job_data) }
end
```

### Local development

There's no managed queue locally, so enqueueing to `:cloud` raises
`NotConfigured`. Use another adapter outside production:

```ruby
# config/environments/development.rb
config.active_job.queue_adapter = :async
```

To work against a real managed queue from your machine, set
`LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` and AWS credentials (the SDK's default
credential chain is used). Then:

```bash
bundle exec laravel-cloud queue:work --queue critical,default --stop-when-empty
```

### Reference

#### Commands

| Command | Description |
| --- | --- |
| `laravel-cloud queue:work [options]` | Run a worker (see [Running workers](#running-workers)). |

#### Environment variables

Laravel Cloud sets these; you only set them yourself when running outside Cloud.

| Variable | Purpose |
| --- | --- |
| `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` | Queue locations, region, credentials mode and agent settings (JSON). |
| `LARAVEL_CLOUD_AGENT_SOCKET` | Agent socket path, if the config doesn't give one. Default `/tmp/cloud-agent.sock`. |
| `LARAVEL_CLOUD_LOG_SOCKET` | Where dashboard events are sent. Default `unix:///tmp/cloud-init.sock`. |

#### Errors

| Error | Raised when |
| --- | --- |
| `LaravelCloud::Queue::InvalidDispatch` | Dispatching something SQS can't do: a delay over 15 minutes, any delay on a FIFO queue, a deduplication ID on a standard queue. |
| `LaravelCloud::Queue::NotConfigured` | Enqueueing or working with no managed queue configured. |
| `ActiveJob::EnqueueError` | SQS rejects the message or can't be reached. `perform_later` returns `false` and sets `job.enqueue_error`, as with any adapter. |

---

## Development

```bash
bundle install
bundle exec rake test
```

## License

`rails-on-cloud` is open-sourced software licensed under the [MIT license](LICENSE.md).
