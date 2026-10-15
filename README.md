# rails-on-cloud

Laravel Cloud features for Ruby on Rails applications.

## Requirements

- Ruby 3.2+
- Rails / Active Job 7.1+

## Installation

```bash
bundle add rails-on-cloud
```

## Features

| Feature | Summary |
| --- | --- |
| [Managed Queues](#managed-queues) | Laravel Cloud managed queues as an Active Job backend. |

---

## Managed Queues

Run Active Job on Laravel Cloud managed queues. Laravel Cloud runs and scales
the queue workers for you. Throughput, duration and failed jobs appear in the
Laravel Cloud dashboard, and failed jobs can be retried from there.

### Setup

Use the `:cloud` adapter in production:

```ruby
# config/environments/production.rb
config.active_job.queue_adapter = :cloud
```

That's all. Laravel Cloud provides the rest.

### Dispatching jobs

Use Active Job as usual. A job's queue name is the managed queue it goes to;
jobs on Active Job's default queue go to your environment's default queue.

```ruby
class ProcessReportJob < ApplicationJob
  queue_as :critical
end

ProcessReportJob.perform_later(report)
ProcessReportJob.set(wait: 5.minutes).perform_later(report)
ActiveJob.perform_all_later(jobs)
```

- `wait:` / `wait_until:` can be at most 15 minutes; longer raises
  `LaravelCloud::Queue::InvalidDispatch`.
- `priority` isn't supported and is ignored.

### FIFO queues

A queue whose name ends in `.fifo` delivers jobs in order and deduplicates
them. It works without extra setup. To control the ordering group or the
deduplication key:

```ruby
class ProcessOrderJob < ApplicationJob
  include LaravelCloud::Queue::MessageAttributes

  queue_as "orders.fifo"

  message_group_id { "customer-#{arguments.first.customer_id}" }
  message_deduplication_id { "order-#{arguments.first.id}" }
end
```

FIFO queues don't support `wait:`.

### Fair queues

On a standard queue, a message group ID marks the tenant a job belongs to, so
one busy tenant can't starve the others:

```ruby
class SyncAccountJob < ApplicationJob
  include LaravelCloud::Queue::MessageAttributes

  message_group_id { "account-#{arguments.first.id}" }
end
```

### Retries and failures

`retry_on` and `discard_on` work as usual. Retry waits can be up to 12 hours.

```ruby
class ProcessReportJob < ApplicationJob
  retry_on Net::OpenTimeout, wait: :polynomially_longer, attempts: 5
end
```

A job's attempts are counted together across all its `retry_on` rules, rather
than per exception.

An exception that isn't handled fails the job. Failed jobs show up in the
Laravel Cloud dashboard, where you can retry them.

### Configuration

Optional, in an initializer:

```ruby
# config/initializers/laravel_cloud.rb
LaravelCloud::Queue.configure do |queue|
  # Only run these job classes (default: any Active Job class).
  queue.allowed_job_classes = %w[ProcessReportJob SendInvoiceJob]

  # Called when a job fails, e.g. to report it to an error tracker.
  queue.on_failure = ->(error, job_data) { Rails.error.report(error, context: job_data) }
end
```

### Local development

Managed queues only exist on Laravel Cloud, so use another adapter locally:

```ruby
# config/environments/development.rb
config.active_job.queue_adapter = :async
```

### Errors

| Error | Raised when |
| --- | --- |
| `LaravelCloud::Queue::InvalidDispatch` | A dispatch the queue can't honour: a wait over 15 minutes, a wait on a FIFO queue, or a deduplication ID on a standard queue. |
| `LaravelCloud::Queue::NotConfigured` | No managed queue is configured. |
| `ActiveJob::EnqueueError` | The job couldn't be sent to the queue. `perform_later` returns `false`, as with any adapter. |

---

## Development

```bash
bundle install
bundle exec rake test
```

## License

`rails-on-cloud` is open-sourced software licensed under the [MIT license](LICENSE.md).
