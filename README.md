# rails-on-cloud

Laravel Cloud features for Ruby on Rails applications.

- [Requirements](#requirements)
- [Installation](#installation)
- [Features](#features)
- [Managed Queues](#managed-queues)
- [WebSockets](#websockets)
- [Development](#development)
- [License](#license)

## Requirements

- Ruby 3.2+
- Rails / Active Job 7.1+

## Installation

Install the gem from GitHub:

```bash
bundle add rails-on-cloud --git https://github.com/laravel/rails-on-cloud.git
```

## Features

| Feature | Summary |
| --- | --- |
| [Managed Queues](#managed-queues) | Laravel Cloud managed queues as an Active Job backend. |
| [WebSockets](#websockets) | Action Cable and AnyCable bridge to Laravel Cloud managed WebSockets. |

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

## WebSockets

Use existing Action Cable channels and browser clients with a Pusher-compatible
WebSocket backend. The gem runs channel actions in Rails and sends broadcasts
through the backend. You can keep the AnyCable browser client and Rails channel
state accessors.

### Setup

Attach a managed WebSocket resource to your Laravel Cloud environment. Cloud
provides the connection settings through these environment variables:

| Variable | Value |
| --- | --- |
| `PUSHER_APP_ID` | WebSocket application ID. |
| `PUSHER_APP_KEY` | Public application key. |
| `PUSHER_APP_SECRET` | Application secret used to sign requests. |
| `PUSHER_HOST` | WebSocket server hostname. |
| `PUSHER_PORT` | WebSocket server port. |
| `PUSHER_SCHEME` | `http` or `https`. |

When all six variables are nonempty, the gem selects its Action Cable adapter
and injects its browser transport into your HTML pages. No bridge initializer,
changes to `config/cable.yml`, or Pusher JavaScript dependency are needed.
The application secret stays on the server.

### Action Cable channels

Use your existing [Action Cable](https://guides.rubyonrails.org/v8.1/action_cable_overview.html)
channel APIs. For example, this channel accepts subscriptions to a demo room:

```ruby
class RoomChannel < ApplicationCable::Channel
  def subscribed
    return reject unless params[:room] == "demo"

    stream_from "room:demo"
  end

  def speak(data)
    ActionCable.server.broadcast("room:demo", { text: data.fetch("text") })
  end
end
```

Connection authentication still belongs in `ApplicationCable::Connection#connect`.
The bridge calls your connection and channel code, including subscription
rejections, lifecycle callbacks, and periodic transmissions. Bridge commands
use same-origin HTTP requests and a signed HttpOnly cookie issued by the gem.
Your existing Rails session cookies remain available to connection authentication.

Keep your existing Action Cable client. With the standard `@rails/actioncable`
package loaded by your application, a subscription looks like this:

```js
import { createConsumer } from '@rails/actioncable';

const consumer = createConsumer();
const room = consumer.subscriptions.create(
    { channel: 'RoomChannel', room: 'demo' },
    {
        received(message) {
            console.log(message.text);
        },
    },
);
```

After the subscription connects, call channel actions as usual:

```js
room.perform('speak', { text: 'Hello from Action Cable' });
```

### Broadcasting

Broadcast from your existing controllers or jobs:

```ruby
ActionCable.server.broadcast("room:demo", { text: "Your report is ready" })
```

Rails receives the stream through a private Pusher channel, applies channel
callbacks and coders, and forwards each subscriber's frames through a separate
private channel. `stream_for`, model broadcasts, and Turbo Streams use the same
adapter. Existing channel prefixes are preserved.

### AnyCable clients

If your application already uses `@anycable/web`, keep its client API:

```js
import { createCable } from '@anycable/web';

const consumer = createCable();
const room = consumer.subscribeTo('RoomChannel', { room: 'demo' });

room.on('message', (message) => {
    console.log(message.text);
});
```

After the subscription connects, call `room.perform('speak', { text: 'Hello' })`.
Applications using `anycable-rails-core` may also keep `state_attr_accessor` in
their channels. Channels using `stream_from "room:demo", whisper: true` can
receive `room.whisper(...)` through the AnyCable Ruby extension.

### Connections and recovery

The gem routes commands to the Rails process that owns the connection through
the WebSocket backend. Multiple Puma workers and application replicas use the
same six variables. No separate Redis connection is required by the bridge.

Shared worker heartbeats keep browser clients connected. Each browser connection
renews its server lease every 30 seconds; inactive connections expire after
90 seconds. When Rails or the backend restarts, the standard clients reconnect
and subscribe again.

`remote_connections.where(...).disconnect` uses Rails' existing connection
identifiers. Call it when logging out a user or revoking access to stop future
delivery to their connection.

The HTML transport loads before application scripts and uses the page's CSP
nonce. The gem adds the managed WebSocket origin to `connect-src`. Clients in
Web Workers or other contexts without HTML use Rails' native `/cable` transport
and the same Cloud broadcast adapter.

### Local development

Without all six `PUSHER_*` variables, the bridge stays disabled and your existing
Action Cable adapter remains in use.

To exercise the bridge locally, provide all six variables for a Pusher-compatible
server. `PUSHER_HOST` and `PUSHER_PORT` must be reachable from both Rails and the
browser.

## Development

```bash
bundle install
bundle exec rake test
```

## License

`rails-on-cloud` is open-sourced software licensed under the [MIT license](LICENSE.md).
