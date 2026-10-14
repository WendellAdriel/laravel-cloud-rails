# frozen_string_literal: true

require "logger"

module LaravelCloud
  module Queue
    # Processes managed-queue jobs — what `laravel-cloud queue:work` runs.
    #
    # Active Job has no worker of its own, so this is the consumer half of the
    # adapter. It receives one message at a time — from the in-container
    # cloud-agent on Laravel Cloud, or straight from SQS locally — runs it
    # through Active Job, and reports exactly one outcome per message:
    #
    #   - success / discard        → delete              ("processed")
    #   - `retry_on` retrying      → visibility reset    ("released")
    #   - an exception, tries left → visibility reset    ("released")
    #   - an exception, no tries   → delete + failed job ("failed")
    #
    # `tries` and `backoff` are Laravel's `queue:work` options. `tries` (default
    # 1) is how many times a job that raises is attempted in all; with the
    # default an escaping exception is terminal, as it is everywhere in Active
    # Job, and retries are opted into per job with `retry_on`.
    #
    # `tries` also caps deliveries: a message delivered more times than its job
    # may be attempted fails as MaxAttemptsExceeded without running. That stops
    # a job that never finishes a delivery — one that crashes or hangs the
    # worker — from coming back forever. A job's `retry_on` attempts raise its
    # cap to match (see RetryAttempts), so the cap never cuts a retry short.
    class Worker
      # The message has been delivered more times than its job may be attempted.
      class MaxAttemptsExceeded < LaravelCloud::Error; end

      # Raised into the worker thread to abandon a long-poll, or a job still
      # running when the shutdown timeout lapses. An Exception (not a
      # StandardError) so neither `retry_on` nor a job's own `rescue` treats it
      # as a job failure.
      class Shutdown < Exception; end # rubocop:disable Lint/InheritException

      SIGNALS = %w[TERM INT].freeze

      # - `queues`: the managed queues to poll on the SQS-direct path, in
      #   order; empty means the environment's default queue. On Laravel Cloud
      #   the agent is pinned to its queue and serves it regardless.
      # - `tries` / `backoff`: see above. `backoff` is in seconds.
      # - `max_jobs` / `max_time`: stop after that many jobs / seconds.
      # - `shutdown_timeout`: seconds a running job gets to finish after a stop
      #   is requested before it's abandoned and released for redelivery.
      # - `stop_when_empty`: exit once a poll finds nothing — for draining a
      #   queue locally or in CI.
      def initialize(queues: [], tries: 1, backoff: 0, max_jobs: nil, max_time: nil, shutdown_timeout: 15,
                     stop_when_empty: false, runtime: Queue.runtime, logger: nil)
        @queues = queues
        @tries = [tries.to_i, 1].max
        @backoff = backoff.to_i.clamp(0, Dispatcher::MAX_VISIBILITY_SECONDS)
        @max_jobs = max_jobs
        @max_time = max_time
        @shutdown_timeout = shutdown_timeout
        @stop_when_empty = stop_when_empty
        @runtime = runtime
        @logger = logger || ActiveJob::Base.logger || Logger.new($stdout)
        @stop_requested_at = nil
        @polling = false
        @executing = false
        @processed = 0
      end

      def run
        @thread = Thread.current
        @started_at = monotonic
        previous_handlers = trap_signals
        watchdog = start_watchdog

        @logger.info("[LaravelCloud] Processing managed queue jobs #{source_description}")

        until stopping? || limit_reached?
          delivery = poll

          if delivery.nil?
            break if @stop_when_empty

            next
          end

          process(delivery)
          @processed += 1
        end
      rescue Shutdown
        # Shut down mid-poll; nothing was taken.
      ensure
        watchdog&.kill
        restore_signals(previous_handlers) if previous_handlers
        @logger.info("[LaravelCloud] Stopped processing managed queue jobs")
      end

      # Finish the job in hand (within the shutdown timeout), then exit. When
      # idle in a long-poll (up to ~55s against the agent) the poll is
      # abandoned so shutdown isn't held up; a message that lands in the instant
      # between the poll returning and this interrupt is simply never processed
      # here, and SQS redelivers it once its visibility timeout lapses.
      def stop
        @stop_requested_at ||= monotonic
        @thread.raise(Shutdown) if @polling && @thread
      end

      def stopping?
        !@stop_requested_at.nil?
      end

      # Run one received message through Active Job and report its outcome.
      def process(delivery)
        job_data = begin
          Message.decode(delivery.body, receive_count: delivery.receive_count)
        rescue Message::Undecodable => e
          # Drop the poison message so it isn't redelivered forever.
          @logger.error("[LaravelCloud] Deleting undecodable managed queue message #{delivery.message_id}: #{e.message}")
          acknowledge(delivery)
          return
        end

        delivery.bind(job_data)

        started_at = observer.now
        observer.started(delivery.queue, started_at)

        begin
          job_class = JobClass.resolve(job_data["job_class"], allowed: @runtime.settings.allowed_job_classes)
          enforce_max_attempts!(delivery, job_class)
          execute(delivery, job_data)
        rescue Shutdown
          # Abandoned at shutdown: hand it straight back so another worker can
          # pick it up now, rather than after its visibility timeout. Like a
          # killed Laravel job, the abandoned delivery counts as an attempt.
          @logger.warn("[LaravelCloud] Shutdown timeout exceeded; releasing #{job_data['job_class']} (#{delivery.message_id}) for redelivery")
          release(delivery, 0)
          observer.released(delivery.queue, started_at, observer.now)
          return
        rescue StandardError => e
          handle_failure(delivery, job_data, e, started_at)
          return
        end

        if delivery.released?
          release(delivery, delivery.release_delay)
          observer.released(delivery.queue, started_at, observer.now)
        else
          acknowledge(delivery)

          if delivery.retried?
            observer.released(delivery.queue, started_at, observer.now)
          else
            observer.processed(delivery.queue, started_at, observer.now)
          end
        end
      end

      private

      def config
        @runtime.config
      end

      def observer
        @runtime.observer
      end

      # The watchdog only interrupts while @executing, so the interrupt lands in
      # the job rather than in the outcome reporting around it — bar a job that
      # finishes in the same instant, whose message is then redelivered and run
      # again (SQS delivery is at-least-once regardless).
      def execute(delivery, job_data)
        @executing = true
        Delivery.within(delivery) { ActiveJob::Base.execute(job_data) }
      ensure
        @executing = false
      end

      def enforce_max_attempts!(delivery, job_class)
        limit = [@tries, job_class.cloud_retry_attempts].max

        return if delivery.receive_count <= limit

        raise MaxAttemptsExceeded, "#{job_class.name} has been attempted too many times (#{delivery.receive_count} deliveries, limit #{limit})."
      end

      def handle_failure(delivery, job_data, error, started_at)
        if retryable?(error) && delivery.receive_count < @tries
          release(delivery, @backoff)
          observer.released(delivery.queue, started_at, observer.now)
          return
        end

        # Terminal: delete it (the agent's "processed" — there is no "failed"
        # outcome) and surface the failure through observability instead.
        @logger.error("[LaravelCloud] #{job_data['job_class']} (#{delivery.message_id}) failed: #{error.class}: #{error.message}")
        acknowledge(delivery)
        observer.failed(delivery, error, started_at, observer.now)
        notify_failure(error, job_data)
      end

      # Whether another delivery could turn out differently. A job class that
      # can never run here, or a message out of attempts, can't.
      def retryable?(error)
        !error.is_a?(JobClass::Unrunnable) && !error.is_a?(MaxAttemptsExceeded)
      end

      def notify_failure(error, job_data)
        @runtime.settings.on_failure&.call(error, job_data)
      rescue StandardError => e
        @logger.error("[LaravelCloud] The on_failure handler raised #{e.class}: #{e.message}")
      end

      def poll
        @polling = true
        config.agent_available? ? receive_from_agent : receive_from_sqs
      ensure
        @polling = false
      end

      def receive_from_agent
        data = @runtime.agent.next_message
        return nil if data.nil?

        message_id = data["messageId"]
        return nil unless message_id.is_a?(String) && !message_id.empty?

        # The agent reports the real queue URL the message came from; the
        # outcome is reported, and metrics attributed, against that queue.
        queue_url = data["queueUrl"].is_a?(String) && !data["queueUrl"].empty? ? data["queueUrl"] : config.queue_url
        attributes = data["attributes"].is_a?(Hash) ? data["attributes"] : {}

        Delivery.new(
          message_id: message_id,
          receipt_handle: data["receiptHandle"].is_a?(String) ? data["receiptHandle"] : nil,
          queue_url: queue_url,
          queue: config.normalize_queue(queue_url),
          body: data["body"].to_s,
          receive_count: attributes["ApproximateReceiveCount"] || 1,
          from_agent: true,
        )
      end

      # Poll each requested queue in turn, taking the first message found.
      def receive_from_sqs
        queue_names.each do |name|
          queue_url = config.queue_url_for(name)
          raise NotConfigured, "No Laravel Cloud managed queue is configured (LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is missing)." if queue_url.nil?

          message = @runtime.sqs.receive_message(
            queue_url: queue_url,
            max_number_of_messages: 1,
            wait_time_seconds: 20,
            message_attribute_names: ["All"],
            # ApproximateReceiveCount is the attempt counter for retries.
            message_system_attribute_names: ["ApproximateReceiveCount"],
          ).messages.first

          next if message.nil?

          return Delivery.new(
            message_id: message.message_id,
            receipt_handle: message.receipt_handle,
            queue_url: queue_url,
            queue: config.normalize_queue(queue_url),
            body: message.body.to_s,
            receive_count: message.attributes&.fetch("ApproximateReceiveCount", nil) || 1,
            from_agent: false,
          )
        end

        nil
      end

      def queue_names
        @queues.empty? ? [config.queue] : @queues
      end

      def acknowledge(delivery)
        if delivery.from_agent?
          report(delivery, "processed")
        elsif delivery.receipt_handle
          @runtime.sqs.delete_message(queue_url: delivery.queue_url, receipt_handle: delivery.receipt_handle)
        end
      end

      # Release the original message for SQS to redeliver after `delay` seconds.
      # It is never re-sent, so this is unaffected by the 15-minute delay cap
      # and works identically on FIFO queues.
      def release(delivery, delay)
        if delivery.from_agent?
          report(delivery, "released", delay: delay)
        elsif delivery.receipt_handle
          @runtime.sqs.change_message_visibility(
            queue_url: delivery.queue_url,
            receipt_handle: delivery.receipt_handle,
            visibility_timeout: delay,
          )
        end
      end

      # An Unavailable agent escapes and ends the worker (it only recovers on a
      # restart); a rejected outcome concerns this one message, so it's logged
      # and the worker moves on.
      def report(delivery, status, delay: nil)
        @runtime.agent.report(delivery.message_id, delivery.receipt_handle, status, delay: delay)
      rescue AgentClient::Rejected => e
        @logger.error("[LaravelCloud] #{e.message} (message #{delivery.message_id}, outcome #{status})")
      end

      def limit_reached?
        if @max_jobs && @processed >= @max_jobs
          @logger.info("[LaravelCloud] Processed #{@processed} jobs; stopping (max jobs)")
          return true
        end

        if @max_time && monotonic - @started_at >= @max_time
          @logger.info("[LaravelCloud] Ran for #{@max_time}s; stopping (max time)")
          return true
        end

        false
      end

      # Enforces the shutdown timeout. A signal handler can't wait or take a
      # lock, so it only notes when the stop was asked for; this thread watches
      # the clock and interrupts a job that outlives the timeout.
      def start_watchdog
        return nil if @shutdown_timeout.nil?

        Thread.new do
          loop do
            sleep 0.25

            next unless stopping? && @executing && monotonic - @stop_requested_at >= @shutdown_timeout

            @thread.raise(Shutdown)
            break
          end
        end
      end

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def source_description
        if config.agent_available?
          "from the Laravel Cloud agent"
        else
          "directly from SQS (#{queue_names.join(', ')})"
        end
      end

      def trap_signals
        SIGNALS.to_h { |signal| [signal, Signal.trap(signal) { stop }] }
      rescue ArgumentError
        # Not on the main thread (e.g. embedded in tests); signals stay as-is.
        nil
      end

      def restore_signals(handlers)
        handlers.each { |signal, handler| Signal.trap(signal, handler || "DEFAULT") }
      end
    end
  end
end
