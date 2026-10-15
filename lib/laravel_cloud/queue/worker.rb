# frozen_string_literal: true

require "logger"

module LaravelCloud
  module Queue
    # Runs managed-queue jobs (`laravel-cloud queue:work`). Receives one message
    # at a time — from the Cloud agent, or straight from SQS without one — and
    # reports exactly one outcome per message:
    #
    #   - success / discard        → delete              ("processed")
    #   - `retry_on` retrying      → visibility reset    ("released")
    #   - an exception, tries left → visibility reset    ("released")
    #   - an exception, no tries   → delete + failed job ("failed")
    #
    # `tries` / `backoff` are Laravel's `queue:work` options; the default of one
    # try keeps Active Job's behaviour (an escaping exception is terminal).
    # `tries` also caps deliveries, so a job that crashes the worker can't come
    # back forever; a job's `retry_on` attempts raise its cap (RetryAttempts).
    class Worker
      # The message has been delivered more times than its job may be attempted.
      class MaxAttemptsExceeded < LaravelCloud::Error; end

      # Raised into the worker thread to abandon a long-poll or an overrunning
      # job at shutdown. Not a StandardError, so `retry_on` and a job's own
      # `rescue` don't treat it as a failure.
      class Shutdown < Exception; end # rubocop:disable Lint/InheritException

      SIGNALS = %w[TERM INT].freeze

      # `queues` only applies without the agent, which is pinned to one queue.
      # `shutdown_timeout` is how long a running job gets after a stop request
      # before it's abandoned and released.
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

      # Finish the current job, then exit. An idle long-poll (up to ~55s) is
      # abandoned so shutdown isn't held up; a message received at that exact
      # moment is redelivered by SQS after its visibility timeout.
      def stop
        @stop_requested_at ||= monotonic
        @thread.raise(Shutdown) if @polling && @thread
      end

      def stopping?
        !@stop_requested_at.nil?
      end

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
          # Release now so another worker can take it immediately. As with a
          # killed Laravel job, this counts as an attempt.
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
      # the job, not the outcome reporting. A job finishing at that same instant
      # may run again; SQS is at-least-once anyway.
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

        # Terminal: delete it (the agent has no "failed" outcome) and report the
        # failure through observability.
        @logger.error("[LaravelCloud] #{job_data['job_class']} (#{delivery.message_id}) failed: #{error.class}: #{error.message}")
        acknowledge(delivery)
        observer.failed(delivery, error, started_at, observer.now)
        notify_failure(error, job_data)
      end

      # Whether another delivery could turn out differently.
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

        # The queue the message actually came from, for metrics.
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

      # Polls each queue in turn and takes the first message found.
      def receive_from_sqs
        queue_names.each do |name|
          queue_url = config.queue_url_for(name)
          raise NotConfigured, "No Laravel Cloud managed queue is configured (LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is missing)." if queue_url.nil?

          message = @runtime.sqs.receive_message(
            queue_url: queue_url,
            max_number_of_messages: 1,
            wait_time_seconds: 20,
            message_attribute_names: ["All"],
            # The attempt counter (see Message.decode).
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

      # Have SQS redeliver the original message after `delay` seconds.
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

      # An unavailable agent ends the worker (it only recovers on restart); a
      # rejected outcome only concerns this message, so it's just logged.
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

      # Enforces the shutdown timeout: signal handlers can't wait or lock, so
      # they only note the stop time and this thread watches the clock.
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
        # Not on the main thread (e.g. in tests).
        nil
      end

      def restore_signals(handlers)
        handlers.each { |signal, handler| Signal.trap(signal, handler || "DEFAULT") }
      end
    end
  end
end
