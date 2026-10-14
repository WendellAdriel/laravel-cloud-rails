# frozen_string_literal: true

require "active_support/isolated_execution_state"

module LaravelCloud
  module Queue
    # One received managed-queue message, from the moment the worker takes it
    # until it reports the outcome. It carries the SQS coordinates the outcome
    # is reported against, plus the retry verdict the adapter records while the
    # job runs.
    #
    # The worker makes the delivery *current* for the duration of the job, which
    # is how the adapter recognises Active Job's retry re-enqueue: `retry_on`
    # (and a manual `retry_job`) re-enqueues a dup of the running job, which
    # keeps its job_id. Rather than pushing a duplicate message, the adapter
    # records a release here and the worker resets the *original* message's
    # visibility once the job returns — the SQS-native equivalent of Laravel's
    # `$job->release($delay)`.
    class Delivery
      STATE_KEY = :laravel_cloud_queue_delivery

      attr_reader :message_id, :receipt_handle, :queue_url, :queue, :body, :receive_count, :job_id, :job_queue_name,
                  :release_delay

      # Run the block with this delivery as the one being processed.
      def self.within(delivery)
        previous = current
        ActiveSupport::IsolatedExecutionState[STATE_KEY] = delivery
        yield
      ensure
        ActiveSupport::IsolatedExecutionState[STATE_KEY] = previous
      end

      def self.current
        ActiveSupport::IsolatedExecutionState[STATE_KEY]
      end

      # `receive_count` is SQS's ApproximateReceiveCount: how many times this
      # message has been delivered, mirroring Laravel's SqsJob::attempts().
      def initialize(message_id:, receipt_handle:, queue_url:, queue:, body:, receive_count:, from_agent:)
        @message_id = message_id
        @receipt_handle = receipt_handle
        @queue_url = queue_url
        @queue = queue
        @body = body
        @receive_count = [receive_count.to_i, 1].max
        @from_agent = from_agent
        @job_id = nil
        @job_queue_name = nil
        @release_delay = nil
        @superseded = false
      end

      def from_agent?
        @from_agent
      end

      # Record which job this message carries, from its decoded job data.
      def bind(job_data)
        @job_id = job_data["job_id"]
        @job_queue_name = job_data["queue_name"]
        self
      end

      # Whether an enqueue of `job` is a retry of the job this delivery is
      # running, rather than a fresh dispatch of some other job.
      def retry_of?(job)
        !job_id.nil? && job.job_id == job_id
      end

      # Ask for the original message to be released back to SQS after `delay`
      # seconds instead of being deleted.
      def release!(delay)
        @release_delay = delay
        @superseded = false
      end

      # The retry was pushed as a fresh message onto a different queue,
      # so the original is finished with and should be deleted.
      def supersede!
        @release_delay = nil
        @superseded = true
      end

      def released?
        !@release_delay.nil?
      end

      # Whether the job asked to run again, whichever way that was honoured.
      def retried?
        released? || @superseded
      end
    end
  end
end
