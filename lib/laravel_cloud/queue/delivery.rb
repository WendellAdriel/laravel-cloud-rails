# frozen_string_literal: true

require "active_support/isolated_execution_state"

module LaravelCloud
  module Queue
    # A received message and its outcome so far.
    #
    # The worker makes it *current* while the job runs. `retry_on` re-enqueues
    # a dup of the job with the same job_id, so the adapter can recognise the
    # retry and record a release here instead of pushing a duplicate — the
    # equivalent of Laravel's `$job->release($delay)`.
    class Delivery
      STATE_KEY = :laravel_cloud_queue_delivery

      attr_reader :message_id, :receipt_handle, :queue_url, :queue, :body, :receive_count, :job_id, :job_queue_name,
                  :release_delay

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

      # `receive_count` is SQS's ApproximateReceiveCount.
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

      def bind(job_data)
        @job_id = job_data["job_id"]
        @job_queue_name = job_data["queue_name"]
        self
      end

      # Whether enqueueing `job` is a retry of the running job.
      def retry_of?(job)
        !job_id.nil? && job.job_id == job_id
      end

      def release!(delay)
        @release_delay = delay
        @superseded = false
      end

      # The retry was pushed to another queue, so delete the original.
      def supersede!
        @release_delay = nil
        @superseded = true
      end

      def released?
        !@release_delay.nil?
      end

      def retried?
        released? || @superseded
      end
    end
  end
end
