# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    # `config.active_job.queue_adapter = :cloud`. SQS has no priority, so
    # `priority` is ignored.
    class CloudAdapter < (defined?(AbstractAdapter) ? AbstractAdapter : Object)
      def initialize(runtime: nil)
        super()
        @runtime = runtime
      end

      def enqueue(job)
        dispatcher.dispatch(job, delay: 0)
      end

      def enqueue_at(job, timestamp)
        dispatcher.dispatch(job, delay: timestamp - Time.now.to_f)
      end

      def enqueue_all(jobs)
        dispatcher.dispatch_all(jobs)
      end

      private

      # Resolved per call, so the app boots without a managed queue.
      def dispatcher
        (@runtime || LaravelCloud::Queue.runtime).dispatcher
      end
    end
  end
end
