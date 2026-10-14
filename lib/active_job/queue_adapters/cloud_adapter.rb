# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    # The Active Job adapter for Laravel Cloud Managed Queues:
    #
    #   config.active_job.queue_adapter = :cloud
    #
    # A job's queue (`queue_as`) names the managed queue it is pushed to, and
    # Active Job's default queue maps to the environment's default managed
    # queue. Jobs are processed by `laravel-cloud queue:work`. SQS has no
    # notion of priority, so `priority` is ignored.
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

      # `ActiveJob.perform_all_later`, sent with SendMessageBatch.
      def enqueue_all(jobs)
        dispatcher.dispatch_all(jobs)
      end

      private

      # Resolved per call rather than at boot, so the adapter can be configured
      # in an app with no managed queue provisioned (it raises on enqueue).
      def dispatcher
        (@runtime || LaravelCloud::Queue.runtime).dispatcher
      end
    end
  end
end
