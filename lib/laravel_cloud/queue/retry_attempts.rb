# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Records the highest `retry_on` `attempts:` on a job class, so the
    # worker's delivery cap never cuts a retry short: retries reuse the
    # message, so each one is another delivery. Active Job doesn't expose this,
    # so `retry_on` is wrapped; its arguments pass through untouched.
    module RetryAttempts
      DEFAULT_ATTEMPTS = 5 # Active Job's own default

      def retry_on(*exceptions, attempts: DEFAULT_ATTEMPTS, **options, &block)
        limit = attempts == :unlimited ? Float::INFINITY : attempts.to_i
        self.cloud_retry_attempts = [cloud_retry_attempts, limit].max

        super
      end
    end

    ActiveSupport.on_load(:active_job) do
      # 0 without `retry_on`; Infinity for `attempts: :unlimited`.
      class_attribute :cloud_retry_attempts, instance_accessor: false, default: 0

      ActiveJob::Exceptions::ClassMethods.prepend(RetryAttempts)
    end
  end
end
