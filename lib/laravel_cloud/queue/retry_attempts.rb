# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Records the most attempts any `retry_on` on a job class allows, so the
    # worker's attempt cap (see Worker) never cuts off a retry Active Job was
    # asked to make.
    #
    # The cap counts SQS deliveries, and because retries release the original
    # message, every `retry_on` retry is another delivery of it. A worker-wide
    # `--tries 1` on its own would therefore fail a `retry_on ..., attempts: 5`
    # job on its second delivery. Active Job doesn't expose a job's retry
    # policy (`retry_on` compiles to a `rescue_from` handler), so this wraps
    # `retry_on` to note its `attempts:` — the only thing it does; the
    # arguments go through to Active Job untouched.
    module RetryAttempts
      # Active Job's own default for `retry_on`'s `attempts:`.
      DEFAULT_ATTEMPTS = 5

      def retry_on(*exceptions, attempts: DEFAULT_ATTEMPTS, **options, &block)
        limit = attempts == :unlimited ? Float::INFINITY : attempts.to_i
        self.cloud_retry_attempts = [cloud_retry_attempts, limit].max

        super
      end
    end

    ActiveSupport.on_load(:active_job) do
      # The most deliveries a `retry_on` on this class (or an ancestor) allows;
      # 0 when it has none. Infinity for `attempts: :unlimited`.
      class_attribute :cloud_retry_attempts, instance_accessor: false, default: 0

      ActiveJob::Exceptions::ClassMethods.prepend(RetryAttempts)
    end
  end
end
