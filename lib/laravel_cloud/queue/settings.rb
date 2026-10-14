# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Optional, app-level Managed Queues settings, set from an initializer:
    #
    #   LaravelCloud::Queue.configure do |queue|
    #     queue.allowed_job_classes = %w[ProcessReportJob SendInvoiceJob]
    #     queue.on_failure = ->(error, job_data) { Rails.error.report(error) }
    #   end
    #
    # Nothing here is required — the injected Cloud config covers the rest.
    class Settings
      # Job class names the worker may run, or nil (the default) for any
      # ActiveJob::Base subclass. Checked as strings *before* the class name
      # from the message is constantized, so a name outside the list never
      # triggers autoloading.
      attr_reader :allowed_job_classes

      # Called with (error, job_data) when a job fails terminally — the place to
      # report it to an error tracker. `job_data` is the job's serialized hash
      # (nil if the message couldn't be decoded that far). Errors raised by the
      # handler are logged and swallowed.
      attr_accessor :on_failure

      def initialize
        @allowed_job_classes = nil
        @on_failure = nil
      end

      def allowed_job_classes=(classes)
        @allowed_job_classes = classes.nil? ? nil : Array(classes).map(&:to_s)
      end
    end
  end
end
