# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Optional settings, set from an initializer:
    #
    #   LaravelCloud::Queue.configure do |queue|
    #     queue.allowed_job_classes = %w[ProcessReportJob SendInvoiceJob]
    #     queue.on_failure = ->(error, job_data) { Rails.error.report(error) }
    #   end
    class Settings
      # Job class names the worker may run (nil: any Active Job). Checked before
      # constantizing, so other names never trigger autoloading.
      attr_reader :allowed_job_classes

      # Called with (error, job_data) when a job fails for good. Errors it
      # raises are logged and ignored.
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
