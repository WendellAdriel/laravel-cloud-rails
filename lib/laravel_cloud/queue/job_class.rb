# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Validates the job class a message names before constantizing it.
    module JobClass
      NAME_PATTERN = /\A[A-Z]\w*(::[A-Z]\w*)*\z/

      # Can never run here, so not retried.
      class Unrunnable < LaravelCloud::Error; end

      # Not defined here, e.g. mid-deploy. Retried, as another worker may have it.
      class NotDefined < LaravelCloud::Error; end

      def self.resolve(name, allowed: nil)
        name = name.to_s

        raise Unrunnable, "#{name.inspect} is not a valid job class name." unless NAME_PATTERN.match?(name)
        raise Unrunnable, "#{name} is not in the allowed job classes." if allowed && !allowed.include?(name)

        klass = begin
          Object.const_get(name)
        rescue NameError
          raise NotDefined, "The job class #{name} is not defined."
        end

        raise Unrunnable, "#{name} is not an Active Job class." unless klass.is_a?(Class) && klass < ActiveJob::Base

        klass
      end
    end
  end
end
