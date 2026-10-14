# frozen_string_literal: true

module LaravelCloud
  module Queue
    # Resolves the job class a message names, before anything is constantized
    # on its say-so.
    module JobClass
      # An (optionally namespaced) constant path, and nothing else.
      NAME_PATTERN = /\A[A-Z]\w*(::[A-Z]\w*)*\z/

      # The message names something that can never run here: not a constant
      # path, outside the allowlist, or not an Active Job. Terminal — retrying
      # can't change the answer.
      class Unrunnable < LaravelCloud::Error; end

      # The message names a job class this process doesn't have — typically a
      # rolling deploy, where an older worker receives a job class that only
      # the new release defines. Retryable: a later delivery may land on a
      # worker that has it.
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
