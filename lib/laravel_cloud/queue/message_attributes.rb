# frozen_string_literal: true

require "active_support/concern"

module LaravelCloud
  module Queue
    # SQS message group and deduplication IDs for a job, for FIFO and fair
    # queues. Active Job has no per-dispatch options beyond queue/wait/priority,
    # so these are declared on the job class and resolved against each job:
    #
    #   class ProcessOrderJob < ApplicationJob
    #     include LaravelCloud::Queue::MessageAttributes
    #
    #     queue_as "orders.fifo"
    #
    #     message_group_id { "customer-#{arguments.first.customer_id}" }
    #     message_deduplication_id { "order-#{arguments.first.id}" }
    #   end
    #
    # What a message group means follows the queue the job lands on: on a
    # ".fifo" queue it is the ordering scope; on a standard queue it is a fair
    # queues tenant key (no ordering, no deduplication). A deduplication ID is
    # FIFO-only and raises on a standard queue rather than being ignored.
    #
    # Each takes a value or a block, evaluated in the context of the job.
    module MessageAttributes
      extend ActiveSupport::Concern

      included do
        class_attribute :cloud_message_group_id_resolver, instance_writer: false
        class_attribute :cloud_message_deduplication_id_resolver, instance_writer: false
      end

      class_methods do
        def message_group_id(value = nil, &block)
          self.cloud_message_group_id_resolver = block || value
        end

        def message_deduplication_id(value = nil, &block)
          self.cloud_message_deduplication_id_resolver = block || value
        end
      end

      def cloud_message_group_id
        resolve_cloud_attribute(cloud_message_group_id_resolver)
      end

      def cloud_message_deduplication_id
        resolve_cloud_attribute(cloud_message_deduplication_id_resolver)
      end

      private

      def resolve_cloud_attribute(resolver)
        value = resolver.is_a?(Proc) ? instance_exec(&resolver) : resolver
        value&.to_s
      end
    end
  end
end
