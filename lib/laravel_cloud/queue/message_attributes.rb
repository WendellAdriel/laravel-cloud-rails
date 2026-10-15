# frozen_string_literal: true

require "active_support/concern"

module LaravelCloud
  module Queue
    # Message group and deduplication IDs for FIFO and fair queues, declared on
    # the job class (a value, or a block evaluated on the job):
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
    # On a FIFO queue the group is the ordering scope; on a standard queue it's
    # a fair-queues tenant key. Deduplication is FIFO-only.
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
