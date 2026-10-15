# frozen_string_literal: true

require "aws-sdk-sqs"

module LaravelCloud
  # Laravel Cloud Managed Queues for Active Job. Jobs are pushed straight to
  # SQS, and received through the Cloud agent or, without one, from SQS.
  module Queue
    # LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is missing.
    class NotConfigured < LaravelCloud::Error; end

    # A dispatch the queue can't honour, raised rather than silently degraded.
    class InvalidDispatch < LaravelCloud::Error; end

    # What the adapter and worker share. Built lazily, so an app boots without
    # a managed queue; injectable for tests.
    class Runtime
      attr_reader :settings

      def initialize(config: nil, sqs: nil, agent: nil, events: nil, settings: Settings.new)
        @config = config
        @sqs = sqs
        @agent = agent
        @events = events
        @settings = settings
      end

      def config
        @config ||= ManagedQueueConfig.from_env
      end

      def sqs
        @sqs ||= build_sqs_client
      end

      def agent
        @agent ||= AgentClient.new(config.agent_socket)
      end

      def observer
        @observer ||= Observer.new(@events || LaravelCloud.events)
      end

      def dispatcher
        @dispatcher ||= Dispatcher.new(self)
      end

      private

      def build_sqs_client
        args = { region: config.region || "us-east-1" }

        # "ecs" means the container credential provider; otherwise the SDK's
        # default chain.
        args[:credentials] = Aws::ECSCredentials.new if config.ecs_credentials?

        Aws::SQS::Client.new(**args)
      end
    end

    class << self
      def runtime
        @runtime ||= Runtime.new
      end

      attr_writer :runtime

      def configure
        yield runtime.settings
      end
    end
  end
end

require_relative "queue/settings"
require_relative "queue/retry_attempts"
require_relative "queue/job_class"
require_relative "queue/managed_queue_config"
require_relative "queue/agent_client"
require_relative "queue/delivery"
require_relative "queue/message"
require_relative "queue/observer"
require_relative "queue/dispatcher"
require_relative "queue/message_attributes"
require_relative "queue/worker"
