# frozen_string_literal: true

require "aws-sdk-sqs"

module LaravelCloud
  # Laravel Cloud Managed Queues for Active Job.
  #
  # Pushing always goes straight to SQS. Receiving goes through the in-container
  # cloud-agent sidecar on Laravel Cloud, or straight from SQS when there is no
  # agent (local development). Retries are Active Job's own `retry_on`,
  # executed as SQS visibility resets of the original message — see Delivery.
  module Queue
    # No managed queue is provisioned (LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is
    # absent) but one was needed.
    class NotConfigured < LaravelCloud::Error; end

    # A dispatch the queue model can't honour — a delay beyond the SQS cap, a
    # delay on a FIFO queue, FIFO deduplication on a standard queue. Raised
    # rather than silently degraded.
    class InvalidDispatch < LaravelCloud::Error; end

    # The collaborators the adapter and worker share, built lazily so an app
    # boots (and enqueues raise a clear NotConfigured) with no managed queue
    # provisioned. Anything can be injected, which is how the tests swap in a
    # stubbed SQS client and a real-socket agent stub.
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

        # Laravel Cloud runs on ECS and signals "use the container credential
        # provider" with credentials: "ecs". Anything else falls through to the
        # SDK's default chain (env vars, profiles, instance role).
        args[:credentials] = Aws::ECSCredentials.new if config.ecs_credentials?

        Aws::SQS::Client.new(**args)
      end
    end

    class << self
      def runtime
        @runtime ||= Runtime.new
      end

      attr_writer :runtime

      # Adjust the optional app-level settings — see Settings.
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
