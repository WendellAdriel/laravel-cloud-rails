# frozen_string_literal: true

require "json"

module LaravelCloud
  module Queue
    # Parses LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG, which Laravel Cloud injects:
    #
    #   {
    #     "driver": "cloud",
    #     "queue": "default",
    #     "connection": {
    #       "driver": "sqs",
    #       "prefix": "https://sqs.us-east-2.amazonaws.com/036991040854",
    #       "suffix": "-env-a21e326b-...",
    #       "queue": "default",
    #       "region": "us-east-2",
    #       "credentials": "ecs"
    #     },
    #     "agent": {
    #       "enabled": true,
    #       "socket": "/tmp/cloud-agent.sock"
    #     }
    #   }
    class ManagedQueueConfig
      DEFAULT_AGENT_SOCKET = "/tmp/cloud-agent.sock"
      FIFO_SUFFIX = ".fifo"

      attr_reader :queue_url, :region, :credentials, :queue, :agent_socket, :prefix, :suffix

      def self.from_env(env = ENV)
        from_json(env["LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG"], agent_socket: env["LARAVEL_CLOUD_AGENT_SOCKET"])
      end

      # Missing or malformed config gives an unconfigured result rather than an
      # error, so the app still boots.
      def self.from_json(json, agent_socket: nil)
        data = parse(json)
        connection = data["connection"].is_a?(Hash) ? data["connection"] : {}

        agent = data["agent"].is_a?(Hash) ? data["agent"] : {}

        # Agent socket: the config's, then LARAVEL_CLOUD_AGENT_SOCKET, then the default.
        socket = presence(agent["socket"]) || presence(agent_socket) || DEFAULT_AGENT_SOCKET

        new(
          prefix: connection["prefix"].to_s.sub(%r{/+\z}, ""),
          suffix: connection["suffix"].to_s,
          queue: (connection["queue"] || data["queue"] || "default").to_s,
          region: connection["region"]&.to_s,
          credentials: (connection["credentials"] || "default").to_s,
          agent_socket: socket,
          agent_enabled: agent["enabled"] == true,
        )
      end

      def self.parse(json)
        return {} if json.nil? || json.empty?

        decoded = JSON.parse(json)
        decoded.is_a?(Hash) ? decoded : {}
      rescue JSON::ParserError
        {}
      end
      private_class_method :parse

      def self.presence(value)
        value.is_a?(String) && !value.empty? ? value : nil
      end
      private_class_method :presence

      # ".fifo" is the only marker SQS and Laravel use for FIFO queues.
      def self.fifo?(queue)
        queue.to_s.end_with?(FIFO_SUFFIX)
      end

      def initialize(prefix:, suffix:, queue:, region:, credentials:, agent_socket:, agent_enabled:)
        @prefix = prefix
        @suffix = suffix
        @queue = queue
        @region = region
        @credentials = credentials
        @agent_socket = agent_socket
        @agent_enabled = agent_enabled
        @queue_url = queue_url_for(queue)
      end

      # The SQS URL for a queue name, as Laravel's SqsQueue::suffixQueue builds
      # it: {prefix}/{queue}{suffix}, with the suffix before ".fifo" on FIFO
      # queues ({prefix}/{base}{suffix}.fifo). Nil when unconfigured.
      def queue_url_for(queue)
        return nil if prefix.empty?

        if self.class.fifo?(queue)
          "#{prefix}/#{queue.delete_suffix(FIFO_SUFFIX)}#{suffix}#{FIFO_SUFFIX}"
        else
          "#{prefix}/#{queue}#{suffix}"
        end
      end

      # The queue name for an SQS URL — the inverse of #queue_url_for, and the
      # name the dashboard groups metrics by (Laravel's Cloud\Queue::normalizeQueue).
      def normalize_queue(queue)
        queue = queue.to_s
        queue = queue.delete_prefix("#{prefix}/") unless prefix.empty?

        return queue if suffix.empty?

        if self.class.fifo?(queue)
          base = queue.delete_suffix(FIFO_SUFFIX)
          base.end_with?(suffix) ? "#{base.delete_suffix(suffix)}#{FIFO_SUFFIX}" : queue
        else
          queue.delete_suffix(suffix)
        end
      end

      def configured?
        !queue_url.nil?
      end

      # Whether to receive through the Cloud agent. Trusts the config's flag
      # rather than probing for the socket, which may appear or vanish.
      def agent_available?
        @agent_enabled
      end

      def ecs_credentials?
        credentials == "ecs"
      end
    end
  end
end
